package de.jetsli.lumeo;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import de.jetsli.lumeo.util.IndexOp;
import de.jetsli.lumeo.util.Mapping;
import de.jetsli.lumeo.util.SearchExecutor;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LogByteSizeMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.NRTManager;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.SearcherWarmer;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Uses a buffer to accumulate uncommitted state. Should stay independent of Blueprints API.
 * 
 * @author Peter Karich, info@jetsli.de
 */
public class RawLucene {

    // of type long, for more efficient storage of node references
    public static final String ID = "_id";
    // of type String, can be defined by the user
    public static final String UID = "_uid";
    // of type String
    public static final String TYPE = "_type";
    public static final String EDGE_OUT = "_eout";
    public static final String EDGE_IN = "_ein";
    public static final String EDGE_LABEL = "_elabel";
    public static final String VERTEX_OUT = "_vout";
    public static final String VERTEX_IN = "_vin";
    public static final Version VERSION = Version.LUCENE_35;
    private IndexWriter writer;
    private Directory dir;
    private NRTManager nrtManager;
    private Term uIdTerm = new Term(UID);
    private Term idTerm = new Term(ID);
    private int bloomFilterSize = 50 * 1024 * 1024;
    private int maxNumRecordsBeforeIndexing = 1000;
    //Avoid Lucene performing "mega merges" with a finite limit on segments sizes that can be merged
    private int maxMergeMB = 3000;
    private long luceneOperations = 0;
    private long failedLuceneReads = 0;
    private long successfulLuceneReads = 0;
    private long startTime = System.currentTimeMillis();
    private double ramBufferSizeMB = 128;
    private int termIndexIntervalSize = 512;
    private long maxFlushInterval = 1000L;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    // id -> indexOp (create, update, delete)    
    // we could group indexop and same type (same analyzer) to make indexing faster
    private Map<Long, IndexOp> batchBuffer = new ConcurrentHashMap<Long, IndexOp>(maxNumRecordsBeforeIndexing);
    private Logger logger = LoggerFactory.getLogger(getClass());
    private Map<String, Mapping> mappings = new ConcurrentHashMap<String, Mapping>(2);
    private Mapping defaultMapping = new Mapping("_default");
    private String name;
    private boolean closed = false;
    private FlushThread flushThread;

    public RawLucene(String path) {
        try {
            dir = FSDirectory.open(new File(path));
            name = "fs:" + path + " " + dir.toString();
        } catch (IOException ex) {
            throw new RuntimeException("cannot open lucene directory located at " + path + " error:" + ex.getMessage());
        }
    }

    public RawLucene(Directory directory) {
        dir = directory;
        name = "mem " + dir.toString();
    }

    public RawLucene init() {
        lock.writeLock().lock();
        try {
            if (closed)
                throw new IllegalStateException("Already closed");

            if (writer != null)
                throw new IllegalStateException("Already initialized");

            // release locks when started
            if (IndexWriter.isLocked(dir)) {
                logger.warn("shard is locked, releasing lock");
                IndexWriter.unlock(dir);
            }
            IndexWriterConfig cfg = new IndexWriterConfig(VERSION, defaultMapping.getAnalyzerWrapper());
            LogByteSizeMergePolicy mp = new LogByteSizeMergePolicy();
            mp.setMaxMergeMB(getMaxMergeMB());
            cfg.setRAMBufferSizeMB(ramBufferSizeMB);
            cfg.setTermIndexInterval(termIndexIntervalSize);
            cfg.setMergePolicy(mp);
            cfg.setMaxThreadStates(8);
            boolean create = !IndexReader.indexExists(dir);
            cfg.setOpenMode(create ? IndexWriterConfig.OpenMode.CREATE : IndexWriterConfig.OpenMode.APPEND);
            writer = new IndexWriter(dir, cfg);
            nrtManager = new NRTManager(writer, new SearcherWarmer() {

                @Override
                public void warm(IndexSearcher s) throws IOException {
                    // TODO get some random vertices via getVertices?
                }
            });
            flushThread = new FlushThread();
            flushThread.setName("flush");
            flushThread.start();
            return this;
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    Term getIdTerm(long id) {
        return idTerm.createTerm(NumericUtils.longToPrefixCoded(id));
    }

    long getId(Document doc) {
        return ((NumericField) doc.getFieldable(ID)).getNumericValue().longValue();
    }

    public Document findById(final long id) {
        IndexOp result = batchBuffer.get(id);
        if (result != null)
            return result.document;

        return searchSomething(new SearchExecutor<Document>() {

            @Override public Document execute(IndexSearcher searcher) throws Exception {
                IndexReader reader = searcher.getIndexReader();
                Term searchTerm = getIdTerm(id);
                TermDocs td = reader.termDocs(searchTerm);
                if (td.next()) {
                    Document doc = reader.document(td.doc());
                    successfulLuceneReads++;
                    return doc;
                }
                failedLuceneReads++;
                return null;
            }
        });
    }

    public Document findByUserId(final String uId) {
        return searchSomething(new SearchExecutor<Document>() {

            @Override public Document execute(IndexSearcher searcher) throws Exception {
                IndexReader reader = searcher.getIndexReader();
                Term searchTerm = uIdTerm.createTerm(uId);
                TermDocs td = reader.termDocs(searchTerm);
                if (td.next())
                    return reader.document(td.doc());

                return null;
            }
        });
    }

    long getCurrentGen() {
        return nrtManager.getCurrentSearchingGen(true);
    }

    public <T> T searchSomething(SearchExecutor<T> exec) {
        SearcherManager sm = nrtManager.getSearcherManager(true);
        IndexSearcher searcher = sm.acquire();
        try {
            return (T) exec.execute(searcher);
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            try {
                sm.release(searcher);
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    public boolean exists(long id) {
        return findById(id) != null;
    }

    public boolean existsUserId(String uId) {
        return findByUserId(uId) != null;
    }

    public void close() {
        try {
            closed = true;
            nrtManager.close();
            // writer.rollback();
            writer.close();
            dir.close();
            flushThread.interrupt();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public Document createDocument(String uId, long id, Class cl) {
        Document doc = new Document();
        Mapping m = getMapping(cl.getSimpleName());
        doc.add(m.newStringField(RawLucene.TYPE, cl.getSimpleName()));
        doc.add(m.newUIdField(UID, uId));
        doc.add(m.newIdField(ID, id));
        return doc;
    }

    long count(final String fieldName, final Object value) {
        return searchSomething(new SearchExecutor<Long>() {

            @Override public Long execute(IndexSearcher searcher) throws Exception {
                Term searchTerm = new Term(fieldName, defaultMapping.toTermString(value));
                TermDocs td = searcher.getIndexReader().termDocs(searchTerm);
                try {
                    long c = 0;
                    while (td.next()) {
                        c++;
                    }
                    return c;
                } finally {
                    td.close();
                }
            }
        });
    }

    void removeById(final long id) {
        batchBuffer.put(id, new IndexOp(IndexOp.Type.DELETE));
    }

    public long fastPut(long id, Document newDoc) {
        try {
            String type = newDoc.get(TYPE);
            if (type == null)
                throw new UnsupportedOperationException("Document needs to have a type associated");

            batchBuffer.put(id, new IndexOp(newDoc, IndexOp.Type.UPDATE));
            luceneOperations++;
            long currentSearchGeneration = nrtManager.getCurrentSearchingGen(true);
            if (batchBuffer.size() > maxNumRecordsBeforeIndexing)
                currentSearchGeneration = flush();

            return currentSearchGeneration;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public long put(String uId, long id, Document newDoc) {
        String type = newDoc.get(TYPE);
        if (type == null)
            throw new UnsupportedOperationException("Document needs to have a type associated");
        Mapping m = getMapping(type);
        if (newDoc.get(ID) == null)
            newDoc.add(m.newIdField(ID, id));

        if (newDoc.get(UID) == null)
            newDoc.add(m.newUIdField(UID, uId));

        return fastPut(id, newDoc);
    }

    public double getRamBufferSizeMB() {
        return ramBufferSizeMB;
    }

    public void setRamBufferSizeMB(double ramBufferSizeMB) {
        this.ramBufferSizeMB = ramBufferSizeMB;
    }

    public int getTermIndexIntervalSize() {
        return termIndexIntervalSize;
    }

    public void setTermIndexIntervalSize(int termIndexIntervalSize) {
        this.termIndexIntervalSize = termIndexIntervalSize;
    }

    public int getBloomFilterSize() {
        return bloomFilterSize;
    }

    public void setBloomFilterSize(int bloomFilterSize) {
        this.bloomFilterSize = bloomFilterSize;
    }

    public int getMaxNumRecordsBeforeCommit() {
        return maxNumRecordsBeforeIndexing;
    }

    public void setMaxNumRecordsBeforeCommit(int maxNumRecordsBeforeCommit) {
        this.maxNumRecordsBeforeIndexing = maxNumRecordsBeforeCommit;
    }

    public void setMaxMergeMB(int maxMergeMB) {
        this.maxMergeMB = maxMergeMB;
    }

    public int getMaxMergeMB() {
        return maxMergeMB;
    }

    public long getLuceneAdds() {
        return luceneOperations;
    }

    public long getFailedLuceneReads() {
        return failedLuceneReads;
    }

    public long getSuccessfulLuceneReads() {
        return successfulLuceneReads;
    }

    public long getMaxFlushInterval() {
        return maxFlushInterval;
    }

    public void setMaxFlushInterval(long maxFlushInterval) {
        this.maxFlushInterval = maxFlushInterval;
    }

    void refresh() {
        try {
            writer.commit();
        } catch (Exception ex) {
            throw new RuntimeException();
        }
    }

    /** You'll need to call releaseUnmanagedSearcher afterwards */
    IndexSearcher newUnmanagedSearcher() {
        SearcherManager sm = nrtManager.getSearcherManager(true);
        return sm.acquire();
    }

    void releaseUnmanagedSearcher(IndexSearcher searcher) {
        // TODO: is it ok to avoid calling the searchmanager?
        try {
            searcher.getIndexReader().decRef();
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    void removeDoc(Document doc) {
        removeById(getId(doc));
    }

    void lockStart() {
        lock.writeLock().lock();
    }

    void lockRelease() {
        lock.writeLock().unlock();
    }

    // TODO add feed listener to every IndexOp?
    private class FlushThread extends Thread {

        @Override public void run() {
            long start = System.currentTimeMillis();
            while (true) {
                try {
                    long newStart = System.currentTimeMillis();
                    if (batchBuffer.size() > maxNumRecordsBeforeIndexing
                            || newStart - start > maxFlushInterval) {
                        start = newStart;
                        flush();
                    }
                } catch (IOException ex) {
                    logger.error("Problem while flushing", ex);
                }

                try {
                    for (int i = 0; i < 100; i++) {
                        // make sure we do not wait if massive indexing rate
                        if (batchBuffer.size() > maxNumRecordsBeforeIndexing)
                            break;

                        Thread.yield();
                        Thread.sleep(10);
                    }
                } catch (InterruptedException ex) {
                    if (logger.isDebugEnabled())
                        logger.debug("Flush thread interrupted");
                    break;
                }
            }
        }
    }

    long flush() throws IOException {
        long currentSearchGeneration = -1;
        for (Entry<Long, IndexOp> entry : batchBuffer.entrySet()) {
            Document d = entry.getValue().document;
            switch (entry.getValue().type) {
                case CREATE:
                    Mapping m = getMapping(d.get(TYPE));
                    currentSearchGeneration = nrtManager.addDocument(d, m.getAnalyzerWrapper());
                    break;
                case UPDATE:
                    Term t = getIdTerm(entry.getKey());
                    m = getMapping(d.get(TYPE));
                    currentSearchGeneration = nrtManager.updateDocument(t, d, m.getAnalyzerWrapper());
                    break;
                case DELETE:
                    t = getIdTerm(entry.getKey());
                    currentSearchGeneration = nrtManager.deleteDocuments(t);
                    break;
                default:
                    throw new UnsupportedOperationException("type " + entry.getValue().type + " is not allowed");

            }
        }
        luceneOperations += batchBuffer.size();
        batchBuffer.clear();
        nrtManager.maybeReopen(true);
        if (logger.isDebugEnabled()) {
            long diff = System.currentTimeMillis() - startTime;
            logger.debug("time to last flush:" + diff / 1000 + ", reads:" + successfulLuceneReads
                    + ", failed reads:" + failedLuceneReads + ", ops:" + luceneOperations);
        }

        failedLuceneReads = 0;
        luceneOperations = 0;
        successfulLuceneReads = 0;
        startTime = System.currentTimeMillis();
        return currentSearchGeneration;
    }

    @Override public String toString() {
        return name;
    }

    void initRelation(Document edgeDoc, Document vOut, Document vIn) {
        long oIndex = getId(vOut);
        edgeDoc.add(defaultMapping.newIdField(VERTEX_OUT, oIndex));
        long iIndex = getId(vIn);
        edgeDoc.add(defaultMapping.newIdField(VERTEX_IN, iIndex));

        long eId = getId(edgeDoc);
        vOut.add(defaultMapping.newIdField(EDGE_OUT, eId));
        vIn.add(defaultMapping.newIdField(EDGE_IN, eId));

        fastPut(oIndex, vOut);
        fastPut(iIndex, vIn);
    }

    static String getVertexFieldForEdgeType(String edgeType) {
        if (EDGE_IN.equals(edgeType))
            return VERTEX_IN;
        else if (EDGE_OUT.equals(edgeType))
            return VERTEX_OUT;
        else
            throw new UnsupportedOperationException("Edge type not supported:" + edgeType);
    }

    /**
     * @return never null. Automatically creates a mapping if it does not exist.
     */
    public Mapping getMapping(String type) {
        if (type == null)
            throw new NullPointerException("Type shouldn't be empty!");

        Mapping m = mappings.get(type);
        if (m == null) {
            mappings.put(type, m = new Mapping(type));

            if (logger.isDebugEnabled())
                logger.debug("Created mapping for type " + type);
        }
        return m;
    }
}
