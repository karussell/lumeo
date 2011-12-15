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
import de.jetsli.lumeo.util.MapEntry;
import de.jetsli.lumeo.util.Mapping;
import de.jetsli.lumeo.util.SearchExecutor;
import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
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
import org.apache.lucene.search.NRTManagerReopenThread;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.SearcherWarmer;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Uses a buffer to accumulate uncommitted state. Should stay independent of Blueprints API.  
 * 
 * Minor impressions taken from
 * http://code.google.com/p/graphdb-load-tester/source/browse/trunk/src/com/tinkerpop/graph/benchmark/index/LuceneKeyToNodeIdIndexImpl.java
 * 
 * -> still use batchBuffer to support realtime get and to later support versioning
 * -> use near real time reader, no need for commit 
 * -> no bloomfilter then it is 1 sec (>10%) faster for testIndexing and less memory usage
 *    TODO check if traversal benchmark is also faster
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
    //Avoid Lucene performing "mega merges" with a finite limit on segments sizes that can be merged
    private int maxMergeMB = 3000;
    private volatile long luceneOperations = 0;
    private long failedLuceneReads = 0;
    private long successfulLuceneReads = 0;
    private double ramBufferSizeMB = 128;
    private int termIndexIntervalSize = 512;
    private final ReadWriteLock indexRWLock = new ReentrantReadWriteLock();
    // id -> indexOp (create, update, delete)    
    // we could group indexop and same type (same analyzer) to make indexing faster    
    private final Map<Long, Map<Long, IndexOp>> realTimeCache = new ConcurrentHashMap<Long, Map<Long, IndexOp>>();
    private Logger logger = LoggerFactory.getLogger(getClass());
    private Map<String, Mapping> mappings = new ConcurrentHashMap<String, Mapping>(2);
    private Mapping defaultMapping = new Mapping("_default");
    private String name;
    private boolean closed = false;
    private FlushThread flushThread;
    private NRTManagerReopenThread reopenThread;
    private volatile long latestGen = -1;

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
        indexLock();
        try {
            if (closed)
                throw new IllegalStateException("Already closed");

            if (writer != null)
                throw new IllegalStateException("Already initialized");

            // release locks when started
            if (IndexWriter.isLocked(dir)) {
                logger.warn("index is locked + " + name + " -> releasing lock");
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

            getCurrentRTCache(latestGen);
            int priority = Math.min(Thread.currentThread().getPriority() + 2, Thread.MAX_PRIORITY);
            flushThread = new FlushThread("flush-thread");
            flushThread.setPriority(priority);
            flushThread.setDaemon(true);
            flushThread.start();

            // If there are waiting searchers how long should reopen takes?
            double incomingSearchesMaximumWaiting = 0.03;
            // If there are no waiting searchers reopen it less frequent.
            // This also controls how large the realtime cache can be. less frequent reopens => larger cache
            double ordinaryWaiting = 5.0;
            reopenThread = new NRTManagerReopenThread(nrtManager, ordinaryWaiting, incomingSearchesMaximumWaiting);
            reopenThread.setName("NRT Reopen Thread");
            reopenThread.setPriority(priority);
            reopenThread.setDaemon(true);
            reopenThread.start();
            return this;
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            indexUnlock();
        }
    }

    Term getIdTerm(long id) {
        return idTerm.createTerm(NumericUtils.longToPrefixCoded(id));
    }

    long getId(Document doc) {
        return ((NumericField) doc.getFieldable(ID)).getNumericValue().longValue();
    }

    public Document findById(final long id) {
        IndexOp result = getCurrentRTCache(latestGen).get(id);
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

    // not thread safe => only an estimation
    public int calcSize() {
        int unflushedEntries = 0;
        for (Entry<Long, Map<Long, IndexOp>> e : realTimeCache.entrySet()) {
            unflushedEntries = e.getValue().size();
        }
        return unflushedEntries;
    }

    public void close() {
        indexLock();
        try {
            flushThread.interrupt();
            reopenThread.close();
            flushThread.join();
            // avoid loosing data in buffer            
            flush();

            int unflushedEntries = calcSize();
            if (unflushedEntries > 0)
                logger.warn("RawLucene.close with unflushed entries greater zero: " + unflushedEntries);
            closed = true;
            nrtManager.close();
            writer.rollback();
            writer.close();
            dir.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            indexUnlock();
        }
    }

    public Document createDocument(String uId, long id, Class cl) {
        Document doc = new Document();
        Mapping m = getMapping(cl.getSimpleName());
        doc.add(m.createField(RawLucene.TYPE, cl.getSimpleName()));
        doc.add(m.newUIdField(UID, uId));
        doc.add(m.newIdField(ID, id));
        return doc;
    }

    long count(final String fieldName, final Object value) {
        return searchSomething(new SearchExecutor<Long>() {

            @Override public Long execute(IndexSearcher searcher) throws Exception {
                Term searchTerm = defaultMapping.toTerm(fieldName, value);
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

    long removeById(final long id) {
        try {
            latestGen = nrtManager.deleteDocuments(getIdTerm(id));
            getCurrentRTCache(latestGen).put(id, new IndexOp(IndexOp.Type.DELETE));
            return latestGen;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public long fastPut(long id, Document newDoc) {
        try {
            String type = newDoc.get(TYPE);
            if (type == null)
                throw new UnsupportedOperationException("Document needs to have a type associated");
            Mapping m = getMapping(type);
            latestGen = nrtManager.updateDocument(getIdTerm(id), newDoc, m.getAnalyzerWrapper());
            getCurrentRTCache(latestGen).put(id, new IndexOp(newDoc, IndexOp.Type.UPDATE));
            return latestGen;
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

    void refresh() {
        try {
            writer.commit();
            nrtManager.maybeReopen(true);
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

    void indexLock() {
        indexRWLock.writeLock().lock();
    }

    void indexUnlock() {
        indexRWLock.writeLock().unlock();
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
            throw new NullPointerException("Type mustn't be empty!");

        Mapping m = mappings.get(type);
        if (m == null) {
            mappings.put(type, m = new Mapping(type));

            if (logger.isDebugEnabled())
                logger.debug("Created mapping for type " + type);
        }
        return m;
    }

    void flushOld() throws IOException {
//        try {
//            logger.info("flush:" + batchBuffer.size() + " " + luceneOperations);
//            for (Entry<Long, IndexOp> entry : batchBuffer.entrySet()) {
//                luceneOperations++;
//                Document d = entry.getValue().document;
//                switch (entry.getValue().type) {
//                    case CREATE:
//                        Mapping m = getMapping(d.get(TYPE));
//                        latestGen = nrtManager.addDocument(d, m.getAnalyzerWrapper());
//                        break;
//                    case UPDATE:
//                        Term t = getIdTerm(entry.getKey());
//                        m = getMapping(d.get(TYPE));
//                        latestGen = nrtManager.updateDocument(t, d, m.getAnalyzerWrapper());
//                        break;
//                    case DELETE:
//                        t = getIdTerm(entry.getKey());
//                        latestGen = nrtManager.deleteDocuments(t);
//                        break;
//                    default:
//                        throw new UnsupportedOperationException("type " + entry.getValue().type + " is not allowed");
//                }
//            }
//            logger.info("flush:" + batchBuffer.size() + " " + luceneOperations);
//            
//            if (logger.isDebugEnabled()) {
//                long diff = System.currentTimeMillis() - startTime;
//                logger.debug(latestGen + " time to last flush:" + diff / 1000f
//                        + ", reads:" + successfulLuceneReads + ", failed reads:" + failedLuceneReads
//                        + ", current ops " + batchBuffer.size() + ", all ops:" + luceneOperations);
//            }
//
//            batchBuffer.clear();
//            failedLuceneReads = 0;
//            successfulLuceneReads = 0;
//            startTime = System.currentTimeMillis();
//        } finally {
////            indexUnlock();
//            // TODO
////            synchronized (bufferFullLock) {
////                bufferFullLock.notifyAll();
////            }
//        }
    }
    private Map<Long, IndexOp> tmpCache;
    private long tmpGen = -2;

    private Map<Long, IndexOp> getCurrentRTCache(long gen) {
        if (gen > tmpGen)
            synchronized (realTimeCache) {
                tmpGen = gen;
                tmpCache = new ConcurrentHashMap<Long, IndexOp>(100);
                realTimeCache.put(gen, tmpCache);
            }

        return tmpCache;
    }

    private class FlushThread extends Thread {

        public FlushThread(String name) {
            super(name);
        }

        @Override public void run() {
            Throwable exception = null;
            while (!isInterrupted()) {
                try {
                    flush(latestGen);
                } catch (InterruptedException ex) {
                    exception = ex;
                    break;
                } catch (AlreadyClosedException ex) {
                    exception = ex;
                    break;
                } catch (OutOfMemoryError er) {
                    logger.error("Now closing writer due to OOM", er);
                    try {
                        writer.close();
                    } catch (Exception ex) {
                        logger.error("Error while closing writer", ex);
                    }
                    exception = er;
                    break;
                } catch (Exception ex) {
                    logger.error("Problem while flushing", ex);
                }
            }

            logger.debug("flush-thread interrupted, " + ((exception == null) ? "" : exception.getMessage())
                    + ", buffer:" + calcSize());
        }
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

    public void flush() {
        try {
            flush(latestGen);
        } catch (InterruptedException ex) {
            logger.error("Interrupted", ex);
        }
    }

    public void flush(long gen) throws InterruptedException {
        if (nrtManager.getCurrentSearchingGen(true) >= gen) {
            Thread.sleep(20);
            return;
        }

        logger.info("wait for gen:" + gen);
        nrtManager.waitForGeneration(gen, true);
        int removed = 0;
        Iterator<Entry<Long, Map<Long, IndexOp>>> iter = realTimeCache.entrySet().iterator();
        while (iter.hasNext()) {
            Entry<Long, Map<Long, IndexOp>> e = iter.next();
            if (e.getKey() < gen) {
                iter.remove();
                removed++;
                e.getValue().clear();
            }
        }
        logger.info("removed maps:" + removed);
    }
}
