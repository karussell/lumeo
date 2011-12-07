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
import de.jetsli.lumeo.util.Mapping;
import de.jetsli.lumeo.util.SearchExecutor;
import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.lucene.document.DateTools;
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
    // type -> id, doc
    private Map<String, Map<Long, Document>> batchBuffer = new ConcurrentHashMap<String, Map<Long, Document>>(2);
    private IndexWriter writer;
    private Directory dir;
    private NRTManager nrtManager;
    private Term uIdTerm = new Term(UID, "");
    private Term idTerm = new Term(ID, "");
    private int bloomFilterSize = 50 * 1024 * 1024;
    private int maxNumRecordsBeforeIndexing = 500000;
//    private OpenBitSet bloomFilter;
    //Avoid Lucene performing "mega merges" with a finite limit on segments sizes that can be merged
    private int maxMergeMB = 3000;
    private long luceneAdds = 0;
    private long failedLuceneReads = 0;
    private long successfulLuceneReads = 0;
    private long startTime = System.currentTimeMillis();
    private double ramBufferSizeMB = 300;
    private int termIndexIntervalSize = 512;
    private Logger logger = LoggerFactory.getLogger(getClass());
    private Map<String, Mapping> mappings = new ConcurrentHashMap<String, Mapping>(2);
    private Mapping defaultMapping = new Mapping("_default");
    private String name;

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
        try {
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
            return this;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public Document findById(final long id) {
        for (Map<Long, Document> mapBuffer : batchBuffer.values()) {
            Document result = mapBuffer.get(id);
            if (result != null)
                return result;
        }

        return searchSomething(new SearchExecutor<Document>() {

            @Override public Document execute(IndexSearcher searcher) throws Exception {
                IndexReader reader = searcher.getIndexReader();
                Term searchTerm = idTerm.createTerm(NumericUtils.longToPrefixCoded(id));
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
            nrtManager.close();
            writer.close();
            dir.close();
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
                Term searchTerm = new Term(fieldName, toTermString(value));
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

    String toTermString(Object o) {
        if (o instanceof String)
            return (String) o;
        else if (o instanceof Long)
            return NumericUtils.longToPrefixCoded((Long) o);
        else if (o instanceof Double)
            return NumericUtils.doubleToPrefixCoded((Double) o);
        else if (o instanceof Date)
            return DateTools.timeToString(((Date) o).getTime(), DateTools.Resolution.MINUTE);
        else
            throw new UnsupportedOperationException("couldn't transform into string " + o);
    }

    long removeById(final long id) {
        try {
            return nrtManager.deleteDocuments(idTerm.createTerm(NumericUtils.longToPrefixCoded(id)));
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public long fastPut(long id, Document newDoc) {
        try {
            String type = newDoc.get(TYPE);
            if (type == null)
                throw new UnsupportedOperationException("Document needs to have a type associated");

            Map<Long, Document> buffer = getBuffer(type);
            buffer.put(id, newDoc);
            luceneAdds++;
            long currentSearchGeneration = nrtManager.getCurrentSearchingGen(true);
            if (buffer.size() > maxNumRecordsBeforeIndexing)
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

     <T extends LuceneElement> void update(Document doc) {
        long id = ((NumericField) doc.getFieldable(ID)).getNumericValue().longValue();
        // TODO PERFORMANCE should we really check?
        if (exists(id))
            removeById(id);
        fastPut(id, doc);
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
        return luceneAdds;
    }

    public long getFailedLuceneReads() {
        return failedLuceneReads;
    }

    public long getSuccessfulLuceneReads() {
        return successfulLuceneReads;
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

    // TODO instead of async feeding use a queue or batchBuffer and this thread
    // requirements:
    // - collect objects up to maximum 1000 to call more efficient addDocuments instead of addDocument
    // - guarantee order except if id is identical => in this case overwrite (TODO version/optimistic locking)
    //   TODO how to guarantee order for different types?
    // - feed listener to watch filters and implement sync e.g. for tests    
    
    class FlushThread implements Runnable {

        @Override public void run() {
            while (true) {
            }
        }
    }

    long flush() throws IOException {
        // TODO LIMIT count per add
        // TODO UPDATE, ADD, DELETE via nrtManager.updateDocument(term, docs, analyzer);
//        nrtManager.updateDocuments(new Term, null);
//        nrtManager.deleteDocuments(terms);
        long currentSearchGeneration = -1;
        for (Entry<String, Map<Long, Document>> docEntries : batchBuffer.entrySet()) {
            Mapping m = getMapping(docEntries.getKey());
            
            currentSearchGeneration = nrtManager.addDocuments(docEntries.getValue().values(),
                    m.getAnalyzerWrapper());
            luceneAdds += docEntries.getValue().values().size();
            docEntries.getValue().clear();
        }
        
        nrtManager.maybeReopen(true);
        if (logger.isInfoEnabled()) {
            long diff = System.currentTimeMillis() - startTime;
            logger.info(diff + "," + failedLuceneReads + "," + successfulLuceneReads + "," + luceneAdds);
        }

        failedLuceneReads = 0;
        luceneAdds = 0;
        successfulLuceneReads = 0;
        startTime = System.currentTimeMillis();
        return currentSearchGeneration;
    }

    @Override public String toString() {
        return name;
    }

    void initRelation(Document edgeDoc, Document vOut, Document vIn) {
        long oIndex = ((NumericField) vOut.getFieldable(ID)).getNumericValue().longValue();
        edgeDoc.add(defaultMapping.newIdField(VERTEX_OUT, oIndex));
        long iIndex = ((NumericField) vIn.getFieldable(ID)).getNumericValue().longValue();
        edgeDoc.add(defaultMapping.newIdField(VERTEX_IN, iIndex));

        long eId = ((NumericField) edgeDoc.getFieldable(ID)).getNumericValue().longValue();
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
        Mapping m = mappings.get(type);
        if (m == null) {
            mappings.put(type, m = new Mapping(type));
            logger.info("Created mapping for type " + type);
        }
        return m;
    }

    Map<Long, Document> getBuffer(String type) {
        Map<Long, Document> docs = batchBuffer.get(type);
        if (docs == null) {
            docs = new ConcurrentHashMap<Long, Document>();
            batchBuffer.put(type, docs);
        }
        return docs;
    }
}
