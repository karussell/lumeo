package com.pannous.tmpo;

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
import com.pannous.tmpo.util.SelectiveAnalyzer;
import com.pannous.tmpo.util.SearchExecutor;
import java.io.File;
import java.io.IOException;


import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.DateTools;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.document.NumericField;
import org.apache.lucene.index.FieldInfo.IndexOptions;
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
 * Taken from:
 * http://code.google.com/p/graphdb-load-tester/
 * 
 * Uses a buffer to accumulate uncommitted state.
 * 
 * @author Peter Karich, info@jetsli.de
 * @author Mark
 *
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
    // TODO use trove collection
    private Map<Long, Document> batchBuffer = new ConcurrentHashMap<Long, Document>();
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
    public static final Version VERSION = Version.LUCENE_35;
    private Analyzer analyzer = new SelectiveAnalyzer();
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

    public RawLucene setAnalyzer(Analyzer analyzer) {
        this.analyzer = analyzer;
        return this;
    }

    public Analyzer getAnalyzer() {
        return analyzer;
    }

    public RawLucene init() {
        try {
            IndexWriterConfig cfg = new IndexWriterConfig(VERSION, analyzer);
            LogByteSizeMergePolicy mp = new LogByteSizeMergePolicy();
            mp.setMaxMergeMB(getMaxMergeMB());
            //mp.setUseCompoundFile(useCompoundFile);
            cfg.setRAMBufferSizeMB(ramBufferSizeMB);
            cfg.setTermIndexInterval(termIndexIntervalSize);
            cfg.setMergePolicy(mp);
            // why this?
            //cfg.setOpenMode(IndexWriterConfig.OpenMode.CREATE);

            // improve ID lookup via LUCENE 4.0 see http://blog.mikemccandless.com/2010/06/lucenes-pulsingcodec-on-primary-key.html
            // cfg.setCodecProvider()
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
        Document result = batchBuffer.get(id);
        if (result != null)
            return result;

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

    public Document createDocument(String uId, long id) {
        Document doc = new Document();
        doc.add(newUIdField(UID, uId));
        doc.add(newIdField(ID, id));
        return doc;
    }

    public static Fieldable newDateField(String name, long value) {
        return new Field(name, DateTools.timeToString(value, DateTools.Resolution.MINUTE),
                Field.Store.YES, Field.Index.NOT_ANALYZED);
    }

    public static Fieldable newUIdField(String name, String value) {
        Field uIdField = new Field(name, value, Field.Store.YES, Field.Index.NOT_ANALYZED_NO_NORMS);
        uIdField.setIndexOptions(IndexOptions.DOCS_ONLY);
        return uIdField;
    }

    public static Fieldable newIdField(String name, long id) {
        NumericField idField = new NumericField(name, 4, Field.Store.YES, true).setLongValue(id);
        idField.setIndexOptions(IndexOptions.DOCS_ONLY);
        return idField;
    }

    public static Fieldable newStringField(String name, String val) {
        Field field = new Field(name, val, Field.Store.YES, Field.Index.NOT_ANALYZED_NO_NORMS);
        field.setIndexOptions(IndexOptions.DOCS_ONLY);
        return field;
    }

    void clear() {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    long count(final String fieldName, final Object value) {
        return searchSomething(new SearchExecutor<Long>() {

            @Override public Long execute(IndexSearcher searcher) throws Exception {
                Term searchTerm = new Term(fieldName).createTerm(toTermString(value));
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
            batchBuffer.put(id, newDoc);
            luceneAdds++;
            long currentSearchGeneration = nrtManager.getCurrentSearchingGen(true);
            if (batchBuffer.size() > maxNumRecordsBeforeIndexing)
                currentSearchGeneration = flush();

            return currentSearchGeneration;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public long put(String uId, long id, Document newDoc) {
        if (newDoc.get(ID) == null)
            newDoc.add(newIdField(ID, id));

        if (newDoc.get(UID) == null)
            newDoc.add(newUIdField(UID, uId));

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

    /**
     * You'll need to call releaseUnmanagedSearcher afterwards
     */
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

    long flush() throws IOException {
        //TODO UPDATE docs!! 
        // nrtManager.deleteDocuments(terms);        
        long currentSearchGeneration = nrtManager.addDocuments(batchBuffer.values());
        luceneAdds += batchBuffer.values().size();
        batchBuffer.clear();
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
        edgeDoc.add(newIdField(VERTEX_OUT, oIndex));
        long iIndex = ((NumericField) vIn.getFieldable(ID)).getNumericValue().longValue();
        edgeDoc.add(newIdField(VERTEX_IN, iIndex));

        long eId = ((NumericField) edgeDoc.getFieldable(ID)).getNumericValue().longValue();
        vOut.add(newIdField(EDGE_OUT, eId));
        vIn.add(newIdField(EDGE_IN, eId));

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
}
