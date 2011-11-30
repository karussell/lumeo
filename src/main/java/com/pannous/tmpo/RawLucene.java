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
import com.tinkerpop.blueprints.pgm.Vertex;
import java.io.File;
import java.io.IOException;
import java.util.LinkedHashMap;


import java.util.Map;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.KeywordAnalyzer;
import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
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
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.OpenBitSet;
import org.apache.lucene.util.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Taken from:
 * http://code.google.com/p/graphdb-load-tester/
 * 
 * Uses Lucene to store and retrieve vertex Ids keyed on a user-defined key.
 * Uses:
 * 1) A bloom filter "to know what we don't know"
 * 2) An LRU cache to remember commonly accessed values we do know
 * 3) Uses a buffer to accumulate uncommitted state
 * 
 * Lucene takes care of the rest.
 * 
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
    public static final String VERTEX_OUT = "_vout";
    public static final String VERTEX_IN = "_vin";
    private Map<String, Document> batchBuffer = new LinkedHashMap<String, Document>();
    private IndexWriter writer;
    private Directory dir;
    private IndexReader reader;
    private Term uIdTerm = new Term(UID, "");
    private Term idTerm = new Term(ID, "");
    private int bloomFilterSize = 50 * 1024 * 1024;
    private int maxNumRecordsBeforeIndexing = 500000;
    private int lruCacheSize = 500000;
    private OpenBitSet bloomFilter;
    //Avoid Lucene performing "mega merges" with a finite limit on segments sizes that can be merged
    private int maxMergeMB = 3000;
    //Stats for each batch of updates
    private long bloomReadSaves = 0;
    private long luceneAdds = 0;
    private long failedLuceneReads = 0;
    private long successfulLuceneReads = 0;
    private long startTime = System.currentTimeMillis();
    private boolean showDebug;
    private boolean useCompoundFile = false;
    private double ramBufferSizeMB = 300;
    private int termIndexIntervalSize = 512;
    private Logger logger = LoggerFactory.getLogger(getClass());
    public static final Version VERSION = Version.LUCENE_35;
    public static final Analyzer WHITESPACE_ANALYZER = new WhitespaceAnalyzer(VERSION);
    public static final Analyzer STANDARD_ANALYZER = new StandardAnalyzer(VERSION);
    public static final Analyzer KEYWORD_ANALYZER = new KeywordAnalyzer();

    public RawLucene(String path) {
        try {
            dir = FSDirectory.open(new File(path));
        } catch (IOException ex) {
            logger.error("cannot open lucene directory located at " + path);
        }
    }

    public RawLucene(Directory directory) {
        dir = directory;
    }

    public RawLucene init() {
        try {
            bloomFilter = new OpenBitSet(bloomFilterSize);
            IndexWriterConfig cfg = new IndexWriterConfig(VERSION, WHITESPACE_ANALYZER);
            LogByteSizeMergePolicy mp = new LogByteSizeMergePolicy();
            mp.setMaxMergeMB(getMaxMergeMB());
            mp.setUseCompoundFile(useCompoundFile);
            cfg.setRAMBufferSizeMB(ramBufferSizeMB);
            cfg.setTermIndexInterval(termIndexIntervalSize);
            cfg.setMergePolicy(mp);
            // why this?
            //cfg.setOpenMode(IndexWriterConfig.OpenMode.CREATE);

            // improve ID lookup via LUCENE 4.0 see http://blog.mikemccandless.com/2010/06/lucenes-pulsingcodec-on-primary-key.html
            // cfg.setCodecProvider()
            writer = new IndexWriter(dir, cfg);
            return this;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public Document findById(long id) {
        try {
            reader = getReader();
            Term searchTerm = idTerm.createTerm(Long.toString(id));
            TermDocs td = reader.termDocs(searchTerm);
            if (td.next()) {
                Document doc = reader.document(td.doc());
                successfulLuceneReads++;
                return doc;
            }
            failedLuceneReads++;
            return null;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public boolean exists(String uId) {
        return findByUserId(uId) != null;
    }

    public Document findByUserId(String uId) {
        int bloomKey = Math.abs(uId.hashCode() % bloomFilterSize);
        if (!bloomFilter.fastGet(bloomKey)) {
            //Not seen - fail
            bloomReadSaves++;
            return null;
        }

        Document result = batchBuffer.get(uId);
        if (result != null)
            return result;

        try {
            reader = getReader();
            Term searchTerm = uIdTerm.createTerm(uId);
            TermDocs td = reader.termDocs(searchTerm);
            if (td.next())
                return reader.document(td.doc());

            return null;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void put(String uId, long id) {
        try {
            if (batchBuffer.size() > maxNumRecordsBeforeIndexing) {
                writer.commit();
                if (reader != null) {
                    IndexReader newReader = IndexReader.openIfChanged(reader, true);
                    if (newReader != null) {
                        reader.close();
                        reader = newReader;
                    }
                }
                batchBuffer.clear();

                if (showDebug) {
                    long diff = System.currentTimeMillis() - startTime;
                    logger.info(diff + "," + reader.maxDoc() + "," + bloomReadSaves + "," + failedLuceneReads + "," + successfulLuceneReads + "," + luceneAdds);
                }

                bloomReadSaves = 0;
                failedLuceneReads = 0;
                luceneAdds = 0;
                successfulLuceneReads = 0;
                startTime = System.currentTimeMillis();
            }

            int bloomKey = Math.abs(uId.hashCode() % bloomFilterSize);
            bloomFilter.fastSet(bloomKey);
            Document doc = createDocument(uId, id);
            batchBuffer.put(uId, doc);
            writer.addDocument(doc);
            luceneAdds++;
        } catch (Exception e) {
            throw new RuntimeException("Error adding key to index", e);
        }
    }

    public void close() {
        try {
            if (reader != null)
                reader.close();

            writer.close();
            dir.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected Document createDocument(String uId, long id) {
        Document doc = new Document();
        Field uIdField = new Field(UID, uId, Field.Store.NO, Field.Index.NOT_ANALYZED_NO_NORMS);
        uIdField.setIndexOptions(IndexOptions.DOCS_ONLY);
        doc.add(uIdField);
        doc.add(new Field(ID, Long.toString(id), Field.Store.YES, Field.Index.NO));
        return doc;
    }

    IndexReader getReader() {
        if (reader == null) {
            try {
                reader = IndexReader.open(writer, true);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return reader;
    }

    void clear() {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    long count(String fieldName, String value) {
        IndexSearcher searcher = new IndexSearcher(getReader());
        CountCollector cc = new CountCollector();
        try {
            Query q = new QueryParser(RawLucene.VERSION, fieldName, RawLucene.WHITESPACE_ANALYZER).parse(value);
            searcher.search(q, cc);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        return cc.getCount();
    }

    @SuppressWarnings("serial")
    static class LRUCache<K, V> extends LinkedHashMap<K, V> {

        private int maxSize;

        public LRUCache(int maxSize) {
            super(maxSize * 4 / 3 + 1, 0.75f, true);
            this.maxSize = maxSize;
        }

        @Override
        protected boolean removeEldestEntry(java.util.Map.Entry<K, V> eldest) {
            return size() > maxSize;
        }
    }

    public boolean isUseCompoundFile() {
        return useCompoundFile;
    }

    public void setUseCompoundFile(boolean useCompoundFile) {
        this.useCompoundFile = useCompoundFile;
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

    public void setLruCacheSize(int lruCacheSize) {
        this.lruCacheSize = lruCacheSize;
    }

    public int getLruCacheSize() {
        return lruCacheSize;
    }

    public void setMaxMergeMB(int maxMergeMB) {
        this.maxMergeMB = maxMergeMB;
    }

    public int getMaxMergeMB() {
        return maxMergeMB;
    }

    public long getBloomReadSaves() {
        return bloomReadSaves;
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

    public static Fieldable newIndexed(String name, String value) {
        return new Field(name, value, Field.Store.NO, Field.Index.ANALYZED);
    }

    public static Fieldable newStored(String name, String value) {
        return new Field(name, value, Field.Store.YES, Field.Index.NOT_ANALYZED_NO_NORMS);
    }    
}
