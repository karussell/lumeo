/*
 *  Copyright 2011 Peter Karich info@jetsli.de
 * 
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 * 
 *       http://www.apache.org/licenses/LICENSE-2.0
 * 
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package de.jetsli.lumeo.util;

import de.jetsli.lumeo.RawLucene;
import static org.junit.Assert.assertEquals;

import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.LongField;
import org.apache.lucene.index.*;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.NRTManager;
import org.apache.lucene.search.NRTManager.TrackingIndexWriter;
import org.apache.lucene.search.SearcherFactory;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.Version;
import org.junit.Test;

/**
 *
 * @author Peter Karich, info@jetsli.de
 */
public class LuceneHelperTest {

    @Test public void testCopyLong() {
        long longVal = Long.MAX_VALUE / 2;
        BytesRef ref = new BytesRef();
        LuceneHelper.copyLong(ref, longVal);

        assertEquals(longVal, LuceneHelper.asLong(ref));
    }

    @Test public void testCopyInt() {
        int integer = Integer.MAX_VALUE / 2;
        BytesRef ref = new BytesRef();
        LuceneHelper.copyInt(ref, integer);

        assertEquals(integer, LuceneHelper.asInt(ref));
    }

    @Test public void testTermMatching() throws Exception {
        RAMDirectory dir = new RAMDirectory();
        IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(RawLucene.VERSION, new KeywordAnalyzer()));
        Document d = new Document();        
        
        FieldType ft = Mapping.getLongFieldType(true, true);
        d.add(new LongField("id", 1234, ft));
        d.add(new LongField("tmp", 1111, ft));
        w.addDocument(d);

        d = new Document();
        d.add(new LongField("id", 1234, ft));
        d.add(new LongField("tmp", 2222, ft));
        w.updateDocument(getTerm("id", 1234), d);

        d = new Document();
        d.add(new LongField("id", 0, ft));
        w.addDocument(d);
        w.commit();

        IndexReader reader = DirectoryReader.open(w, true);
        IndexSearcher searcher = new IndexSearcher(reader);

        BytesRef bytes = new BytesRef();
        NumericUtils.longToPrefixCoded(1234, 0, bytes);
        TopDocs td = searcher.search(new TermQuery(new Term("id", bytes)), 10);
        assertEquals(1, td.totalHits);
        assertEquals(1234L, searcher.doc(td.scoreDocs[0].doc).getField("id").numericValue());
        assertEquals(2222L, searcher.doc(td.scoreDocs[0].doc).getField("tmp").numericValue());
        w.close();
    }

    @Test public void testTermMatchingNrt() throws Exception {
        RAMDirectory dir = new RAMDirectory();
        IndexWriterConfig cfg = new IndexWriterConfig(Version.LUCENE_40, new KeywordAnalyzer());
        IndexWriter unwrappedWriter = new IndexWriter(dir, cfg);
        TrackingIndexWriter w=new TrackingIndexWriter(unwrappedWriter);

        NRTManager nrtManager = new NRTManager(w, new SearcherFactory() {

//            @Override public void warm(IndexSearcher s) throws IOException {
//                // TODO do some warming
//            }
        });

        LumeoPerFieldAnalyzer analyzer = new LumeoPerFieldAnalyzer(Mapping.KEYWORD_ANALYZER);
//        analyzer.putAnalyzer("id", Mapping.KEYWORD_ANALYZER);
//        analyzer.putAnalyzer("tmp", Mapping.KEYWORD_ANALYZER);

        // It is required to use a reopen thread otherwise waitForGeneration will block forever!        
        // If there are waiting searchers how long should reopen takes?
        double incomingSearchesMaximumWaiting = 0.03;
        // If there are no waiting searchers reopen it less frequent.        
        double ordinaryWaiting = 5.0;
//        NRTManagerReopenThread reopenThread = new NRTManagerReopenThread(nrtManager, ordinaryWaiting,
//                incomingSearchesMaximumWaiting);
//        reopenThread.setName("NRT Reopen Thread");
//        reopenThread.setDaemon(true);
//        reopenThread.start();

        FieldType ft = Mapping.getLongFieldType(true, true);
        Document d = new Document();
        d.add(new LongField("id", 1234, ft));
        d.add(new LongField("tmp", 1111, ft));
        long latestGen = w.updateDocument(getTerm("id", 1234), d, analyzer);

        d = new Document();
        d.add(new LongField("id", 1234, ft));
        d.add(new LongField("tmp", 2222, ft));
        latestGen = w.updateDocument(getTerm("id", 1234), d, analyzer);

        d = new Document();
        d.add(new LongField("id", 0, ft));
        latestGen = w.updateDocument(getTerm("id", 0), d, analyzer);

        w.getIndexWriter().commit();
        nrtManager.maybeRefreshBlocking();
//        nrtManager.waitForGeneration(latestGen, true);

        IndexSearcher searcher = nrtManager.acquire();
        try {
            TopDocs td = searcher.search(new TermQuery(getTerm("id", 1234)), 10);
            assertEquals(1, td.totalHits);
            assertEquals(1, td.scoreDocs.length);
            assertEquals(1234L, searcher.doc(td.scoreDocs[0].doc).getField("id").numericValue());
            assertEquals(2222L, searcher.doc(td.scoreDocs[0].doc).getField("tmp").numericValue());

            td = searcher.search(new TermQuery(getTerm("id", 0)), 10);
            assertEquals(1, td.totalHits);
        } finally {
//            reopenThread.close();
            nrtManager.release(searcher);
            w.getIndexWriter().close();
        }
    }

    Term getTerm(String field, long id) {
        return new Term(field, LuceneHelper.newRefFromLong(id));
    }
}
