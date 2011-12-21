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

import org.apache.lucene.search.NRTManagerReopenThread;
import java.io.IOException;
import org.apache.lucene.search.NRTManager;
import org.apache.lucene.search.SearcherWarmer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.Version;
import org.junit.Test;
import static org.junit.Assert.*;

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
        IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(Version.LUCENE_40, new KeywordAnalyzer()));

        Document d = new Document();
        d.add(new NumericField("id", 6, NumericField.TYPE_STORED).setLongValue(1234));
        d.add(new NumericField("tmp", 6, NumericField.TYPE_STORED).setLongValue(1111));
        w.addDocument(d);

        d = new Document();
        d.add(new NumericField("id", 6, NumericField.TYPE_STORED).setLongValue(1234));
        d.add(new NumericField("tmp", 6, NumericField.TYPE_STORED).setLongValue(2222));
        w.updateDocument(getTerm("id", 1234), d);

        d = new Document();
        d.add(new NumericField("id", 6, NumericField.TYPE_STORED).setLongValue(0));
        w.addDocument(d);
        w.commit();

        IndexReader reader = IndexReader.open(w, true);
        IndexSearcher searcher = new IndexSearcher(reader);

        BytesRef bytes = new BytesRef();
        NumericUtils.longToPrefixCoded(1234, 0, bytes);
        TopDocs td = searcher.search(new TermQuery(new Term("id", bytes)), 10);
        assertEquals(1, td.totalHits);
        assertEquals("1234", searcher.doc(td.scoreDocs[0].doc).get("id"));
        assertEquals("2222", searcher.doc(td.scoreDocs[0].doc).get("tmp"));
        w.close();
    }

    @Test public void testTermMatchingNrt() throws Exception {
        RAMDirectory dir = new RAMDirectory();
        IndexWriterConfig cfg = new IndexWriterConfig(Version.LUCENE_40, new KeywordAnalyzer());
        IndexWriter w = new IndexWriter(dir, cfg);

        NRTManager nrtManager = new NRTManager(w, new SearcherWarmer() {

            @Override public void warm(IndexSearcher s) throws IOException {
                // TODO do some warming
            }
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

        Document d = new Document();
        d.add(new NumericField("id", 6, NumericField.TYPE_STORED).setLongValue(1234));
        d.add(new NumericField("tmp", 6, NumericField.TYPE_STORED).setLongValue(1111));
        long latestGen = nrtManager.updateDocument(getTerm("id", 1234), d, analyzer);

        d = new Document();
        d.add(new NumericField("id", 6, NumericField.TYPE_STORED).setLongValue(1234));
        d.add(new NumericField("tmp", 6, NumericField.TYPE_STORED).setLongValue(2222));
        latestGen = nrtManager.updateDocument(getTerm("id", 1234), d, analyzer);

        d = new Document();
        d.add(new NumericField("id", 6, NumericField.TYPE_STORED).setLongValue(0));
        latestGen = nrtManager.updateDocument(getTerm("id", 0), d, analyzer);

        w.commit();
        nrtManager.maybeReopen(true);
//        nrtManager.waitForGeneration(latestGen, true);

        IndexSearcher searcher = nrtManager.getSearcherManager(true).acquire();
        try {
            TopDocs td = searcher.search(new TermQuery(getTerm("id", 1234)), 10);
            assertEquals(1, td.totalHits);
            assertEquals(1, td.scoreDocs.length);
            assertEquals("1234", searcher.doc(td.scoreDocs[0].doc).get("id"));
            assertEquals("2222", searcher.doc(td.scoreDocs[0].doc).get("tmp"));

            td = searcher.search(new TermQuery(getTerm("id", 0)), 10);
            assertEquals(1, td.totalHits);
        } finally {
//            reopenThread.close();
            nrtManager.getSearcherManager(true).release(searcher);
            w.close();
        }
    }

    Term getTerm(String field, long id) {
        return new Term(field, LuceneHelper.newRefFromLong(id));
    }
}
