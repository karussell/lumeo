/*
 *  Copyright 2011 Peter Karich, info@jetsli.de
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
package de.jetsli.lumeo;

import org.apache.lucene.index.codecs.PostingsFormat;
import org.apache.lucene.index.codecs.lucene40.Lucene40Codec;
import org.apache.lucene.index.codecs.pulsing.Pulsing40PostingsFormat;
import org.apache.lucene.search.NRTManager;
import org.apache.lucene.search.SearcherWarmer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;
import de.jetsli.lumeo.util.LumeoPerFieldAnalyzer;
import org.apache.lucene.document.NumericField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.Term;
import de.jetsli.lumeo.util.LuceneHelper;
import de.jetsli.lumeo.util.Mapping;
import de.jetsli.lumeo.util.SearchExecutor;
import java.io.IOException;
import org.apache.lucene.document.Document;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * @author Peter Karich, info@jetsli.de
 */
public class RawLuceneTest extends SimpleLuceneTestBase {

    Mapping m;

    @Override public void setUp() {
        super.setUp();
        m = g.getRaw().getMapping(Tmp.class.getSimpleName());
        m.putField("xy", Mapping.Type.LONG);
        m.putField("name", Mapping.Type.STRING);
    }

    static class Tmp {
    }

    @Test public void testSimpleCount() throws IOException {
        RawLucene rl = g.getRaw();
        Document doc = rl.createDocument("tmp1", 1, Tmp.class);
        doc.add(m.createField("xy", 12L));
        rl.put("tmp1", 1, doc);
        refresh();
        assertEquals(1, rl.count(Tmp.class, "xy", 12L));
    }

    @Test public void testCountWithUpdate() {
        RawLucene rl = g.getRaw();
        Document doc = rl.createDocument("tmp1", 1, Tmp.class);
        doc.add(m.createField("xy", 12L));
        doc.add(m.createField("name", "peter"));
        rl.put("tmp1", 1, doc);

        doc = rl.createDocument("tmp2", 2, Tmp.class);
        doc.add(m.createField("xy", 1L));
        doc.add(m.createField("name", "peter"));
        rl.put("tmp2", 2, doc);

        doc = rl.createDocument("tmp2", 2, Tmp.class);
        doc.add(m.createField("xy", 12L));
        doc.add(m.createField("name", "peter 2"));
        rl.put("tmp2", 2, doc);

        refresh();
        assertEquals(2, rl.count(Tmp.class, "xy", 12L));
        assertEquals(1, rl.count(Tmp.class, "name", "peter"));
    }

    @Test public void testUpdateDoc() {
        RawLucene rl = g.getRaw();
        long id = 1;
        Document doc = rl.createDocument("myId", id, Tmp.class);
        doc.add(m.createField("name", "peter"));
        rl.put("myId", id, doc);
        refresh();
        doc = rl.searchSomething(new SearchExecutor<Document>() {

            @Override public Document execute(IndexSearcher o) throws Exception {
                TopDocs td = o.search(new MatchAllDocsQuery(), 10);
                if (td.scoreDocs.length == 0)
                    throw new IllegalStateException("no doc found");
                if (td.scoreDocs.length > 1)
                    throw new IllegalStateException("too many docs found");

                return o.doc(td.scoreDocs[0].doc);
            }
        });
        assertEquals("peter", doc.get("name"));

        doc = rl.createDocument("myId", id, Tmp.class);
        doc.add(m.createField("name", "different"));
        rl.put("myId", id, doc);
        refresh();
        doc = rl.searchSomething(new SearchExecutor<Document>() {

            @Override public Document execute(IndexSearcher o) throws Exception {
                TopDocs td = o.search(new MatchAllDocsQuery(), 10);
                if (td.scoreDocs.length == 0)
                    throw new IllegalStateException("no doc found");
                if (td.scoreDocs.length > 1)
                    throw new IllegalStateException("too many docs found");

                return o.doc(td.scoreDocs[0].doc);
            }
        });
        assertEquals("different", doc.get("name"));
    }

    @Test public void testRealtimeGetShouldWorkEvenAfterFlushWithoutRefresh() throws IOException {
        RawLucene rl = g.getRaw();
        long id = 1;
        Document doc = rl.createDocument("myId", id, Tmp.class);
        doc.add(m.createField("name", "peter"));
        rl.put("myId", id, doc);

        // realtime get
        assertNotNull(rl.findById(id));

        // no refresh but wait
        rl.waitUntilSearchable();

        // get and search via lucene
        assertNotNull(rl.findById(id));
        assertEquals(1, rl.count(Tmp.class, "name", "peter"));
    }

    @Test public void testFindByIdWithoutRefresh() {
        RawLucene rl = g.getRaw();
        Document doc = rl.createDocument("test", 123, Tmp.class);
        rl.put("test", 123, doc);

        // no refresh but wait
        rl.waitUntilSearchable();

        assertNotNull(rl.findById(123));
        assertEquals(0, rl.count(Tmp.class, "name", "peter"));
        assertNotNull("UserId 'test' should be available", rl.findByUserId("test"));
    }
}
