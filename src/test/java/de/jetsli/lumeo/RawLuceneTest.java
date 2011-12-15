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

import de.jetsli.lumeo.util.Mapping;
import de.jetsli.lumeo.util.SearchExecutor;
import java.io.IOException;
import org.apache.lucene.document.Document;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
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
        m = new Mapping(Tmp.class.getSimpleName());
        m.putField("xy", Mapping.Type.LONG);
        m.putField("name", Mapping.Type.STRING);
    }
    
    class Tmp {
    }

    @Test public void testCount() {
        RawLucene rl = g.getRaw();        
        Document doc = rl.createDocument("tmp1", 1, Tmp.class);
        doc.add(m.createField("xy", 12L));
        doc.add(m.createField("name", "peter"));
        rl.put("idSomething", 1, doc);

        doc = rl.createDocument("tmp2", 2, Tmp.class);
        doc.add(m.createField("xy", 1L));
        doc.add(m.createField("name", "peter"));
        rl.put("idSomething2", 2, doc);

        doc = rl.createDocument("tmp3", 3, Tmp.class);
        doc.add(m.createField("name", "peter 2"));
        rl.put("idSomething3", 3, doc);

        refresh();
        assertEquals(1, rl.count("xy", 12L));
        assertEquals(2, rl.count("name", "peter"));
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
        assertNotNull(rl.findById(id));
        rl.flush();
        // TODO do not clear buffer in flush but call clear in reopen thread if generation is ok!
        assertNotNull(rl.findById(id));        
    }    
}
