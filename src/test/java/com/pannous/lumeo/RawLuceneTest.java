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
package com.pannous.lumeo;

import com.pannous.lumeo.util.SearchExecutor;
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

    @Test public void testCount() {
        RawLucene rl = g.getRaw();

        Document doc = new Document();
        doc.add(RawLucene.newIdField("xy", 12L));
        doc.add(RawLucene.newStringField("name", "peter"));
        rl.put("idSomething", 1, doc);

        doc = new Document();
        doc.add(RawLucene.newIdField("xy", 1L));
        doc.add(RawLucene.newStringField("name", "peter"));
        rl.put("idSomething2", 2, doc);

        doc = new Document();
        doc.add(RawLucene.newStringField("name", "peter 2"));
        rl.put("idSomething3", 3, doc);

        flushAndRefresh();
        assertEquals(1, rl.count("xy", 12L));
        assertEquals(2, rl.count("name", "peter"));
    }

    @Test public void testUpdateDoc() {
        RawLucene rl = g.getRaw();
        Document doc = new Document();        
        long id = 1;
        doc.add(RawLucene.newStringField("name", "peter"));        
        rl.put("myId", id, doc);
        flushAndRefresh();
        doc = rl.searchSomething(new SearchExecutor<Document>() {

            @Override public Document execute(IndexSearcher o) throws Exception {
                TopDocs td = o.search(new MatchAllDocsQuery(), 10);
                if (td.scoreDocs.length == 0)
                    throw new IllegalStateException("no doc found");
                if (td.scoreDocs.length > 1)
                    throw new IllegalStateException("no many docs found");

                return o.doc(td.scoreDocs[0].doc);
            }
        });
        assertEquals("peter", doc.get("name"));

        doc = new Document();
        doc.add(RawLucene.newIdField(RawLucene.ID, id));
        doc.add(RawLucene.newStringField("name", "different"));               
        rl.update(doc);        
        flushAndRefresh();
        doc = rl.searchSomething(new SearchExecutor<Document>() {

            @Override public Document execute(IndexSearcher o) throws Exception {
                TopDocs td = o.search(new MatchAllDocsQuery(), 10);
                if (td.scoreDocs.length == 0)
                    throw new IllegalStateException("no doc found");
                if (td.scoreDocs.length > 1)
                    throw new IllegalStateException("no many docs found");

                return o.doc(td.scoreDocs[0].doc);
            }
        });
        assertEquals("different", doc.get("name"));
    }
}
