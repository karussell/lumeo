/*
 *  Copyright 2011 Peter Karich jetwick_@_pannous_._info
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

import com.pannous.lumeo.util.Helper;
import com.tinkerpop.blueprints.pgm.AutomaticIndex;
import com.tinkerpop.blueprints.pgm.CloseableSequence;
import com.tinkerpop.blueprints.pgm.Vertex;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * @author Peter Karich, info@jetsli.de
 */
public class LuceneGraphTest extends SimpleLuceneTestBase {

    @Test public void testCreateAutomaticIndex() {
        String indexName = "index1";
        g.createAutomaticIndex(indexName, Vertex.class, Helper.set("fieldA"));
        Vertex v = g.addVertex("peter");
        v.setProperty("fieldA", "test");
        v.setProperty("fieldB", "pest");
        flushAndRefresh();

        CloseableSequence<Vertex> seq = g.getIndex(indexName, Vertex.class).get("fieldA", "pest");
        assertCount(0, seq);

        seq = g.getIndex(indexName, Vertex.class).get("fieldA", "test");
        assertCount(1, seq);

        try {
            seq = g.getIndex(indexName, Vertex.class).get("fieldB", "pest");
            assertTrue("fieldB should be not supported by this index", false);
        } catch (UnsupportedOperationException ex) {
        }
    }

    @Test public void testAutoUpdateVertex() {
        Vertex v = g.addVertex("peter");
        assertNotNull(v);
        AutomaticIndex<Vertex> index = g.createAutomaticIndex("vertex", Vertex.class, Helper.set("fullname", "fullname2,TEXT"));
        v.setProperty("fullname", "peter something");
        flushAndRefresh();

        CloseableSequence<Vertex> seq = index.get("fullname", "peter something");
        assertCount(1, seq);

        seq = index.get("fullname", "peter");
        assertCount(0, seq);

        // Now do some lucene magic ...
        v.setProperty("fullname2", "peter something");
        v.setProperty("fullname3", "peter some thing");
        flushAndRefresh();

        // ... and search via StandardAnalyzer due to ",TEXT"
        seq = index.get("fullname2", "peter");
        assertCount(1, seq);

        try {
            g.createAutomaticIndex("vertex", Vertex.class, Helper.set("fullname", "fullname,TEXT"));
            assertFalse("exception should be raised for multiple key definition", true);
        } catch (Exception ex) {
        }

        try {
            seq = index.get("unsupportedkey", "peter");
            assertFalse("exception should be raised for unsupported property", true);
        } catch (Exception ex) {
        }
    }

    // not sure if we should really introduce this notation, as it is not clear weather
    // the field name is fullname or fullname_t    
//    @Test public void testUnderscoreNotation() {        
//        try {
//            g.createAutomaticIndex("vertex", Vertex.class, Helper.set("fullname", "fullname_t"));
//            assertFalse("exception should be raised for multiple key definition", true);
//        } catch (Exception ex) {
//        }
//        g.createAutomaticIndex("vertex", Vertex.class, Helper.set("fullname3"));
//        seq = index.get("fullname3", "thing");
//        assertCount(1, seq);
//    }
    
    @Test public void testAddAndRemoveVertex() {
        Vertex v = g.addVertex("peter");
        assertNotNull(v);
        assertNotNull(g.addVertex(null));
        flushAndRefresh();

        Vertex tmp = g.getVertex("peter");
        assertNotNull(tmp);
        assertEquals(v, tmp);
        flushAndRefresh();

        g.removeVertex(tmp);
        flushAndRefresh();
        assertNull(g.getVertex("peter"));

        assertCount(1, g.getVertices());
    }
}
