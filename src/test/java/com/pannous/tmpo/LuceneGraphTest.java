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
package com.pannous.tmpo;

import com.tinkerpop.blueprints.pgm.AutomaticIndex;
import java.util.Iterator;
import com.tinkerpop.blueprints.pgm.CloseableSequence;
import com.tinkerpop.blueprints.pgm.Vertex;
import java.util.LinkedHashSet;
import java.util.Set;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author Peter Karich, info@jetsli.de
 */
public class LuceneGraphTest extends SimpleLuceneTestBase {

    @Test public void testCreateAutomaticIndex() {
        Set<String> set = new LinkedHashSet<String>();
        set.add("fieldA");
        String indexName = "index1";
        g.createAutomaticIndex(indexName, Vertex.class, set);
        Vertex v = g.addVertex("peter");
        v.setProperty("fieldA", "test");
        v.setProperty("fieldB", "pest");

        CloseableSequence<Vertex> seq = g.getIndex(indexName, Vertex.class).get("fieldB", "pest");
        assertFalse(seq.hasNext());

        seq = g.getIndex(indexName, Vertex.class).get("fieldA", "test");
        assertTrue(seq.hasNext());
        seq.next();
        assertFalse(seq.hasNext());
    }

    @Test public void testAutoUpdateVertex() {
        Vertex v = g.addVertex("peter");
        assertNotNull(v);
        Set<String> set = new LinkedHashSet<String>();
        set.add("fullname");
        AutomaticIndex<Vertex> index = g.createAutomaticIndex("keyword", Vertex.class, set);
        v.setProperty("fullname", "peter something");
        flushAndRefresh();
        CloseableSequence<Vertex> seq = index.get("fullname", "peter something");
        assertTrue(seq.hasNext());
        seq.next();
        assertFalse(seq.hasNext());
        
        seq = index.get("fullname", "peter");
        assertFalse(seq.hasNext());

        // Now do some lucene magic ...
        set.clear();        
        set.add("fullname_t");
        index = g.createAutomaticIndex("standard", Vertex.class, set);
        v.setProperty("fullname_t", "peter something");
        flushAndRefresh();
        // ... and search via StandardAnalyzer!        
        seq = index.get("fullname_t", "peter");
        assertTrue(seq.hasNext());
        seq.next();
        assertFalse(seq.hasNext());
    }

    @Test public void testAddVertex() {
        Vertex v = g.addVertex("peter");
        assertNotNull(v);
        assertNotNull(g.addVertex(null));

        g.getRaw().refresh();
        Vertex tmp = g.getVertex("peter");
        assertNotNull(tmp);
        assertEquals(v, tmp);

        g.removeVertex(tmp);
        assertNull(g.getVertex("peter"));

        Iterator<Vertex> iter = g.getVertices().iterator();
        assertTrue(iter.hasNext());
        iter.next();
        assertTrue(iter.hasNext());
        iter.next();
        assertFalse(iter.hasNext());
    }
}
