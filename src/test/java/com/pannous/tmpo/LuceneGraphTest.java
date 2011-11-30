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

import java.util.Iterator;
import com.tinkerpop.blueprints.pgm.CloseableSequence;
import com.tinkerpop.blueprints.pgm.IndexableGraph;
import com.tinkerpop.blueprints.pgm.Vertex;
import java.util.LinkedHashSet;
import java.util.Set;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author Peter Karich, info@jetsli.de
 */
public class LuceneGraphTest {

    IndexableGraph g;

    @Before public void setUp() {
        g = new LuceneGraph();
    }

    @After public void tearDown() {
        g.shutdown();
    }

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

    @Test public void testAddVertex() {
        Vertex v = g.addVertex("peter");
        assertNotNull(v);
        assertNotNull(g.addVertex(null));
        
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

    @Test
    public void testGetEdges() {
    }

    @Test
    public void testRemoveVertex() {
    }

    @Test
    public void testAddEdge() {
    }

    @Test
    public void testGetEdge() {
    }

    @Test public void testRemoveEdge() {
    }

    @Test public void testClear() {
    }

    // skip for now
    @Test public void testCreateManualIndex() {
    }

    @Test public void testDropIndex() {
    }
}
