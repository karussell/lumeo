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

import com.tinkerpop.blueprints.pgm.Edge;
import java.util.Iterator;
import com.tinkerpop.blueprints.pgm.Vertex;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.QueryWrapperFilter;
import org.apache.lucene.search.TermQuery;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author Peter Karich, info@jetsli.de
 */
public class EdgeVertexTraversalSequenceTest {

    private LuceneGraph g;

    @Before public void setUp() {
        g = new LuceneGraph();
    }

    @Test public void testIterator() {
        Vertex x1 = g.addVertex("test");
        Vertex x2 = g.addVertex("test2");
        Edge e = g.addEdge(null, x1, x2, "association");
        EdgeVertexTraversalSequence seq = new EdgeVertexTraversalSequence(g);
        Iterator<Edge> iter = seq.iterator();
        assertTrue(iter.hasNext());
        assertEquals(e, iter.next());
        assertFalse(iter.hasNext());
    }

    @Test public void testIteratorWithVertexFilter() {
        Vertex x1 = g.addVertex("final");
        x1.setProperty("hello", "world");
        Vertex x2 = g.addVertex("countdown");
        final Edge e = g.addEdge(null, x1, x2, "association");
        x1 = g.addVertex("test");
        x1.setProperty("hello", "test");
        x2 = g.addVertex("test2");
        g.addEdge(null, x1, x2, "association2");
        EdgeVertexTraversalSequence seq = new EdgeVertexTraversalSequence(g).
                setFilter(new QueryWrapperFilter(new TermQuery(new Term("hello", "world"))));
        Iterator<Edge> iter = seq.iterator();
        assertTrue(iter.hasNext());
        assertEquals(e, iter.next());
        assertFalse(iter.hasNext());
    }

    @Test
    public void testClose() {
        EdgeVertexTraversalSequence seq = new EdgeVertexTraversalSequence(g);
        seq.close();
    }
}
