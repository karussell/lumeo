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
package de.jetsli.lumeo;

import de.jetsli.lumeo.util.Helper;
import de.jetsli.lumeo.util.TermFilter;
import com.tinkerpop.blueprints.pgm.Edge;
import java.util.Iterator;
import com.tinkerpop.blueprints.pgm.Vertex;
import org.apache.lucene.index.Term;
import org.apache.lucene.util.BytesRef;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author Peter Karich, info@jetsli.de
 */
public class EdgeFilterSequenceTest extends SimpleLuceneTestBase {

    @Test public void testIterator() {
        g.createAutomaticIndex("edges", Edge.class, Helper.<String>set("association"));
        Vertex x1 = g.addVertex("test");
        Vertex x2 = g.addVertex("test2");
        Edge e = g.addEdge(null, x1, x2, "association");
        refresh();

        EdgeFilterSequence seq = new EdgeFilterSequence(g);
        Iterator<Edge> iter = seq.iterator();
        assertTrue(iter.hasNext());
        assertEquals(e, iter.next());
        assertFalse(iter.hasNext());
    }

    @Test public void testIteratorWithVertexFilter() {
        g.createAutomaticIndex("edges", Edge.class, Helper.<String>set("hello"));
        g.createAutomaticIndex("vertices", Vertex.class, Helper.<String>set("hellov"));
        Vertex x1 = g.addVertex("final");
        Vertex x2 = g.addVertex("countdown");
        Edge e = g.addEdge(null, x1, x2, "association");
        e.setProperty("hello", "test");
        e.setProperty("hellov", "test");

        Vertex x3 = g.addVertex("test");
        x3.setProperty("hello", "test");
        e = g.addEdge(null, x1, x3, "association2");
        e.setProperty("hello", "world");
        e.setProperty("hellov", "world");
        refresh();

        assertCount(1, new EdgeFilterSequence(g).setFilter(new TermFilter("hello", new BytesRef("world"))));

        // hellov gets not indexed
        assertCount(0, new EdgeFilterSequence(g).setFilter(new TermFilter("hellov", new BytesRef("world"))));

        // ... but stored
        e = new EdgeFilterSequence(g).setFilter(new TermFilter("hello", new BytesRef("world"))).next();
        assertEquals("world", e.getProperty("hellov"));
    }

    @Test
    public void testClose() {
        EdgeFilterSequence seq = new EdgeFilterSequence(g);
        seq.close();
    }
}
