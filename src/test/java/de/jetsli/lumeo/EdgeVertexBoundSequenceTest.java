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

import com.tinkerpop.blueprints.pgm.Edge;
import com.tinkerpop.blueprints.pgm.Vertex;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author Peter Karich, info@jetsli.de
 */
public class EdgeVertexBoundSequenceTest extends SimpleLuceneTestBase {

    @Test public void testSequence() {
        Vertex v1 = g.addVertex(1L);
        Vertex v2 = g.addVertex(2L);
        g.addEdge(3L, v1, v2, "twitteraccount");
        flushAndRefresh();

        EdgeVertexBoundSequence eSeq = new EdgeVertexBoundSequence(g, (LuceneVertex) v1, RawLucene.EDGE_IN);
        assertCount(0, eSeq);

        Vertex v3 = g.addVertex(5L);
        g.addEdge(4L, v3, v1, "twitteraccounting");
        flushAndRefresh();

        eSeq = new EdgeVertexBoundSequence(g, (LuceneVertex) v1, RawLucene.EDGE_IN);
        assertCount(1, eSeq);

        eSeq = new EdgeVertexBoundSequence(g, (LuceneVertex) v1);
        assertCount(2, eSeq);

        eSeq = new EdgeVertexBoundSequence(g, (LuceneVertex) v1, RawLucene.EDGE_IN, RawLucene.EDGE_OUT);
        assertTrue(eSeq.hasNext());
        Edge e = eSeq.next();
        assertEquals("twitteraccount", e.getLabel());
        assertNotNull(e.getId());
        assertTrue("id should of type String", e.getId() instanceof String);
        assertTrue(eSeq.hasNext());
        eSeq.next();
        assertFalse(eSeq.hasNext());
    }

    @Test public void testSequenceWithLabels() {
        Vertex v1 = g.addVertex(1L);
        Vertex v2 = g.addVertex(2L);
        g.addEdge(3L, v1, v2, "twitteraccount");

        Vertex v3 = g.addVertex(5L);
        g.addEdge(4L, v3, v1, "twitteraccounting");
        flushAndRefresh();

        EdgeVertexBoundSequence eSeq = new EdgeVertexBoundSequence(g, (LuceneVertex) v1, RawLucene.EDGE_IN).setLabels("twitteraccounting");
        assertCount(1, eSeq);

        eSeq = new EdgeVertexBoundSequence(g, (LuceneVertex) v1, RawLucene.EDGE_IN).setLabels("twitteraccount");
        assertCount(0, eSeq);

        eSeq = new EdgeVertexBoundSequence(g, (LuceneVertex) v1).setLabels("twitteraccounting");
        assertCount(1, eSeq);
    }
}
