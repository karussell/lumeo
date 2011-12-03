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
package com.pannous.tmpo;

import com.tinkerpop.blueprints.pgm.CloseableSequence;
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
        Vertex v1 = g.addVertex("peter");
        Vertex v2 = g.addVertex("timetabling");
        g.addEdge("idEdge", v1, v2, "twitteraccount");
        flushAndRefresh();

        EdgeVertexBoundSequence eSeq = new EdgeVertexBoundSequence(g, (LuceneVertex) v1, RawLucene.EDGE_IN);
        assertSeqLength(0, eSeq);

        Vertex v3 = g.addVertex("jetslideapp");
        g.addEdge("idEdge2", v3, v1, "twitteraccounting");
        flushAndRefresh();

        eSeq = new EdgeVertexBoundSequence(g, (LuceneVertex) v1, RawLucene.EDGE_IN);
        assertSeqLength(1, eSeq);

        eSeq = new EdgeVertexBoundSequence(g, (LuceneVertex) v1);
        assertSeqLength(2, eSeq);

        eSeq = new EdgeVertexBoundSequence(g, (LuceneVertex) v1, RawLucene.EDGE_IN, RawLucene.EDGE_OUT);
        assertTrue(eSeq.hasNext());
        Edge e = eSeq.next();
        assertEquals("twitteraccount", e.getLabel());
        assertNotNull(e.getId());
        assertTrue("id should of type long", e.getId() instanceof Long);
        assertTrue(eSeq.hasNext());
        eSeq.next();
        assertFalse(eSeq.hasNext());
    }

    @Test public void testSequenceWithLabels() {
        Vertex v1 = g.addVertex("peter");
        Vertex v2 = g.addVertex("timetabling");
        g.addEdge("idEdge", v1, v2, "twitteraccount");

        Vertex v3 = g.addVertex("jetslideapp");
        g.addEdge("idEdge2", v3, v1, "twitteraccounting");
        flushAndRefresh();

        EdgeVertexBoundSequence eSeq = new EdgeVertexBoundSequence(g, (LuceneVertex) v1, RawLucene.EDGE_IN).setLabels("twitteraccounting");
        assertSeqLength(1, eSeq);

        eSeq = new EdgeVertexBoundSequence(g, (LuceneVertex) v1, RawLucene.EDGE_IN).setLabels("twitteraccount");
        assertSeqLength(0, eSeq);

        eSeq = new EdgeVertexBoundSequence(g, (LuceneVertex) v1).setLabels("twitteraccounting");
        assertSeqLength(1, eSeq);
    }

    void assertSeqLength(int expected, CloseableSequence seq) {
        int counter = 0;
        while (seq.hasNext()) {
            seq.next();
            counter++;
        }
        assertEquals("length of sequence mismatch", expected, counter);
    }
}
