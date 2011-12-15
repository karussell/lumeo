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
import de.jetsli.lumeo.util.Helper;
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
        refresh();

        EdgeVertexBoundSequence eSeq = new EdgeVertexBoundSequence(g, (LuceneVertex) v1, RawLucene.EDGE_IN);
        assertCount(0, eSeq);

        Vertex v3 = g.addVertex("jetslideapp");
        g.addEdge("idEdge2", v3, v1, "twitteraccounting");
        refresh();

        eSeq = new EdgeVertexBoundSequence(g, (LuceneVertex) v1, RawLucene.EDGE_IN);
        assertCount(1, eSeq);

        eSeq = new EdgeVertexBoundSequence(g, (LuceneVertex) v1);
        assertCount(2, eSeq);

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
        refresh();

        EdgeVertexBoundSequence eSeq = new EdgeVertexBoundSequence(g, (LuceneVertex) v1, RawLucene.EDGE_IN).setLabels("twitteraccounting");
        assertCount(1, eSeq);

        eSeq = new EdgeVertexBoundSequence(g, (LuceneVertex) v1, RawLucene.EDGE_IN).setLabels("twitteraccount");
        assertCount(0, eSeq);

        eSeq = new EdgeVertexBoundSequence(g, (LuceneVertex) v1).setLabels("twitteraccounting");
        assertCount(1, eSeq);
    }
    
    @Test public void testCaseInsensitive() {
        g.createAutomaticIndex("tmp", Edge.class, Helper.set("name"));
        Vertex v1 = g.addVertex("peter");
        Vertex v2 = g.addVertex("timetabling");
        Edge e1 = g.addEdge("idEdge", v1, v2, "twitteraccount");
        e1.setProperty("name", "World");

        Vertex v3 = g.addVertex("jetslideapp");
        Edge e2 = g.addEdge("idEdge2", v3, v1, "twitteraccounting");
        e2.setProperty("name", "hellO");
        refresh();

        LuceneFilterSequence<Edge> eSeq = new EdgeVertexBoundSequence(g, (LuceneVertex) v1, 
                RawLucene.EDGE_IN).setValue("name", "Hello");
        assertCount(1, eSeq);

        eSeq = new EdgeVertexBoundSequence(g, (LuceneVertex) v1).setValue("name", "worlD");
        assertCount(1, eSeq);              
        
        // no stemming per default only lower case
        eSeq = new EdgeVertexBoundSequence(g, (LuceneVertex) v1).setValue("name", "worlds");
        assertCount(0, eSeq);
    }
}
