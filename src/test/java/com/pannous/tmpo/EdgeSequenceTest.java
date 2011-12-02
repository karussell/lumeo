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

import com.tinkerpop.blueprints.pgm.Vertex;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author Peter Karich, info@jetsli.de
 */
public class EdgeSequenceTest extends SimpleLuceneTestBase {

    @Test
    public void testRemove() {
    }

    @Test
    public void testIterator() {
        Vertex v1 = g.addVertex("peter");
        Vertex v2 = g.addVertex("timetabling");
        g.addEdge(null, v1, v2, "twitteraccount");

        EdgeSequence eSeq = new EdgeSequence(g, v1, RawLucene.EDGE_IN);
        assertFalse(eSeq.hasNext());

        eSeq = new EdgeSequence(g, v1, RawLucene.EDGE_IN, RawLucene.EDGE_OUT);
        assertTrue(eSeq.hasNext());
        assertEquals("twitteraccount", eSeq.next().getLabel());
        assertFalse(eSeq.hasNext());
    }
}
