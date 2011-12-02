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

import com.tinkerpop.blueprints.pgm.Edge;
import com.tinkerpop.blueprints.pgm.CloseableSequence;
import com.tinkerpop.blueprints.pgm.Index;
import com.tinkerpop.blueprints.pgm.Vertex;
import org.apache.lucene.document.Document;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * @author Peter Karich, info@jetsli.de
 */
public class LuceneIndexTest extends SimpleLuceneTestBase {

    @Test
    public void testPutVertex() {
        Index<Vertex> index = g.createAutomaticIndex("", Vertex.class, null);
        Vertex v = new LuceneVertex(g, new Document());
        index.put("name", "peter", v);

        CloseableSequence<Vertex> seq = index.get("name", "peter");
        assertTrue(seq.hasNext());
        seq.next();
        assertFalse(seq.hasNext());
        seq.close();
    }

    @Test public void testPutEdge() {
        Index<Edge> index = g.createAutomaticIndex("", Edge.class, null);
        Edge v = new LuceneEdge(g, new Document());
        index.put("name", "peter", v);

        CloseableSequence<Edge> seq = index.get("name", "peter");
        assertTrue(seq.hasNext());
        seq.next();
        assertFalse(seq.hasNext());
        seq.close();
    }

    @Test
    public void testCount() {
    }

    @Test
    public void testRemove() {
    }

    @Test
    public void testToString() {
    }
}
