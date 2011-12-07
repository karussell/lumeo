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
import com.tinkerpop.blueprints.pgm.Edge;
import com.tinkerpop.blueprints.pgm.CloseableSequence;
import com.tinkerpop.blueprints.pgm.Index;
import com.tinkerpop.blueprints.pgm.Vertex;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * @author Peter Karich, info@jetsli.de
 */
public class LuceneIndexTest extends SimpleLuceneTestBase {

    @Test public void testPutVertex() {
        Index<Vertex> index = g.createAutomaticIndex("keyword", Vertex.class, Helper.set("name"));
        Vertex v = g.addVertex(null);
        index.put("name", "peter", v);
        flushAndRefresh();

        CloseableSequence<Vertex> seq = index.get("name", "peter");
        assertTrue(seq.hasNext());
        seq.next();
        assertFalse(seq.hasNext());
        seq.close();
    }

    @Test public void testPutEdge() {
        Index<Edge> index = g.createAutomaticIndex("keyword", Edge.class, Helper.set("name"));
        Vertex v1 = g.addVertex(null);        
        Vertex v2 = g.addVertex(null);        
        Edge e = g.addEdge("tmp", v1, v2, "testing");
        index.put("name", "peter", e);
        flushAndRefresh();

        CloseableSequence<Edge> seq = index.get("name", "peter");
        assertTrue(seq.hasNext());
        seq.next();
        assertFalse(seq.hasNext());
        seq.close();
    }    
}
