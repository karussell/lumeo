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
package com.pannous.lumeo;

import com.tinkerpop.blueprints.pgm.Vertex;
import java.util.Iterator;
import org.apache.lucene.document.Document;

/**
 *
 * @author Peter Karich, info@jetsli.de
 */
public class SingleVertexSequence extends VertexFilterSequence {

    private boolean hasNext = true;
    private Vertex v;
    
    public SingleVertexSequence(LuceneGraph g, Vertex v) {
        super(g);
        if (v == null)
            throw new NullPointerException("Vertex shouldn't be null");
        this.v = v;
    }

    public SingleVertexSequence(LuceneGraph g, Document doc) {
        this(g, new LuceneVertex(g, doc));
    }

    @Override public void close() {
    }

    @Override public boolean hasNext() {
        return hasNext;
    }

    @Override public Vertex next() {
        hasNext = false;
        return v;    
    }

    @Override public void remove() {
        g.removeVertex(v);
    }

    @Override public Iterator<Vertex> iterator() {
        return this;
    }
}
