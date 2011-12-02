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

import com.pannous.tmpo.util.Helper;
import com.tinkerpop.blueprints.pgm.CloseableSequence;
import com.tinkerpop.blueprints.pgm.Edge;
import java.util.Iterator;
import org.apache.lucene.search.Filter;

/**
 * Class traverses all vertices (or a subset if filter is specified) and returns
 * all edges
 * 
 * @author Peter Karich, info@jetsli.de
 */
class EdgeVertexTraversalSequence implements CloseableSequence<Edge> {

    private VertexFilterSequence vertices;
    private CloseableSequence<Edge> edgeSeq;
    private LuceneGraph g;

    EdgeVertexTraversalSequence(LuceneGraph g) {
        vertices = new VertexFilterSequence(g);
        this.g = g;
    }

    public EdgeVertexTraversalSequence setFilter(Filter f) {
        vertices.setFilter(f);
        return this;
    }

    CloseableSequence<Edge> renewSeq() {
        if (edgeSeq != null)
            edgeSeq.close();
        return new EdgeSequence(g, ((LuceneVertex) vertices.next()).getRaw());
    }

    @Override
    public Iterator<Edge> iterator() {
        if (!vertices.hasNext())
            return Helper.EMPTY_EDGE_SEQUENCE.iterator();
        return this;
    }

    @Override public boolean hasNext() {
        if (edgeSeq == null) {
            if (vertices.hasNext())
                edgeSeq = renewSeq();
            else
                edgeSeq = Helper.EMPTY_EDGE_SEQUENCE;
        }

        if (!vertices.hasNext()) {
            if (edgeSeq.hasNext())
                return true;
            return false;
        }

        edgeSeq = renewSeq();
        if (edgeSeq == null || !edgeSeq.hasNext())
            return false;

        return true;
    }

    @Override
    public Edge next() {
        return edgeSeq.next();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void close() {
        if (edgeSeq != null)
            edgeSeq.close();
        vertices.close();
    }
}
