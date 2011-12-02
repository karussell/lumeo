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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.document.NumericField;

/**
 *
 * @author Peter Karich, info@jetsli.de
 */
class EdgeSequence implements CloseableSequence<Edge> {

    private LuceneGraph g;
    private List<Fieldable> fieldables;
    private int index = 0;

    public EdgeSequence(LuceneGraph g, Vertex v, String... edgeTypes) {
        this(g, ((LuceneVertex) v).getRaw(), edgeTypes);
    }

    public EdgeSequence(LuceneGraph g, Document doc, String... edgeTypes) {
        if (doc == null)
            throw new NullPointerException("Document shouldn't be null");
        this.g = g;
        if (edgeTypes == null || edgeTypes.length == 0)
            edgeTypes = new String[]{RawLucene.EDGE_IN, RawLucene.EDGE_OUT};

        fieldables = new ArrayList<Fieldable>();
        for (Fieldable field : doc.getFields()) {
            if (field.name().equals(edgeTypes[0])
                    || edgeTypes.length > 1 && field.name().equals(edgeTypes[1]))
                fieldables.add(field);
        }
    }

    @Override
    public void close() {
    }

    @Override
    public boolean hasNext() {
        return index < fieldables.size();
    }

    @Override
    public Edge next() {
        if (!hasNext())
            throw new UnsupportedOperationException("no further element");
        long id = ((NumericField) fieldables.get(index++)).getNumericValue().longValue();
        return new LuceneEdge(g, g.getRaw().findById(id));
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Iterator<Edge> iterator() {
        return this;
    }
}
