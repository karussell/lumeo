package com.pannous.tmpo;


import com.tinkerpop.blueprints.pgm.CloseableSequence;
import com.tinkerpop.blueprints.pgm.Edge;

import java.util.Iterator;
import org.apache.lucene.document.Document;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class LuceneEdgeSequence<T extends Edge> implements CloseableSequence<LuceneEdge> {

    private final Iterator<Document> relationships;
    private final LuceneGraph graph;

    // TODO unused
    private LuceneEdgeSequence(final Iterable<Document> relationships, final LuceneGraph graph) {
        this.graph = graph;
        this.relationships = relationships.iterator();
    }

    @Override public void remove() {
        throw new UnsupportedOperationException();
    }

    @Override public LuceneEdge next() {
        return new LuceneEdge(this.relationships.next(), this.graph);
    }

    @Override public boolean hasNext() {
        return this.relationships.hasNext();
    }

    @Override public Iterator<LuceneEdge> iterator() {
        return this;
    }

    @Override public void close() {        
    }
}