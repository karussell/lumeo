package com.pannous.tmpo;

import com.tinkerpop.blueprints.pgm.CloseableSequence;
import com.tinkerpop.blueprints.pgm.Edge;
import com.tinkerpop.blueprints.pgm.Element;
import com.tinkerpop.blueprints.pgm.Index;
import com.tinkerpop.blueprints.pgm.Vertex;
import com.tinkerpop.blueprints.pgm.impls.StringFactory;

/**
 * @author Peter Karich, info@jetsli.de
 */
public class LuceneIndex<T extends Element> implements Index<T> {

    private final Class<T> indexClass;
    protected final LuceneGraph graph;
    private final String indexName;    

    public LuceneIndex(final String indexName, final Class<T> indexClass, final LuceneGraph graph) {
        this.indexClass = indexClass;
        this.graph = graph;
        this.indexName = indexName;        
    }

    @Override public Type getIndexType() {
        return Type.MANUAL;
    }

    @Override public Class<T> getIndexClass() {
        if (Vertex.class.isAssignableFrom(this.indexClass))
            return (Class) Vertex.class;
        else
            return (Class) Edge.class;
    }

    @Override public String getIndexName() {
        return this.indexName;
    }

    @Override public void put(final String key, final Object value, final T element) {
        // graph.updateDocument(element);
    }

    @Override public CloseableSequence<T> get(final String key, final Object value) {
        throw new UnsupportedOperationException();
    }

    @Override public long count(final String key, final Object value) {
        throw new UnsupportedOperationException();
    }

    @Override public void remove(final String key, final Object value, final T element) {        
    }

    protected void removeBasic(final String key, final Object value, final T element) {
        
    }

    protected void putBasic(final String key, final Object value, final T element) {
        
    }

    @Override public String toString() {
        return StringFactory.indexString(this);
    }
}
