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

    private static RuntimeException UNSUPP_TYPE = new UnsupportedOperationException("indexClass must be from type Edge or Vertex (or a sublcass)");
    private final Class<T> indexClass;
    protected final LuceneGraph graph;
    private final String indexName;

    /** at the moment we have only automatic indices */
    protected LuceneIndex(final String indexName, final Class<T> indexClass, final LuceneGraph graph) {
        this.indexClass = indexClass;
        this.graph = graph;
        this.indexName = indexName;

        if (!Vertex.class.isAssignableFrom(indexClass) && !Edge.class.isAssignableFrom(indexClass))
            throw UNSUPP_TYPE;
    }

    @Override public Type getIndexType() {
        return Type.MANUAL;
    }

    @Override public Class<T> getIndexClass() {
        return indexClass;
    }

    @Override public String getIndexName() {
        return this.indexName;
    }

    @Override public void put(final String key, final Object value, final T element) {
        element.setProperty(key, value);        
    }

    @Override public CloseableSequence<T> get(final String key, final Object value) {
        if (Vertex.class.isAssignableFrom(indexClass)) {
            return (CloseableSequence<T>) new VertexFilterSequence(graph);
        } else if (Edge.class.isAssignableFrom(indexClass)) {
            return (CloseableSequence<T>) new EdgeVertexTraversalSequence(graph);
        } else
            throw UNSUPP_TYPE;
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
