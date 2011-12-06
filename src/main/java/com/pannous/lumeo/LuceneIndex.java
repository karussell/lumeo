package com.pannous.lumeo;

import com.tinkerpop.blueprints.pgm.CloseableSequence;
import com.tinkerpop.blueprints.pgm.Edge;
import com.tinkerpop.blueprints.pgm.Element;
import com.tinkerpop.blueprints.pgm.Index;
import com.tinkerpop.blueprints.pgm.Vertex;
import com.tinkerpop.blueprints.pgm.impls.StringFactory;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Fieldable;

/**
 * @author Peter Karich, info@jetsli.de
 */
public class LuceneIndex<T extends Element> implements Index<T> {

    private static String UNSUPP_TYPE = "indexClass must be from type Edge or Vertex (or a sublcass)";
    private final Class<T> indexClass;
    protected final LuceneGraph g;

    /** at the moment we have only automatic indices */
    protected LuceneIndex(LuceneGraph graph, Class<T> indexClass) {
        this.indexClass = indexClass;
        this.g = graph;

        if (!Vertex.class.isAssignableFrom(indexClass) && !Edge.class.isAssignableFrom(indexClass))
            throw new RuntimeException(UNSUPP_TYPE + ":" + indexClass.getSimpleName());
    }

    @Override public Type getIndexType() {
        return Type.MANUAL;
    }

    @Override public Class<T> getIndexClass() {
        return indexClass;
    }

    @Override public String getIndexName() {
        return getIndexClass().getSimpleName();
    }

    @Override public CloseableSequence<T> get(final String key, final Object value) {
        if (Vertex.class.isAssignableFrom(indexClass)) {
            return (CloseableSequence<T>) new VertexFilterSequence(g).setValue(key, value);
        } else if (Edge.class.isAssignableFrom(indexClass)) {
            return (CloseableSequence<T>) new EdgeFilterSequence(g).setValue(key, value);
        } else
            throw new RuntimeException(UNSUPP_TYPE + ":" + indexClass.getSimpleName());
    }

    @Override public long count(final String key, final Object value) {
        return g.count(key, value);
    }

    @Override public void remove(final String key, final Object value, final T element) {
        element.removeProperty(key);
    }

    @Override public void put(final String key, final Object value, final T element) {
        element.setProperty(key, value);
    }

    protected void removeField(String key, T element) {
        ((LuceneElement) element).getRaw().removeField(key);
    }

    protected void putField(Fieldable field, T element) {
        ((LuceneElement) element).getRaw().add(field);
    }

    @Override public String toString() {
        return StringFactory.indexString(this);
    }
}
