package com.pannous.lumeo;

import com.tinkerpop.blueprints.pgm.CloseableSequence;
import com.tinkerpop.blueprints.pgm.Edge;
import com.tinkerpop.blueprints.pgm.Element;
import com.tinkerpop.blueprints.pgm.Index;
import com.tinkerpop.blueprints.pgm.Vertex;
import com.tinkerpop.blueprints.pgm.impls.StringFactory;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Field;

/**
 * @author Peter Karich, info@jetsli.de
 */
public class LuceneIndex<T extends Element> implements Index<T> {

    private static String UNSUPP_TYPE = "indexClass must be from type Edge or Vertex (or a sublcass)";
    private Map<String, Analyzer> mapping = new LinkedHashMap<String, Analyzer>();
    private final Class<T> indexClass;
    protected final LuceneGraph graph;

    /** at the moment we have only automatic indices */
    protected LuceneIndex(LuceneGraph graph, Class<T> indexClass) {
        this.indexClass = indexClass;
        this.graph = graph;

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
            return (CloseableSequence<T>) new VertexFilterSequence(graph).setValue(key, value);
        } else if (Edge.class.isAssignableFrom(indexClass)) {
            return (CloseableSequence<T>) new EdgeFilterSequence(graph).setValue(key, value);
        } else
            throw new RuntimeException(UNSUPP_TYPE + ":" + indexClass.getSimpleName());
    }

    @Override public long count(final String key, final Object value) {
        return graph.count(key, value);
    }

    @Override public void remove(final String key, final Object value, final T element) {
        element.removeProperty(key);
    }

    @Override public void put(final String key, final Object value, final T element) {
        element.setProperty(key, value);
    }

    protected void putField(String key, Field field, T element) {
        ((LuceneElement) element).getRaw().add(field);
    }

    @Override public String toString() {
        return StringFactory.indexString(this);
    }

    public Set<String> getAutoIndexKeys() {
        return mapping.keySet();
    }
}
