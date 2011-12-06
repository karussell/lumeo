package com.pannous.lumeo;

import com.pannous.lumeo.util.Mapping;
import com.tinkerpop.blueprints.pgm.Edge;
import com.tinkerpop.blueprints.pgm.Element;
import com.tinkerpop.blueprints.pgm.impls.StringFactory;

import java.util.HashSet;
import java.util.Set;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.document.NumericField;

/**
 * @author Peter Karich, info@jetsli.de
 */
public abstract class LuceneElement implements Element {

    protected final LuceneGraph g;
    protected Document rawElement;
    private Mapping m;

    public LuceneElement(LuceneGraph graph, Document doc) {
        if (doc == null)
            throw new NullPointerException("Document must not be null");
        this.rawElement = doc;
        this.g = graph;
        m = g.getMapping(getType());
    }

    @Override public Object getProperty(final String key) {
        return rawElement.get(key);
    }

    @Override public void setProperty(final String key, final Object value) {
        if (key.equals(RawLucene.ID) || key.equals(RawLucene.TYPE)
                || (this instanceof Edge && key.equals(RawLucene.EDGE_LABEL)))
            throw new RuntimeException(key + StringFactory.PROPERTY_EXCEPTION_MESSAGE);

        try {
            Object oldValue = this.getProperty(key);            
            for (LuceneAutomaticIndex autoIndex : this.g.getAutoIndices(this.getClass())) {
                autoIndex.autoUpdate(key, value, oldValue, this);
            }
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    @Override public Object removeProperty(final String key) {
        try {
            String oldValue = rawElement.get(key);
            rawElement.removeField(key);
            if (oldValue != null)
                for (LuceneAutomaticIndex autoIndex : this.g.getAutoIndices(this.getClass())) {
                    autoIndex.autoRemove(key, oldValue, this);
                }

            return oldValue;
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    @Override public Set<String> getPropertyKeys() {
        final Set<String> keys = new HashSet<String>();
        for (final Fieldable key : this.rawElement.getFields()) {
            keys.add(key.name());
        }
        return keys;
    }

    @Override public int hashCode() {
        return this.getId().hashCode();
    }

    public Document getRaw() {
        return this.rawElement;
    }

    @Override public Object getId() {
        return ((NumericField) rawElement.getFieldable(RawLucene.ID)).getNumericValue().longValue();
    }

    public String getType() {
        String t = rawElement.get(RawLucene.TYPE);
        if (t == null)
            throw new NullPointerException("No type available for " + getId());
        return t;
    }

    @Override public boolean equals(final Object object) {
        return (null != object) && (this.getClass().equals(object.getClass()) && this.getId().equals(((Element) object).getId()));
    }

    Analyzer getAnalyzer(String field) {
        return m.getAnalyzer(field);
    }
}
