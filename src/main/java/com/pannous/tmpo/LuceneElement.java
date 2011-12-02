package com.pannous.tmpo;

import com.tinkerpop.blueprints.pgm.Edge;
import com.tinkerpop.blueprints.pgm.Element;
import com.tinkerpop.blueprints.pgm.impls.StringFactory;

import java.util.HashSet;
import java.util.Set;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.document.NumericField;

/**
 * @author Peter Karich, info@jetsli.de
 */
public abstract class LuceneElement implements Element {

    protected final LuceneGraph graph;
    protected Document rawElement;

    public LuceneElement(LuceneGraph graph, Document doc) {
        if (doc == null)
            throw new NullPointerException("Document must not be null");
        this.rawElement = doc;
        this.graph = graph;
    }

    @Override public Object getProperty(final String key) {
        return ((NumericField) this.rawElement.getFieldable(key)).getNumericValue().longValue();
    }

    @Override public void setProperty(final String key, final Object value) {
        if (key.equals(StringFactory.ID) || (key.equals(StringFactory.LABEL) && this instanceof Edge))
            throw new RuntimeException(key + StringFactory.PROPERTY_EXCEPTION_MESSAGE);

        try {
            Object oldValue = this.getProperty(key);
            for (LuceneAutomaticIndex autoIndex : this.graph.getAutoIndices(this.getClass())) {
                autoIndex.autoUpdate(key, value, oldValue, this);
            }

            this.rawElement.add(new Field(key, value.toString(), Field.Store.NO, Field.Index.NOT_ANALYZED_NO_NORMS));
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    @Override public Object removeProperty(final String key) {
        throw new UnsupportedOperationException();
//        try {            
//            Object oldValue = this.rawElement.removeProperty(key);
//            if (null != oldValue) {
//                for (LuceneAutomaticIndex autoIndex : this.graph.getAutoIndices(this.getClass())) {
//                    autoIndex.autoRemove(key, oldValue, this);
//                }
//            }
//
//            return oldValue;
//        } catch (RuntimeException e) {
//            throw e;
//        } catch (Exception e) {
//            throw new RuntimeException(e.getMessage(), e);
//        }
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
        return getProperty(RawLucene.ID);
    }

    @Override public boolean equals(final Object object) {
        return (null != object) && (this.getClass().equals(object.getClass()) && this.getId().equals(((Element) object).getId()));
    }
}
