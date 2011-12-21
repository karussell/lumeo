package de.jetsli.lumeo;

import de.jetsli.lumeo.util.Mapping;
import com.tinkerpop.blueprints.pgm.AutomaticIndex;

import com.tinkerpop.blueprints.pgm.CloseableSequence;
import com.tinkerpop.blueprints.pgm.Element;
import java.util.Set;

/**
 * @author Peter Karich, info@jetsli.de
 */
public class LuceneAutomaticIndex<T extends Element> extends LuceneIndex<T> implements AutomaticIndex<T> {

    private final Mapping m;

    public LuceneAutomaticIndex(final LuceneGraph graph, final Class<T> indexClass, Mapping m) {
        super(graph, indexClass);
        if (m == null)
            throw new NullPointerException("mapping cannot be null");

        this.m = m;
    }

    @Override public Type getIndexType() {
        return Type.AUTOMATIC;
    }

    boolean handle(String key) {
        return m.exists(key);
    }

    protected void autoUpdate(final String key, final Object newValue, final Object oldValue, final T element) {
        // accept all keys to store them
        if (oldValue != null)
            removeField(key, element);

        putField(m.createField(key, newValue), element);
        g.getRaw().fastPut((Long) element.getId(), ((LuceneElement) element).getRaw());
    }

    protected void autoRemove(final String key, final Object oldValue, final T element) {
        // accept all keys to remove stored
        removeField(key, element);
        g.getRaw().fastPut((Long) element.getId(), ((LuceneElement) element).getRaw());
    }

    @Override public CloseableSequence<T> get(final String key, final Object value) {
        if (handle(key))
            return super.get(key, value);
        else
            throw new UnsupportedOperationException("key not indexed " + key);
    }

    @Override public void put(String key, Object value, T element) {
        if (handle(key))
            super.put(key, value, element);
        else
            throw new UnsupportedOperationException("key not indexed " + key);
    }

    @Override public long count(String key, Object value) {
        if (handle(key))
            return super.count(key, value);
        else
            throw new UnsupportedOperationException("key not indexed " + key);
    }

    @Override public Set<String> getAutoIndexKeys() {
        if (m == null)
            return null;
        return m.getIndexedFields();
    }
}
