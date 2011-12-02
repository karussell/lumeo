package com.pannous.tmpo;

import com.tinkerpop.blueprints.pgm.AutomaticIndex;

import com.tinkerpop.blueprints.pgm.Element;
import java.util.HashSet;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class LuceneAutomaticIndex<T extends Element> extends LuceneIndex<T> implements AutomaticIndex<T> {

    Set<String> autoIndexKeys;

    public LuceneAutomaticIndex(final String name, final Class<T> indexClass, final LuceneGraph graph, final Set<String> keys) {
        super(name, indexClass, graph);
        this.autoIndexKeys = new HashSet<String>();
        if(keys != null)
            this.autoIndexKeys.addAll(keys);
    }

    @Override public Type getIndexType() {
        return Type.AUTOMATIC;
    }

    protected void autoUpdate(final String key, final Object newValue, final Object oldValue, final T element) {
        if (null == this.autoIndexKeys || this.autoIndexKeys.contains(key)) {
            if (oldValue != null)
                this.removeBasic(key, oldValue, element);
            this.putBasic(key, newValue, element);
        }
    }

    protected void autoRemove(final String key, final Object oldValue, final T element) {
        if (null == this.autoIndexKeys || this.autoIndexKeys.contains(key)) {
            this.removeBasic(key, oldValue, element);
        }
    }

    @Override public Set<String> getAutoIndexKeys() {
        return this.autoIndexKeys;
    }
}
