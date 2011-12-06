package com.pannous.lumeo;

import com.tinkerpop.blueprints.pgm.AutomaticIndex;
import com.tinkerpop.blueprints.pgm.Edge;
import com.tinkerpop.blueprints.pgm.Vertex;
import com.tinkerpop.blueprints.pgm.impls.StringFactory;
import org.apache.lucene.document.Document;

/**
 * @author Peter Karich, info@jetsli.de
 */
public class LuceneEdge extends LuceneElement implements Edge {

    public LuceneEdge(LuceneGraph graph, Document doc) {
        this(graph, doc, false);
    }

    protected LuceneEdge(final LuceneGraph graph, final Document doc, boolean isNew) {
        super(graph, doc);
        if (isNew) {
            for (final LuceneAutomaticIndex autoIndex : this.g.getAutoIndices(LuceneEdge.class)) {
                autoIndex.autoUpdate(AutomaticIndex.LABEL, this.getLabel(), null, this);
            }
        }
    }

    @Override public String getLabel() {
        return ((Document) this.rawElement).get(RawLucene.EDGE_LABEL);
    }

    @Override public Vertex getOutVertex() {
        return g.getOutVertex(this);
    }

    @Override public Vertex getInVertex() {
        return g.getInVertex(this);
    }

    @Override public boolean equals(final Object object) {
        return object instanceof LuceneEdge && super.equals(object);
    }

    @Override public String toString() {
        return StringFactory.edgeString(this);
    }
}
