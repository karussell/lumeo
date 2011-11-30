package com.pannous.tmpo;

import com.tinkerpop.blueprints.pgm.AutomaticIndex;
import com.tinkerpop.blueprints.pgm.Edge;
import com.tinkerpop.blueprints.pgm.Vertex;
import com.tinkerpop.blueprints.pgm.impls.StringFactory;
import org.apache.lucene.document.Document;

/**
 * @author Peter Karich, info@jetsli.de
 */
public class LuceneEdge extends LuceneElement implements Edge {

    public LuceneEdge(final Document doc, final LuceneGraph graph) {
        this(doc, graph, false);
    }

    protected LuceneEdge(final Document doc, final LuceneGraph graph, boolean isNew) {
        super(graph);
        this.rawElement = doc;
        if (isNew) {
            for (final LuceneAutomaticIndex autoIndex : this.graph.getAutoIndices(LuceneEdge.class)) {
                autoIndex.autoUpdate(AutomaticIndex.LABEL, this.getLabel(), null, this);
            }
        }
    }

    @Override public String getLabel() {
        return ((Document) this.rawElement).get(RawLucene.ID);
    }

    @Override public Vertex getOutVertex() {
        return graph.getOutVertex(this);
    }

    @Override public Vertex getInVertex() {
        return graph.getInVertex(this);
    }

    @Override public boolean equals(final Object object) {
        return object instanceof LuceneEdge && ((LuceneEdge) object).getId().equals(this.getId());
    }

    @Override public String toString() {
        return StringFactory.edgeString(this);
    }

    @Override
    public int hashCode() {
        int hash = 7;
        return hash;
    }
}
