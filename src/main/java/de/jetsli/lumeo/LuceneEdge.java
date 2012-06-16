package de.jetsli.lumeo;

import com.tinkerpop.blueprints.pgm.AutomaticIndex;
import com.tinkerpop.blueprints.pgm.Edge;
import com.tinkerpop.blueprints.pgm.Vertex;
import com.tinkerpop.blueprints.pgm.impls.StringFactory;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.LongField;
//import org.apache.lucene.document.NumericField;

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
        long id = ((LongField) getRaw().getField(RawLucene.VERTEX_OUT)).numericValue().longValue();
        Document doc = g.getRaw().findById(id);
        if (doc == null)
            throw new NullPointerException("Didn't found out vertex of edge with id " + id);
        return new LuceneVertex(g, doc);
    }

    @Override public Vertex getInVertex() {
        long id = ((LongField) getRaw().getField(RawLucene.VERTEX_IN)).numericValue().longValue();
        Document doc = g.getRaw().findById(id);
        if (doc == null)
            throw new NullPointerException("Didn't found in vertex of edge with id " + id);
        return new LuceneVertex(g, doc);
    }

    @Override public boolean equals(final Object object) {
        return object instanceof LuceneEdge && super.equals(object);
    }

    @Override public String toString() {
        return StringFactory.edgeString(this);
    }
}
