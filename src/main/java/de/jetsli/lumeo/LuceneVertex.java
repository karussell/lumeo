package de.jetsli.lumeo;

import com.tinkerpop.blueprints.pgm.Edge;
import com.tinkerpop.blueprints.pgm.Vertex;
import com.tinkerpop.blueprints.pgm.impls.StringFactory;

import org.apache.lucene.document.Document;

/**
 * @author Peter Karich, info@jetsli.de
 */
public class LuceneVertex extends LuceneElement implements Vertex {

    public LuceneVertex(LuceneGraph graph, Document doc) {
        super(graph, doc);
    }

    @Override public Iterable<Edge> getInEdges(final String... labels) {
        return new EdgeVertexBoundSequence(g, this, RawLucene.EDGE_IN).setLabels(labels);
    }

    @Override public Iterable<Edge> getOutEdges(final String... labels) {
        return new EdgeVertexBoundSequence(g, this, RawLucene.EDGE_OUT).setLabels(labels);        
    }

    @Override public boolean equals(final Object object) {
        return object instanceof LuceneVertex && super.equals(object);
    }

    @Override public String toString() {
        return StringFactory.vertexString(this);
    }
}
