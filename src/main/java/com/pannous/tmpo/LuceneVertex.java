package com.pannous.tmpo;

import com.tinkerpop.blueprints.pgm.Edge;
import com.tinkerpop.blueprints.pgm.Vertex;
import com.tinkerpop.blueprints.pgm.impls.StringFactory;

import org.apache.lucene.document.Document;

/**
 * @author Peter Karich, info@jetsli.de
 */
public class LuceneVertex extends LuceneElement implements Vertex {

    public LuceneVertex(Document doc, final LuceneGraph graph) {
        super(graph);
        this.rawElement = doc;
    }

    @Override
    public Iterable<Edge> getInEdges(final String... labels) {
        return graph.getEdges(this.rawElement, RawLucene.EDGE_IN);
    }

    @Override
    public Iterable<Edge> getOutEdges(final String... labels) {
        return graph.getEdges(this.rawElement, RawLucene.EDGE_OUT);
//        if (labels.length == 0)
//            return new LuceneEdgeSequence(((Document) this.rawElement).getRelationships(Direction.OUTGOING), this.graph);
//        else if (labels.length == 1) {
//            return new LuceneEdgeSequence(((Document) this.rawElement).getRelationships(DynamicRelationshipType.withName(labels[0]), Direction.OUTGOING), this.graph);
//        } else {
//            final List<Iterable<Edge>> edges = new ArrayList<Iterable<Edge>>();
//            for (final String label : labels) {
//                edges.add(new LuceneEdgeSequence(((Document) this.rawElement).getRelationships(DynamicRelationshipType.withName(label), Direction.OUTGOING), this.graph));
//            }
//            return new MultiIterable<Edge>(edges);
//        }
    }

    @Override
    public boolean equals(final Object object) {
        return object instanceof LuceneVertex && ((LuceneVertex) object).getId().equals(this.getId());
    }

    @Override
    public String toString() {
        return StringFactory.vertexString(this);
    }

    public Document getRawVertex() {
        return (Document) this.rawElement;
    }

//    TODO
//    @Override
//    public int hashCode() {
//        return ;
//    }
}
