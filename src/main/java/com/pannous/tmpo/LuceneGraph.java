package com.pannous.tmpo;

import com.sun.org.apache.bcel.internal.generic.LoadClass;
import com.tinkerpop.blueprints.pgm.AutomaticIndex;
import com.tinkerpop.blueprints.pgm.Edge;
import com.tinkerpop.blueprints.pgm.Element;
import com.tinkerpop.blueprints.pgm.Index;
import com.tinkerpop.blueprints.pgm.IndexableGraph;
import com.tinkerpop.blueprints.pgm.TransactionalGraph;
import com.tinkerpop.blueprints.pgm.Vertex;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.document.NumericField;
import org.apache.lucene.store.RAMDirectory;

/**
 * A Blueprints implementation of the Search Engine Apache Lucene (http://lucene.apache.org)
 *
 * @author Peter Karich, info@jetsli.de
 */
public class LuceneGraph implements TransactionalGraph, IndexableGraph {

    private AtomicLong atomicCounter = new AtomicLong(1);
    private Set<String> indexableFields = new LinkedHashSet<String>();
    private RawLucene rawLucene;

    public LuceneGraph() {
        this(new RawLucene(new RAMDirectory()).init());
    }

    public LuceneGraph(String path) {
        this(new RawLucene(path).init());
    }

    public LuceneGraph(RawLucene rl) {
        rawLucene = rl;
    }

    @Override
    public synchronized <T extends Element> Index<T> createManualIndex(final String indexName, final Class<T> indexClass) {
        throw new UnsupportedOperationException();
//        if (this.indices.containsKey(indexName))
//            throw new RuntimeException("Index already exists: " + indexName);
//
//        LuceneIndex index = new LuceneIndex(indexName, indexClass, this);
//        this.indices.put(index.getIndexName(), index);
//        return index;
    }

    @Override
    public synchronized <T extends Element> AutomaticIndex<T> createAutomaticIndex(final String indexName, final Class<T> indexClass, Set<String> keys) {
        return (AutomaticIndex<T>) new LuceneAutomaticIndex<T>(indexName, indexClass, this, new HashSet<String>());
    }

    @Override
    public <T extends Element> Index<T> getIndex(final String indexName, final Class<T> indexClass) {
        return (Index<T>) new LuceneAutomaticIndex<T>(indexName, indexClass, this, new HashSet<String>());
    }

    @Override
    public synchronized void dropIndex(final String indexName) {
        Iterator<String> iter = indexableFields.iterator();
        String prefix = indexName + "_";
        while (iter.hasNext()) {
            if (iter.next().startsWith(prefix))
                iter.remove();
        }
    }

    @Override
    public Iterable<Index<? extends Element>> getIndices() {
        throw new UnsupportedOperationException();
//        List<Index<? extends Element>> list = new ArrayList<Index<? extends Element>>();
//        for (final Index index : this.indices.values()) {
//            list.add(index);
//        }
//        return list;
    }

    @Override
    public Vertex addVertex(Object userIdObj) {
        try {
            String userId;
            long id = -1;
            String idStr = null;
            if (userIdObj == null) {
                id = atomicCounter.incrementAndGet();
                userId = Long.toString(id);
                idStr = userId;
            } else {
                userId = userIdObj.toString();
                if (rawLucene.exists(userId))
                    throw new RuntimeException("Vertex with user id already exists:" + userId);
            }

            Document doc = rawLucene.findByUserId(userId.toString());
            if (doc == null) {
                if (idStr == null) {
                    id = atomicCounter.incrementAndGet();
                    idStr = Long.toString(id);
                }

                doc = new Document();
                doc.add(new Field(RawLucene.ID, idStr, Field.Store.YES, Field.Index.NOT_ANALYZED_NO_NORMS));
                doc.add(new Field(RawLucene.UID, userId.toString(), Field.Store.YES, Field.Index.NOT_ANALYZED_NO_NORMS));
            }

            final Vertex vertex = new LuceneVertex(doc, this);
            return vertex;
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    @Override
    public Vertex getVertex(final Object id) {
        Document doc = rawLucene.findByUserId(id.toString());
        if(doc == null)
            return null;
        
        return new LuceneVertex(doc, this);
    }

    @Override public Iterable<Vertex> getVertices() {
        throw new UnsupportedOperationException();
//        return new LuceneVertexSequence(this.rawGraph.getAllNodes(), this);
    }

    @Override public Iterable<Edge> getEdges() {
        throw new UnsupportedOperationException();
//        return new LuceneGraphEdgeSequence(this.rawGraph.getAllNodes(), this);
    }

    @Override public void removeVertex(final Vertex vertex) {
        throw new UnsupportedOperationException();
//        final Long id = (Long) vertex.getId();
//        final Node node = this.rawGraph.getNodeById(id);
//        if (null != node) {
//            try {
//                AutomaticIndexHelper.removeElement(this, vertex);
//                this.autoStartTransaction();
//                for (final Edge edge : vertex.getInEdges()) {
//                    ((Relationship) ((LuceneEdge) edge).getRawElement()).delete();
//                }
//                for (final Edge edge : vertex.getOutEdges()) {
//                    ((Relationship) ((LuceneEdge) edge).getRawElement()).delete();
//                }
//                node.delete();
//                this.autoStopTransaction(Conclusion.SUCCESS);
//            } catch (RuntimeException e) {
//                this.autoStopTransaction(TransactionalGraph.Conclusion.FAILURE);
//                throw e;
//            } catch (Exception e) {
//                this.autoStopTransaction(TransactionalGraph.Conclusion.FAILURE);
//                throw new RuntimeException(e.getMessage(), e);
//            }
//        }
    }

    @Override public Edge addEdge(final Object id, final Vertex outVertex, final Vertex inVertex, final String label) {
        throw new UnsupportedOperationException();
//        try {
//            this.autoStartTransaction();
//            final Node outNode = ((LuceneVertex) outVertex).getRawVertex();
//            final Node inNode = ((LuceneVertex) inVertex).getRawVertex();
//            final Relationship relationship = outNode.createRelationshipTo(inNode, DynamicRelationshipType.withName(label));
//            final Edge edge = new LuceneEdge(relationship, this, true);
//            this.autoStopTransaction(Conclusion.SUCCESS);
//            return edge;
//        } catch (RuntimeException e) {
//            this.autoStopTransaction(TransactionalGraph.Conclusion.FAILURE);
//            throw e;
//        } catch (Exception e) {
//            this.autoStopTransaction(TransactionalGraph.Conclusion.FAILURE);
//            throw new RuntimeException(e.getMessage(), e);
//        }
    }

    @Override public Edge getEdge(final Object id) {
        throw new UnsupportedOperationException();
//        if (null == id)
//            throw new IllegalArgumentException("Element identifier cannot be null");
//
//        try {
//            final Long longId;
//            if (id instanceof Long)
//                longId = (Long) id;
//            else
//                longId = Double.valueOf(id.toString()).longValue();
//            return new LuceneEdge(this.rawGraph.getRelationshipById(longId), this);
//        } catch (NotFoundException e) {
//            return null;
//        } catch (NumberFormatException e) {
//            return null;
//        }
    }

    @Override public void removeEdge(final Edge edge) {
        throw new UnsupportedOperationException();
//        try {
//            AutomaticIndexHelper.removeElement(this, edge);
//            this.autoStartTransaction();
//            ((Relationship) ((LuceneEdge) edge).getRawElement()).delete();
//            this.autoStopTransaction(Conclusion.SUCCESS);
//        } catch (RuntimeException e) {
//            this.autoStopTransaction(TransactionalGraph.Conclusion.FAILURE);
//            throw e;
//        } catch (Exception e) {
//            this.autoStopTransaction(TransactionalGraph.Conclusion.FAILURE);
//            throw new RuntimeException(e.getMessage(), e);
//        }
    }

    Collection<LuceneAutomaticIndex> getAutoIndices(Class cl) {
        throw new UnsupportedOperationException();
    }

    @Override public void startTransaction() {
    }

    @Override public void stopTransaction(final Conclusion conclusion) {
    }

    @Override public int getMaxBufferSize() {
        // TODO not really the correct values ...
        return rawLucene.getMaxMergeMB();
    }

    @Override public int getCurrentBufferSize() {
        // TODO not really the correct values ...
        return rawLucene.getMaxNumRecordsBeforeCommit();
    }

    @Override public void setMaxBufferSize(final int size) {
        rawLucene.setMaxMergeMB(size);
    }

    @Override public void shutdown() {
        rawLucene.close();
    }

    @Override public void clear() {
        try {
            rawLucene.clear();
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    protected void autoStartTransaction() {
    }

    protected void autoStopTransaction(final Conclusion conclusion) {
    }

    public RawLucene getRawLucene() {
        return rawLucene;
    }

    @Override public String toString() {
        throw new UnsupportedOperationException();
//        return StringFactory.graphString(this, this.rawGraph.toString());
    }

    long count(String fieldName, String value) {
        return rawLucene.count(fieldName, value);
    }

    public Vertex getOutVertex(LuceneEdge e) {
        long id = ((NumericField) e.getRawElement().getFieldable(RawLucene.VERTEX_OUT)).getNumericValue().longValue();
        return new LuceneVertex(rawLucene.findById(id), this);
    }

    public Vertex getInVertex(LuceneEdge e) {
        long id = ((NumericField) e.getRawElement().getFieldable(RawLucene.VERTEX_IN)).getNumericValue().longValue();
        return new LuceneVertex(rawLucene.findById(id), this);
    }

    Iterable<Edge> getEdges(Document rawElement, String edgeType) {
        return new EdgeIterable(rawElement, edgeType, this);
    }

    private static class EdgeIterable implements Iterable<Edge> {

        private Document doc;
        private String fieldName;
        private LuceneGraph rl;

        public EdgeIterable(Document rawElement, String edgeType, LuceneGraph rl) {
            doc = rawElement;
            fieldName = edgeType;
            this.rl = rl;
        }

        @Override
        public Iterator<Edge> iterator() {
            return new Iterator<Edge>() {

                Fieldable[] fieldables = doc.getFieldables(fieldName);
                int index = 0;

                @Override
                public boolean hasNext() {
                    return index < fieldables.length;
                }

                @Override
                public Edge next() {
                    if (!hasNext())
                        throw new UnsupportedOperationException("no further element");

                    long id = ((NumericField) fieldables[index++]).getNumericValue().longValue();
                    return new LuceneEdge(rl.getRawLucene().findById(id), rl);
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException("Not supported yet.");
                }
            };
        }
    }
}