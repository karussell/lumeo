package com.pannous.tmpo;

import com.tinkerpop.blueprints.pgm.AutomaticIndex;
import com.tinkerpop.blueprints.pgm.Edge;
import com.tinkerpop.blueprints.pgm.Element;
import com.tinkerpop.blueprints.pgm.Index;
import com.tinkerpop.blueprints.pgm.IndexableGraph;
import com.tinkerpop.blueprints.pgm.TransactionalGraph;
import com.tinkerpop.blueprints.pgm.Vertex;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericField;
import org.apache.lucene.store.RAMDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Blueprints implementation of the Search Engine Apache Lucene (http://lucene.apache.org)
 * 
 * Why is this a nice idea?
 * - Now in 3.5 there is a searchAfter method making deeply searches (at least a bit) better.
 * - Terms are mapped to documents like edges in one node to other nodes (or edges). 
 *   E.g. OrientGraphDB is baked by a document storage
 * - We can make it distributed later on with the help of ElasticSearch. Then even enhance 
 *   searching via stats, facets, putting different indices into different lucene indices etc
 * - We can traverse the graph without the mismatch (two storages) currently seen 
 *   e.g. in Neo4j + Lucene or infinity graph.
 *   Also, in Neo4j it is easy to get nodes from a query but getting relationships is not easy
 *   http://groups.google.com/group/neo4jrb/browse_thread/thread/8f739197886ecec7
 * 
 * Why is this a bad idea?
 * - heavy alpha software
 * - deletes and realtime results are not easy for search engines
 * - no transaction support
 * 
 * TODO
 * - caching is bad at the moment. also test advanced caching strategies (e.g. measure degree centrality, cluster   )
 *   especially use FieldCache for user and long IDs
 * - Use several tuning possibilities in Lucene
 * - Reimplement a subset of the lucene functionality by using search trees in the graph?
 * 
 * @author Peter Karich, info@jetsli.de
 */
public class LuceneGraph implements TransactionalGraph, IndexableGraph {

    private Logger logger = LoggerFactory.getLogger(getClass());
    private AtomicLong atomicCounter = new AtomicLong(1);
    private RawLucene rawLucene;
    private Map<Class, Index<? extends Element>> indices = new ConcurrentHashMap<Class, Index<? extends Element>>();

    public LuceneGraph() {
        this(new RawLucene(new RAMDirectory()).init());
    }

    public LuceneGraph(String path) {
        this(new RawLucene(path).init());
    }

    public LuceneGraph(RawLucene rl) {
        rawLucene = rl;
    }

    @Override public <T extends Element> Index<T> 
    createManualIndex(final String indexName, final Class<T> indexClass) {
        logger.warn("use automatic indices of manual indices");
        return createAutomaticIndex(indexName, indexClass, null);
    }

    @Override public synchronized <T extends Element> AutomaticIndex<T> 
    createAutomaticIndex(final String indexName, final Class<T> indexClass, Set<String> keys) {
        if (indices.containsKey(indexClass))
            throw new UnsupportedOperationException("index already exists:" + indexName);

        LuceneAutomaticIndex index = new LuceneAutomaticIndex<T>(indexName, indexClass, this, keys);
        indices.put(indexClass, index);
        return index;
    }

    @Override public <T extends Element> Index<T> 
    getIndex(final String indexName, final Class<T> indexClass) {
        Index i = indices.get(indexClass);
        if (i == null)
            throw new UnsupportedOperationException("index not found " + indexName + " " + indexClass);

        return (Index<T>) i;
    }

    @Override public void dropIndex(final String indexName) {
        Iterator<Index<?>> iter = indices.values().iterator();
        while (iter.hasNext()) {
            if (iter.next().getIndexName().equals(indexName)) {
                iter.remove();
                break;
            }
        }
    }

    @Override public Iterable<Index<? extends Element>> getIndices() {
        return indices.values();
    }

    @Override public Vertex addVertex(Object userIdObj) {
        try {
            String userId;
            long id = -1;
            if (userIdObj == null) {
                id = atomicCounter.incrementAndGet();
                userId = Long.toString(id);
            } else {
                userId = userIdObj.toString();
                if (rawLucene.exists(userId))
                    throw new RuntimeException("Vertex with user id already exists:" + userId);
            }

            Document doc = rawLucene.findByUserId(userId.toString());
            if (doc == null) {
                if (id < 0)
                    id = atomicCounter.incrementAndGet();

                doc = rawLucene.createDocument(userId, id);
                rawLucene.put(userId, id, doc, false);
            }

            final Vertex vertex = new LuceneVertex(this, doc);
            return vertex;
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    @Override public Vertex getVertex(final Object id) {
        Document doc = rawLucene.findByUserId(id.toString());
        if (doc == null)
            return null;

        return new LuceneVertex(this, doc);
    }

    @Override public Iterable<Vertex> getVertices() {
        return new VertexFilterSequence(this);
    }

    @Override public void removeVertex(final Vertex vertex) {
        rawLucene.removeById((Long) vertex.getId());
    }

    @Override public Iterable<Edge> getEdges() {
        return new EdgeVertexTraversalSequence(this);
    }

    @Override public Edge addEdge(final Object userIdObj, final Vertex outVertex, final Vertex inVertex, final String label) {
        try {
            String userId;
            long id = -1;
            if (userIdObj == null) {
                id = atomicCounter.incrementAndGet();
                userId = Long.toString(id);
            } else {
                userId = userIdObj.toString();
                if (rawLucene.exists(userId))
                    throw new RuntimeException("Edge with user id already exists:" + userId);
            }

            Document edgeDoc = rawLucene.findByUserId(userId.toString());
            if (edgeDoc == null) {
                if (id < 0)
                    id = atomicCounter.incrementAndGet();

                edgeDoc = rawLucene.createDocument(userId, id);
                edgeDoc.add(new Field(RawLucene.EDGE_LABEL, label, Field.Store.YES, Field.Index.NOT_ANALYZED_NO_NORMS));                
            }                        
            
            rawLucene.initRelation(edgeDoc, ((LuceneElement) outVertex).getRaw(), ((LuceneElement) inVertex).getRaw()); 
            rawLucene.put(userId, id, edgeDoc, false);
            return new LuceneEdge(this, edgeDoc);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public void flush() {
        try {
            rawLucene.flush();
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override public Edge getEdge(final Object id) {
        Document doc = rawLucene.findByUserId(id.toString());
        if (doc == null)
            return null;

        return new LuceneEdge(this, doc);
    }

    @Override public void removeEdge(final Edge edge) {
        throw new UnsupportedOperationException("not yet supported");
    }

     <T extends Element> Collection<LuceneAutomaticIndex<T>> getAutoIndices(Class<T> cl) {
        Collection<LuceneAutomaticIndex<T>> tmp = (Collection<LuceneAutomaticIndex<T>>) indices.get(cl);
        if (tmp == null)
            return Collections.EMPTY_LIST;
        return tmp;
    }

     @Override public int getMaxBufferSize() {
        // TODO not really the correct values ...
        return rawLucene.getMaxMergeMB() * 1024 * 1024;
    }

    @Override public int getCurrentBufferSize() {
        // TODO not really the correct values ...
        return rawLucene.getMaxNumRecordsBeforeCommit() * 5 * 1024;
    }

    @Override public void setMaxBufferSize(final int size) {
        rawLucene.setMaxMergeMB(size / 1024 / 1024);
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

    @Override public void startTransaction() {
    }

    @Override public void stopTransaction(final Conclusion conclusion) {
    }

    public RawLucene getRaw() {
        return rawLucene;
    }

    @Override public String toString() {
        return rawLucene.toString();
    }

    long count(String fieldName, String value) {
        return rawLucene.count(fieldName, value);
    }

    public Vertex getOutVertex(LuceneEdge e) {
        long id = ((NumericField) e.getRaw().getFieldable(RawLucene.VERTEX_OUT)).getNumericValue().longValue();
        Document doc = rawLucene.findById(id);
        if (doc == null)
            throw new NullPointerException("Didn't found out vertex of edge with id " + id);
        return new LuceneVertex(this, doc);
    }

    public Vertex getInVertex(LuceneEdge e) {
        long id = ((NumericField) e.getRaw().getFieldable(RawLucene.VERTEX_IN)).getNumericValue().longValue();
        Document doc = rawLucene.findById(id);
        if (doc == null)
            throw new NullPointerException("Didn't found in vertex of edge with id " + id);
        return new LuceneVertex(this, doc);
    }

    void refresh() {
        rawLucene.refresh();
    }
}