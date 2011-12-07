package com.pannous.lumeo;

import com.pannous.lumeo.util.Mapping;
import com.pannous.lumeo.util.Mapping.Type;
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
    private Map<Class, LuceneAutomaticIndex<? extends Element>> indices = new ConcurrentHashMap<Class, LuceneAutomaticIndex<? extends Element>>();

    public LuceneGraph() {
        this(new RawLucene(new RAMDirectory()).init());
    }

    public LuceneGraph(String path) {
        this(new RawLucene(path).init());
    }

    public LuceneGraph(RawLucene rl) {
        rawLucene = rl;
    }

    @Override public <T extends Element> Index<T> createManualIndex(final String indexName, final Class<T> indexClass) {
        logger.warn("use automatic indices of manual indices");
        return createAutomaticIndex(indexName, indexClass, null);
    }

    @Override public synchronized <T extends Element> AutomaticIndex<T> createAutomaticIndex(final String indexName,
            final Class<T> indexClass, Set<String> keys) {
        LuceneAutomaticIndex index = indices.get(indexClass);
        if (index != null)
            throw new UnsupportedOperationException("index for " + indexClass + " already exists");
        if (keys == null)
            throw new UnsupportedOperationException("you need to specify key which should get indexed for " + indexClass);

        Mapping m = getMapping(indexClass.getSimpleName());
        for (String k : keys) {
            // DEFAULT type for all keys is normal string!
            Mapping.Type type = Mapping.Type.STRING;
            int pos = k.indexOf(",");
            if (pos >= 0) {
                type = Mapping.Type.valueOf(k.substring(pos + 1));
                k = k.substring(0, pos);
            }

            Type old = m.putField(k, type);
            if (old != null)
                throw new UnsupportedOperationException("Property was defined multiple times! new:" + k + " old:" + old);
        }

        index = new LuceneAutomaticIndex<T>(this, indexClass, m);
        index.setIndexName(indexName);
        indices.put(indexClass, index);
        if (Vertex.class.isAssignableFrom(indexClass))
            indices.put(LuceneVertex.class, index);
        else if (Edge.class.isAssignableFrom(indexClass))
            indices.put(LuceneEdge.class, index);
        return index;
    }

    @Override public <T extends Element> Index<T> getIndex(final String indexName, final Class<T> indexClass) {
        Index i = indices.get(indexClass);
        if (i == null)
            throw new UnsupportedOperationException("index not found " + indexName + " ," + indexClass);

        // there is only one index per class at the moment
        if (!indexName.equals(i.getIndexName()))
            throw new UnsupportedOperationException("index with name " + indexName + " not found");

        return (Index<T>) i;
    }

    @Override public void dropIndex(final String indexName) {
        Iterator<LuceneAutomaticIndex<?>> iter = indices.values().iterator();
        while (iter.hasNext()) {
            if (iter.next().getIndexName().equals(indexName)) {
                iter.remove();
                break;
            }
        }
    }

    @Override public Iterable<Index<? extends Element>> getIndices() {
        return (Iterable) indices.values();
    }

    @Override public Vertex addVertex(Object userIdObj) {
        try {
            String userId;
            long id = -1;
            if (userIdObj == null) {
                id = atomicCounter.incrementAndGet();
                // use here NumericUtils.longToPrefixCoded() ?
                userId = Long.toString(id);
            } else {
                userId = userIdObj.toString();
                if (rawLucene.existsUserId(userId))
                    throw new RuntimeException("Vertex with user id already exists:" + userId);
            }

            Document doc = rawLucene.findByUserId(userId.toString());
            if (doc == null) {
                if (id < 0)
                    id = atomicCounter.incrementAndGet();

                doc = rawLucene.createDocument(userId, id, Vertex.class);
                rawLucene.put(userId, id, doc);
            }

            return new LuceneVertex(this, doc);
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
        return new EdgeFilterSequence(this);
    }

    @Override public Edge addEdge(final Object userIdObj, final Vertex outVertex, final Vertex inVertex, final String label) {
        try {
            String userId;
            long id = -1;
            if (userIdObj == null) {
                id = atomicCounter.incrementAndGet();
                // use here NumericUtils.longToPrefixCoded() ?
                userId = Long.toString(id);
            } else {
                userId = userIdObj.toString();
                if (rawLucene.existsUserId(userId))
                    throw new RuntimeException("Edge with user id already exists:" + userId);
            }

            Document edgeDoc = rawLucene.findByUserId(userId.toString());
            if (edgeDoc == null) {
                if (id < 0)
                    id = atomicCounter.incrementAndGet();

                edgeDoc = rawLucene.createDocument(userId, id, Edge.class);
                edgeDoc.add(getMapping(Edge.class.getSimpleName()).newStringField(RawLucene.EDGE_LABEL, label));
            }

            rawLucene.initRelation(edgeDoc, ((LuceneElement) outVertex).getRaw(), ((LuceneElement) inVertex).getRaw());
            rawLucene.put(userId, id, edgeDoc);
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
        LuceneAutomaticIndex<T> tmp = (LuceneAutomaticIndex<T>) indices.get(cl);
        if (tmp == null)
            return Collections.EMPTY_LIST;
        return Collections.<LuceneAutomaticIndex<T>>singletonList(tmp);
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
        throw new UnsupportedOperationException("Not supported yet. (why is this method necessary for transaction support at all??) "
                + "Recreate the graph or use an in-memory instance.");
    }

    @Override public void startTransaction() {
    }

    @Override public void stopTransaction(final Conclusion conclusion) {
        // TODO flush here ?
    }

    public RawLucene getRaw() {
        return rawLucene;
    }

    @Override public String toString() {
        return rawLucene.toString();
    }

    long count(String fieldName, Object value) {
        return rawLucene.count(fieldName, value);
    }

    void refresh() {
        rawLucene.refresh();
    }

    Mapping getMapping(String type) {
        return rawLucene.getMapping(type);
    }
}