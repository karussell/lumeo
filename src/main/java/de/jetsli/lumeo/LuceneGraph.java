package de.jetsli.lumeo;

import de.jetsli.lumeo.util.Mapping;
import de.jetsli.lumeo.util.Mapping.Type;
import com.tinkerpop.blueprints.pgm.AutomaticIndex;
import com.tinkerpop.blueprints.pgm.CloseableSequence;
import com.tinkerpop.blueprints.pgm.Edge;
import com.tinkerpop.blueprints.pgm.Element;
import com.tinkerpop.blueprints.pgm.Index;
import com.tinkerpop.blueprints.pgm.IndexableGraph;
import com.tinkerpop.blueprints.pgm.TransactionalGraph;
import com.tinkerpop.blueprints.pgm.Vertex;
import com.tinkerpop.blueprints.pgm.impls.Parameter;

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
 * Why is this a nice idea? - Now in 3.5 there is a searchAfter method making deeply searches (at
 * least a bit) better. - Terms are mapped to documents like edges in one node to other nodes (or
 * edges). E.g. OrientGraphDB is baked by a document storage - We can make it distributed later on
 * with the help of ElasticSearch. Then even enhance searching via stats, facets, putting different
 * indices into different lucene indices etc - We can traverse the graph without the mismatch (two
 * storages) currently seen e.g. in Neo4j + Lucene or infinity graph. Also, in Neo4j it is easy to
 * get nodes from a query but getting relationships is not easy
 * http://groups.google.com/group/neo4jrb/browse_thread/thread/8f739197886ecec7
 *
 * Why is this a bad idea? - heavy alpha software - deletes and realtime results are not easy for
 * search engines - no transaction support
 *
 * TODO - caching is bad at the moment. - no advanced caching strategies (e.g. measure degree
 * centrality, cluster) - use FieldCache or DocValues for user and long IDs and similar - use
 * several tuning possibilities in Lucene - Reimplement a subset of the lucene functionality by
 * using search trees in the graph?
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

    @Override
    public <T extends Element> Index<T> createManualIndex(String indexName,
            Class<T> indexClass, Parameter... params) {
        logger.warn("use automatic indices of manual indices");
        return createAutomaticIndex(indexName, indexClass, null);
    }

    public synchronized <T extends Element> AutomaticIndex<T> createAutomaticIndex(
            String indexName, Class<T> indexClass, Set<String> keys, Parameter... arg3) {

        LuceneAutomaticIndex index = indices.get(indexClass);
        if (index != null)
            throw new UnsupportedOperationException("index for " + indexClass + " already exists");
        if (keys == null)
            throw new UnsupportedOperationException("you need to specify key which should get indexed for " + indexClass);

        Mapping m = getMapping(indexClass);
        for (String k : keys) {
            // DEFAULT type for all keys is normal string and to lower case!
            Mapping.Type type = m.getDefaultType();
            int pos = k.indexOf(",");
            if (pos >= 0) {
                type = Mapping.Type.valueOf(k.substring(pos + 1));
                k = k.substring(0, pos);
            }

            Type old = m.putField(k, type);
            if (old != null)
                throw new UnsupportedOperationException("Property was already defined! new:" + k + " old:" + old);
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
                userId = Long.toString(id);
            } else {
                userId = userIdObj.toString();
                if (rawLucene.existsUserId(userId))
                    throw new RuntimeException("Vertex with user id already exists:" + userId);
            }

            //TODO -MH  haven't we just done this read above half the time?
            Document doc = rawLucene.findByUserId(userId.toString());
            if (doc == null) {
                if (id < 0)
                    id = atomicCounter.incrementAndGet();

                doc = rawLucene.createDocument(userId, id, Vertex.class);
                rawLucene.fastPut(id, doc);
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

    @Override public CloseableSequence<Vertex> getVertices() {
        return new VertexFilterSequence(this);
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
                userId = Long.toString(id);
            } else {
                userId = userIdObj.toString();
                if (rawLucene.existsUserId(userId))
                    throw new RuntimeException("Edge with user id already exists:" + userId);
            }

            Document edgeDoc = rawLucene.findByUserId(userId.toString());
            rawLucene.indexLock();
            try {
                if (edgeDoc == null) {
                    if (id < 0)
                        id = atomicCounter.incrementAndGet();

                    edgeDoc = rawLucene.createDocument(userId, id, Edge.class);
                    edgeDoc.add(getMapping(Edge.class.getSimpleName()).createField(RawLucene.EDGE_LABEL, label));
                }

                rawLucene.initRelation(edgeDoc, ((LuceneElement) outVertex).getRaw(), ((LuceneElement) inVertex).getRaw());
                rawLucene.fastPut(id, edgeDoc);
            } finally {
                rawLucene.indexUnlock();
            }
            return new LuceneEdge(this, edgeDoc);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    @Override public Edge getEdge(final Object id) {
        Document doc = rawLucene.findByUserId(id.toString());
        if (doc == null)
            return null;

        return new LuceneEdge(this, doc);
    }

    @Override public void removeEdge(final Edge edge) {
        rawLucene.removeById((Long) edge.getId());
    }

    @Override public void removeVertex(final Vertex vertex) {
        rawLucene.removeById((Long) vertex.getId());
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
        return 50 * 1024;
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
        // TODO flush here or use lock of RawLucene?
    }

    public RawLucene getRaw() {
        return rawLucene;
    }

    @Override public String toString() {
        return rawLucene.toString();
    }

    public long count(Class cl, String fieldName, Object value) {
        return rawLucene.count(cl, fieldName, value);
    }

    void refresh() {
        rawLucene.refresh();
    }

    Mapping getMapping(String type) {
        return rawLucene.getMapping(type);
    }

    Mapping getMapping(Class cl) {
        return rawLucene.getMapping(cl);
    }
}