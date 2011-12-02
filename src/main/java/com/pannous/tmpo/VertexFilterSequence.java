/*
 *  Copyright 2011 Peter Karich info@jetsli.de
 * 
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 * 
 *       http://www.apache.org/licenses/LICENSE-2.0
 * 
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.pannous.tmpo;

import com.tinkerpop.blueprints.pgm.CloseableSequence;
import com.tinkerpop.blueprints.pgm.Vertex;
import java.io.IOException;
import java.util.Iterator;
import org.apache.lucene.document.Document;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;

/**
 * This Class iterates through all vertices or only a subset if a filter is specified.
 * 
 * @author Peter Karich, info@jetsli.de
 */
class VertexFilterSequence implements CloseableSequence<Vertex> {

    private Filter filter;
    private LuceneGraph g;
    private int n;
    private int index = 0;
    private TopDocs docs;
    private IndexSearcher searcher;
    private Query mAllQuery = new MatchAllDocsQuery();
    private boolean closed = false;

    public VertexFilterSequence(LuceneGraph rl) {
        this(rl, null, 10);
    }

    public VertexFilterSequence(LuceneGraph rl, Filter filter) {
        this(rl, filter, 10);
    }

    public VertexFilterSequence(LuceneGraph rl, Filter filter, int n) {
        this.filter = filter;
        this.g = rl;
        this.n = n;
        searcher = g.getRaw().newUnmanagedSearcher();
    }

    public void setFilter(Filter filter) {
        this.filter = filter;
    }

    @Override
    public boolean hasNext() {
        if (docs == null) {
            try {
                docs = searcher.search(mAllQuery, filter, n);
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            } finally {
                close();
            }
        }
        return index < docs.scoreDocs.length;
    }

    @Override
    public Vertex next() {
        try {
            if (!hasNext())
                throw new UnsupportedOperationException("no further element");

            if (index >= docs.scoreDocs.length)
                docs = searcher.searchAfter(docs.scoreDocs[n - 1], mAllQuery, filter, n);
            Document doc = searcher.doc(docs.scoreDocs[index++].doc);

            return new LuceneVertex(g, doc);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        } finally {
            close();
        }
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void close() {
        if (!closed) {
            g.getRaw().releaseUnmanagedSearcher(searcher);
            closed = true;
        }
    }

    @Override
    public Iterator<Vertex> iterator() {
        return this;
    }
}
