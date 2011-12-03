/*
 *  Copyright 2011 Peter Karich jetwick_@_pannous_._info
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
package com.pannous.lumeo;

import com.pannous.lumeo.util.TermFilter;
import com.tinkerpop.blueprints.pgm.CloseableSequence;
import java.io.IOException;
import java.util.Iterator;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanFilter;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;

/**
 *
 * @author Peter Karich, info@jetsli.de
 */
public abstract class LuceneFilterSequence<T> implements CloseableSequence<T> {

    protected LuceneGraph g;
    private Filter baseFilter;
    private Filter filter;
    private int n = 10;
    private int index = 0;
    private TopDocs docs;
    private IndexSearcher searcher;
    private Query query = new MatchAllDocsQuery();
    private boolean closed = false;
    private Analyzer analyzer;

    public LuceneFilterSequence(LuceneGraph g, Class<T> type) {
        this.g = g;
        baseFilter = new TermFilter(new Term(RawLucene.TYPE).createTerm(type.getSimpleName()));
        analyzer = g.getAnalyzer();
        searcher = g.getRaw().newUnmanagedSearcher();
    }

    public Filter getBaseFilter() {
        return baseFilter;
    }        

    protected abstract T createElement(Document doc);

    public LuceneFilterSequence<T> setN(int hitsPerPage) {
        n = hitsPerPage;
        return this;
    }

    public LuceneFilterSequence<T> setValue(String field, Object o) {
        // TODO use analyzer !
        query = new TermQuery(new Term(field).createTerm(o.toString()));
        return this;
    }

    public LuceneFilterSequence<T> setFilter(Filter filter) {
        this.filter = filter;
        return this;
    }

    @Override
    public boolean hasNext() {
        if (docs == null) {
            try {
                if (filter == null)
                    filter = getBaseFilter();
                else {
                    if (getBaseFilter() != null) {
                        BooleanFilter newFilter = new BooleanFilter();
                        newFilter.add(getBaseFilter(), Occur.MUST);
                        newFilter.add(filter, Occur.MUST);
                        filter = newFilter;
                    }
                }

                docs = searcher.search(query, filter, n);
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }
        return index < docs.scoreDocs.length;
    }

    @Override
    public T next() {
        try {
            if (!hasNext())
                throw new UnsupportedOperationException("no further element");

            if (index >= docs.scoreDocs.length)
                docs = searcher.searchAfter(docs.scoreDocs[n - 1], query, filter, n);

            Document doc = searcher.doc(docs.scoreDocs[index++].doc);
            return createElement(doc);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
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
    public Iterator<T> iterator() {
        return this;
    }
}
