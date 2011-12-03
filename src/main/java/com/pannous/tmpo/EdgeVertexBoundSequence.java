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

import com.pannous.tmpo.util.TermFilter;
import com.tinkerpop.blueprints.pgm.Vertex;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanFilter;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.TermsFilter;
import org.apache.lucene.util.NumericUtils;

/**
 *
 * @author Peter Karich, info@jetsli.de
 */
public class EdgeVertexBoundSequence extends EdgeFilterSequence {

    private final LuceneVertex vertexDoc;
    private final String[] edgeTypes;
    private String[] edgeLabels;
    private BooleanFilter edgeFilter;

    public EdgeVertexBoundSequence(LuceneGraph g, LuceneVertex vertex, String... edgeTypes) {
        super(g);
        this.vertexDoc = vertex;
        this.edgeTypes = edgeTypes;
    }

    public EdgeVertexBoundSequence setLabels(String... labels) {
        this.edgeLabels = labels;
        return this;
    }

    @Override public Filter getBaseFilter() {
        if (edgeFilter == null) {
            // 1. restrict to edges only
            edgeFilter = new BooleanFilter();
            edgeFilter.add(super.getBaseFilter(), Occur.MUST);

            // 2. restrict to in or out edges
            if (edgeTypes != null) {
                if (edgeTypes.length == 1) {
                    String vertexField = RawLucene.getVertexFieldForEdgeType(edgeTypes[0]);
                    String idStr = NumericUtils.longToPrefixCoded((Long) vertexDoc.getId());
                    edgeFilter.add(new TermFilter(new Term(vertexField).createTerm(idStr)), Occur.MUST);
                }
                // no restriction as both types are accepted
            }

            // 3. restrict to one or more edge labels
            if (edgeLabels != null && edgeLabels.length > 0) {
                TermsFilter tf = new TermsFilter();
                for (String label : edgeLabels) {
                    tf.addTerm(new Term(RawLucene.EDGE_LABEL).createTerm(label));
                }
                edgeFilter.add(tf, Occur.MUST);
            }
        }
        return edgeFilter;
    }
}
