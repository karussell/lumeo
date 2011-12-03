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
package com.pannous.tmpo.util;

import java.io.IOException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.FixedBitSet;

/**
 * @author Peter Karich, info@jetsli.de
 */
public class TermFilter extends Filter {

    private Term term;

    public TermFilter(Term term) {
        if (term == null)
            throw new NullPointerException("Term cannot be null");
        this.term = term;
    }

    /* (non-Javadoc)
     * @see org.apache.lucene.search.Filter#getDocIdSet(org.apache.lucene.index.IndexReader)
     */
    @Override public DocIdSet getDocIdSet(IndexReader reader) throws IOException {
        FixedBitSet result = new FixedBitSet(reader.maxDoc());
        TermDocs td = reader.termDocs();
        try {
            td.seek(term);
            while (td.next()) {
                result.set(td.doc());
            }
        } finally {
            td.close();
        }
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || getClass() != obj.getClass())
            return false;
        final TermFilter other = (TermFilter) obj;
        return this.term == other.term || this.term.equals(other.term);
    }

    @Override
    public int hashCode() {
        return 13 * 7 + this.term.hashCode();
    }
}
