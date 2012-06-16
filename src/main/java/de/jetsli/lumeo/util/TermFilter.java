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
package de.jetsli.lumeo.util;

import java.io.IOException;

import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;

/**
 * @author Peter Karich, info@jetsli.de
 */
public class TermFilter extends Filter {

    private String fieldName;
    private BytesRef bytes;

    public TermFilter(String fieldName, BytesRef bytes) {
        if (bytes == null)
            throw new NullPointerException("Term cannot be null");
        this.bytes = bytes;
        this.fieldName = fieldName;
    }
    
    @Override
    public DocIdSet getDocIdSet(AtomicReaderContext context, Bits acceptDocs) throws IOException {        
        AtomicReader reader = context.reader();
        FixedBitSet result = new FixedBitSet(reader.maxDoc());
        DocsEnum de = reader.termDocsEnum(acceptDocs, fieldName, bytes, false);
        if(de == null)
            return result;
        
        int id;
        while ((id = de.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
            result.set(id);
        }
        return result;
    }
}
