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

import java.io.IOException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Scorer;

/**
 *
 * @author Peter Karich, info@jetsli.de
 */
class CountCollector extends Collector {

    long count = 0;

    public CountCollector() {        
    }

    public long getCount() {
        return count;
    }

    @Override
    public void setScorer(Scorer scorer) throws IOException {
    }

    @Override
    public void collect(int doc) throws IOException {
        count++;
    }

    @Override
    public void setNextReader(IndexReader reader, int docBase) throws IOException {
    }

    @Override
    public boolean acceptsDocsOutOfOrder() {
        return true;
    }
}
