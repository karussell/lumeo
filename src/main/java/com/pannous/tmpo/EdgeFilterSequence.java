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

import com.tinkerpop.blueprints.pgm.Edge;
import org.apache.lucene.document.Document;

/**
 * Class traverses all edges (or a subset if filter is specified)
 * 
 * @author Peter Karich, info@jetsli.de
 */
public class EdgeFilterSequence extends LuceneFilterSequence<Edge> {

    public EdgeFilterSequence(LuceneGraph rl) {
        super(rl, Edge.class);
    }

    @Override protected Edge createElement(Document doc) {
        return new LuceneEdge(g, doc);
    }
}
