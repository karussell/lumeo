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
package com.pannous.lumeo;

import com.tinkerpop.blueprints.pgm.Vertex;
import org.apache.lucene.document.Document;

/**
 * This Class iterates through all vertices or only a subset if a filter is specified.
 * 
 * @author Peter Karich, info@jetsli.de
 */
public class VertexFilterSequence extends LuceneFilterSequence<Vertex> {

    public VertexFilterSequence(LuceneGraph rl) {
        super(rl, Vertex.class);
    }

    @Override protected Vertex createElement(Document doc) {
        return new LuceneVertex(g, doc);
    }
}
