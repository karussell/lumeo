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
import org.apache.lucene.index.IndexReader;

/**
 *
 * @author Peter Karich, info@jetsli.de
 */
public abstract class MyGather {

    private final IndexReader topReader;
    private boolean continueRun = false;

    public MyGather(IndexReader r) {
        topReader = r;
    }

    public int run() throws IOException {
        return run(0, topReader);
    }

    public int run(int docBase) throws IOException {
        return run(docBase, topReader);
    }

    private int run(int base, IndexReader reader) throws IOException {
        throw new RuntimeException("not implemented in latest 4.0");
        
//        IndexReader[] subReaders = reader.getSequentialSubReaders();
//        if (subReaders == null) {
//            // atomic/leaf reader
//            continueRun = runLeaf(base, reader);
//            base += reader.maxDoc();
//        } else {
//            // composite reader
//            for (int i = 0; i < subReaders.length; i++) {
//                base = run(base, subReaders[i]);
//                if (!continueRun)
//                    break;
//            }
//        }
//        return base;
    }

    /**
     * @return true if further runLeaf calls should be made, false if Gather should finish.     
     */
    protected abstract boolean runLeaf(int base, IndexReader leaf) throws IOException;
}
