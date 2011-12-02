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
package com.pannous.tmpo.util;

import com.tinkerpop.blueprints.pgm.CloseableSequence;
import com.tinkerpop.blueprints.pgm.Edge;
import java.util.Iterator;

/**
 *
 * @author Peter Karich, info@jetsli.de
 */
public class Helper {

//    public static Iterator<Edge> EMPTY_EDGE_ITERATOR = new EmptyIterator<Edge>();    
//
//    private static class EmptyIterator<T> implements Iterator<T> {
//
//        @Override
//        public boolean hasNext() {
//            return false;
//        }
//
//        @Override
//        public T next() {
//            throw new UnsupportedOperationException("Empty Collection => Empty Iterator");
//        }
//
//        @Override
//        public void remove() {
//            throw new UnsupportedOperationException("Empty Collection => Empty Iterator");
//        }
//    }
    public static CloseableSequence<Edge> EMPTY_EDGE_SEQUENCE = new EmptyCloseableSequence<Edge>();    

    private static class EmptyCloseableSequence<T> implements CloseableSequence<T> {

        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public T next() {
            throw new UnsupportedOperationException("Empty Collection => Empty Iterator");
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("Empty Collection => Empty Iterator");
        }

        @Override
        public void close() {            
        }

        @Override
        public Iterator<T> iterator() {
            return this;
        }
    }    
}
