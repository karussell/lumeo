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

import java.util.HashMap;
import java.util.Map;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Analyzer.TokenStreamComponents;
import org.apache.lucene.analysis.AnalyzerWrapper;

/**
 * 
 * @author nearly copied from lucene
 */
public final class LumeoPerFieldAnalyzer extends AnalyzerWrapper {

    private final Analyzer defaultAnalyzer;
    private final Map<String, Analyzer> analyzerMap = new HashMap<String, Analyzer>();

    /**
     * Constructs with default analyzer and a map of analyzers to use for 
     * specific fields.
     *
     * @param defaultAnalyzer Any fields not specifically
     * defined to use a different analyzer will use the one provided here.     
     */
    public LumeoPerFieldAnalyzer(Analyzer defaultAnalyzer) {
        this.defaultAnalyzer = defaultAnalyzer;
    }

    /**
     * Defines an analyzer to use for the specified field.
     *
     * @param fieldName field name requiring a non-default analyzer
     * @param analyzer non-default analyzer to use for field     
     */
    public void putAnalyzer(String fieldName, Analyzer analyzer) {
        analyzerMap.put(fieldName, analyzer);
    }

    @Override protected Analyzer getWrappedAnalyzer(String fieldName) {
        Analyzer a = analyzerMap.get(fieldName);
        if (a == null)
            return defaultAnalyzer;
        return a;
    }        

    @Override protected TokenStreamComponents wrapComponents(String fieldName, TokenStreamComponents components) {
        return components;
    }

    @Override public String toString() {
        return "LumeoPerFieldAnalyzer(" + analyzerMap + ", default=" + defaultAnalyzer + ")";
    }
}
