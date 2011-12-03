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

import com.pannous.tmpo.RawLucene;
import java.io.Reader;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.KeywordAnalyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Fieldable;

/**
 *
 * @author Peter Karich, info@jetsli.de
 */
public final class SelectiveAnalyzer extends Analyzer {

    public static final Analyzer WHITESPACE_ANALYZER = new WhitespaceAnalyzer(RawLucene.VERSION);
    public static final Analyzer STANDARD_ANALYZER = new StandardAnalyzer(RawLucene.VERSION);
    public static final Analyzer KEYWORD_ANALYZER = new KeywordAnalyzer();
    public final Analyzer defaultAnalyzer;

    public SelectiveAnalyzer() {
        this(KEYWORD_ANALYZER);
    }

    public SelectiveAnalyzer(Analyzer defaultAnalyzer) {
        this.defaultAnalyzer = defaultAnalyzer;
    }

    @Override public TokenStream tokenStream(String field, final Reader reader) {
        return getAnalyzer(field).tokenStream(field, reader);
    }

    Analyzer getAnalyzer(String field) {
        if (field.endsWith("_s"))
            return KEYWORD_ANALYZER;
        else if (field.endsWith("_t"))
            return STANDARD_ANALYZER;
        else if (field.endsWith("_ws"))
            return WHITESPACE_ANALYZER;
        else
            return defaultAnalyzer;
    }

    @Override public int getPositionIncrementGap(String fieldName) {
        return getAnalyzer(fieldName).getPositionIncrementGap(fieldName);
    }

    @Override public int getOffsetGap(Fieldable field) {
        return getAnalyzer(field.name()).getOffsetGap(field);
    }

    @Override public String toString() {
        return "SelectiveAnalyzer(default=" + defaultAnalyzer + ")";
    }
}
