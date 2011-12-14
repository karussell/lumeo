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

import java.io.Reader;
import org.apache.lucene.analysis.KeywordTokenizer;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.ReusableAnalyzerBase;
import org.apache.lucene.analysis.ReusableAnalyzerBase.TokenStreamComponents;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.util.Version;

/**
 *
 * @author Peter Karich, info@jetsli.de
 */
public class KeywordAnalyzerLowerCase extends ReusableAnalyzerBase {
    
    private Version version;

    public KeywordAnalyzerLowerCase(Version version) {
        this.version = version;
    }
    
    @Override
    protected TokenStreamComponents createComponents(final String fieldName, final Reader reader) {
        Tokenizer tokenizer = new KeywordTokenizer(reader);
        TokenStream stream = new LowerCaseFilter(version, tokenizer);
        return new TokenStreamComponents(tokenizer, stream);
    }
}