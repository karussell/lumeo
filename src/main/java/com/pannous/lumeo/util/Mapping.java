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
package com.pannous.lumeo.util;

import com.pannous.lumeo.RawLucene;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.KeywordAnalyzer;
import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.DateTools;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.document.NumericField;
import org.apache.lucene.index.FieldInfo.IndexOptions;

/**
 * This class defines how specific field will be indexed and queried (if stored, which analyzers, ...)
 * 
 * @author Peter Karich, info@jetsli.de
 */
public class Mapping {

    public static final Analyzer WHITESPACE_ANALYZER = new WhitespaceAnalyzer(RawLucene.VERSION);
    public static final Analyzer STANDARD_ANALYZER = new StandardAnalyzer(RawLucene.VERSION);
    public static final Analyzer KEYWORD_ANALYZER = new KeywordAnalyzer();

    public enum Type {

        DATE, LONG, STRING, TEXT
    }
    // store all fields per default 
    // TODO use _source
    private Field.Store store = Field.Store.YES;
    private Map<String, Type> fieldToTypeMapping = new LinkedHashMap<String, Type>();
    private LumeoPerFieldAnalyzer analyzer = new LumeoPerFieldAnalyzer(KEYWORD_ANALYZER);
    private String type;

    public Mapping(String type) {
        this.type = type;
    }

    /** @return true if no previous type was overwritten */
    public Type putField(String key, Type type) {
        Type oldType = fieldToTypeMapping.put(key, type);                
        switch (type) {
            case TEXT:
                analyzer.addAnalyzer(key, STANDARD_ANALYZER);
                break;
            case DATE:
            case STRING:
            case LONG:
                if (getAnalyzer(key) != KEYWORD_ANALYZER)
                    throw new IllegalStateException("Internal Problem: Mapping Analyzer "
                            + getAnalyzer(key) + " does not match type " + Type.STRING);
                break;
            default:
                throw new IllegalStateException("something went wrong while determing analyzer for type " + type);
        }
        return oldType;
    }

    public Set<String> getIndexedFields() {
        return fieldToTypeMapping.keySet();
    }

    public Fieldable createField(String key, Object value) {
        Type t = fieldToTypeMapping.get(key);

        // if no mapping found -> gets not indexed
        if (t == null)
            return new Field(key, value.toString(), store, Field.Index.NO);

        switch (t) {
            case DATE:
                return newDateField(key, ((Date) value).getTime());
            case STRING:
                if (getAnalyzer(key) == KEYWORD_ANALYZER)
                    return newStringField(key, (String) value);
                else
                    throw new IllegalStateException("Internal Problem: Mapping Analyzer "
                            + getAnalyzer(key) + " does not match type " + Type.STRING);
            case TEXT:
                return newAnalyzedStringField(key, (String) value);
            case LONG:
                return newLongField(key, (Long) value);
            default:
                throw new IllegalStateException("something went wrong while determining field type");
        }
    }

    public Fieldable newDateField(String name, long value) {
        return new Field(name, DateTools.timeToString(value, DateTools.Resolution.MINUTE),
                store, Field.Index.NOT_ANALYZED);
    }

    public Fieldable newLongField(String name, long id) {
        NumericField idField = new NumericField(name, 4, store, true).setLongValue(id);
        idField.setIndexOptions(IndexOptions.DOCS_ONLY);
        return idField;
    }

    /** Creates a numerical identification */
    public Fieldable newIdField(String name, long id) {
        NumericField idField = new NumericField(name, 6, store, true).setLongValue(id);
        idField.setIndexOptions(IndexOptions.DOCS_ONLY);
        return idField;
    }

    /** Creates a user specified identification */
    public Fieldable newUIdField(String name, String val) {
        return newStringField(name, val);
    }

    /** Necessary for keywordanalyzer */
    public Fieldable newStringField(String name, String val) {
        Field field = new Field(name, val, store, Field.Index.NOT_ANALYZED_NO_NORMS);
        field.setIndexOptions(IndexOptions.DOCS_ONLY);
        return field;
    }

    public Fieldable newAnalyzedStringField(String name, String val) {
        return new Field(name, val, store, Field.Index.ANALYZED_NO_NORMS);
    }

    public boolean exists(String key) {
        return fieldToTypeMapping.containsKey(key);
    }

    /** @return the analyzer per field */
    public Analyzer getAnalyzer(String field) {
        return analyzer.getAnalyzer(field);
    }

    public Analyzer getAnalyzerWrapper() {
        return analyzer;
    }

    @Override public String toString() {
        return type + " - mapping:" + fieldToTypeMapping;
    }
}
