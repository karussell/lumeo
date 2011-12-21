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

import de.jetsli.lumeo.RawLucene;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.DateTools;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.NumericField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;

/**
 * This class defines how specific field will be indexed and queried (if stored, which analyzers, ...)
 * 
 * @author Peter Karich, info@jetsli.de
 */
public class Mapping {

    public static final Analyzer WHITESPACE_ANALYZER = new WhitespaceAnalyzer(RawLucene.VERSION);
    public static final Analyzer STANDARD_ANALYZER = new StandardAnalyzer(RawLucene.VERSION);
    public static final Analyzer KEYWORD_ANALYZER = new KeywordAnalyzer();
    public static final Analyzer KEYWORD_ANALYZER_LC = new KeywordAnalyzerLowerCase(RawLucene.VERSION);

    public enum Type {

        DATE, LONG, DOUBLE, STRING, STRING_LC, TEXT
    }
    private final FieldType storedFieldType;
    private final FieldType indexedFieldType;
    private final Map<String, Type> fieldToTypeMapping;
    private final LumeoPerFieldAnalyzer analyzer;
    private String type;

    public Mapping(String type) {
        this.type = type;
        
        // TODO use one field _source instead of several fields
        storedFieldType = new FieldType();        
        storedFieldType.setStored(true);        
        storedFieldType.setOmitNorms(true);    
        storedFieldType.setIndexOptions(IndexOptions.DOCS_ONLY);
        storedFieldType.setIndexed(false);
        storedFieldType.freeze();

        indexedFieldType = new FieldType();
        indexedFieldType.setIndexOptions(IndexOptions.DOCS_ONLY);
        indexedFieldType.setOmitNorms(true);    
        indexedFieldType.setStoreTermVectors(false);        
        indexedFieldType.setStored(true);
        indexedFieldType.setIndexed(true);
        indexedFieldType.freeze();

        analyzer = new LumeoPerFieldAnalyzer(getDefaultAnalyzer());
        fieldToTypeMapping = new LinkedHashMap<String, Type>(4);
        //putField(RawLucene.ID, Type.LONG);
        putField(RawLucene.UID, Type.STRING);
        putField(RawLucene.TYPE, Type.STRING);
        putField(RawLucene.EDGE_LABEL, Type.STRING);
    }

    public Mapping(Class cl) {
        this(cl.getSimpleName());
    }

    public Type getDefaultType() {
        return Type.STRING_LC;
    }

    public Analyzer getDefaultAnalyzer() {
        return KEYWORD_ANALYZER_LC;
    }

    /** @return true if no previous type was overwritten */
    public Type putField(String key, Type type) {
        Type oldType = fieldToTypeMapping.put(key, type);
        switch (type) {
            case TEXT:
                analyzer.putAnalyzer(key, STANDARD_ANALYZER);
                break;
            case STRING_LC:
                analyzer.putAnalyzer(key, KEYWORD_ANALYZER_LC);
                break;
            case DATE:                
            case STRING:                
            case DOUBLE:                
            case LONG:
                analyzer.putAnalyzer(key, KEYWORD_ANALYZER);
                break;
            default:
                throw new IllegalStateException("something went wrong while determing analyzer for type " + type);
        }
        return oldType;
    }

    public Set<String> getIndexedFields() {
        return fieldToTypeMapping.keySet();
    }

    public Field createField(String key, Object value) {
        Type t = fieldToTypeMapping.get(key);

        // if no mapping found -> gets not indexed but stored
        if (t == null)
            return new Field(key, value.toString(), storedFieldType);

        switch (t) {
            case DATE:
                return newDateField(key, ((Date) value).getTime());
            case STRING:
                if (getAnalyzerFor(key) == KEYWORD_ANALYZER)
                    return newStringField(key, (String) value);
                else
                    throw new IllegalStateException("Internal Problem: Mapping Analyzer "
                            + getAnalyzerFor(key) + " does not match type " + Type.STRING);
            case STRING_LC:
                if (getAnalyzerFor(key) == KEYWORD_ANALYZER_LC)
                    return newStringField(key, KeywordAnalyzerLowerCase.transform((String) value));
                else
                    throw new IllegalStateException("Internal Problem: Mapping Analyzer "
                            + getAnalyzerFor(key) + " does not match type " + Type.STRING_LC);
            case TEXT:
                return newTextField(key, (String) value);
            case LONG:
                return newLongField(key, ((Number) value).longValue());
            default:
                throw new IllegalStateException("something went wrong while determining field type");
        }
    }

    public Field newDateField(String name, long value) {
        // TODO performance shouldn't we round the long and use long instead of string?
        return new Field(name, DateTools.timeToString(value, DateTools.Resolution.MINUTE), indexedFieldType);
    }

    public Field newLongField(String name, long id) {
        NumericField idField = new NumericField(name, 6, NumericField.TYPE_STORED).setLongValue(id);
        return idField;
    }

    /** Creates a numerical identification */
    public Field newIdField(String name, long id) {
        NumericField idField = new NumericField(name, 6, NumericField.TYPE_STORED).setLongValue(id);
        return idField;
    }

    /** Creates a user specified identification */
    public Field newUIdField(String name, String val) {
        return newStringField(name, val);
    }

    /** Necessary for keywordanalyzer */
    Field newStringField(String name, String val) {
        Field field = new Field(name, val, StringField.TYPE_STORED);
        return field;
    }

    Field newTextField(String name, String val) {
        return new TextField(name, val);
    }

    public boolean exists(String key) {
        return fieldToTypeMapping.containsKey(key);
    }

    /** @return the analyzer per field */
    public Analyzer getAnalyzerFor(String field) {
        return analyzer.getWrappedAnalyzer(field);
    }

    public Analyzer getCombinedAnalyzer() {
        return analyzer;
    }

    @Override public String toString() {
        return type + ", fields:" + fieldToTypeMapping;
    }

    public BytesRef toBytes(String fieldName, Object o) {
        if (o instanceof String) {
            if (getAnalyzerFor(fieldName) == KEYWORD_ANALYZER_LC)
                return new BytesRef(((String) o).toLowerCase());
            else
                return new BytesRef((String) o);
        } else if (o instanceof Integer)
            return LuceneHelper.newRefFromInt((Integer) o);
        else if (o instanceof Long)
            return LuceneHelper.newRefFromLong((Long) o);
        else if (o instanceof Double)
            return LuceneHelper.newRefFromDouble((Double) o);
        else if (o instanceof Date)
            return new BytesRef(DateTools.timeToString(((Date) o).getTime(), DateTools.Resolution.MINUTE));
        else
            throw new UnsupportedOperationException(
                    "Couldn't find bytesRef usage for object  " + o);
    }

    public Query getQuery(String field, Object o) {
        Analyzer a = getAnalyzerFor(field);
        if (a == KEYWORD_ANALYZER_LC)
            return new TermQuery(new Term(field, ((String) o).toLowerCase()));
        else if (a == KEYWORD_ANALYZER)
            return new TermQuery(new Term(field, (String) o));

        try {
            return new QueryParser(RawLucene.VERSION, field, a).parse(o.toString());
        } catch (ParseException ex) {
            throw new RuntimeException(ex);
        }
    }
}
