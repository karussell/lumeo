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
import org.apache.lucene.analysis.KeywordAnalyzer;
import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.DateTools;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.document.NumericField;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.NumericUtils;

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
    // store all fields per default 
    // TODO use _source
    private Field.Store store = Field.Store.YES;
    private final Map<String, Type> fieldToTypeMapping;
    private final LumeoPerFieldAnalyzer analyzer;
    private String type;

    public Mapping(String type) {
        this.type = type;
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
                analyzer.putAnalyzer(key, KEYWORD_ANALYZER);
                break;
            case STRING:
                analyzer.putAnalyzer(key, KEYWORD_ANALYZER);
                break;
            case DOUBLE:
                analyzer.putAnalyzer(key, KEYWORD_ANALYZER);
                break;
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
            case STRING_LC:
                if (getAnalyzer(key) == KEYWORD_ANALYZER_LC)
                    return newStringField(key, KeywordAnalyzerLowerCase.transform((String) value));
                else
                    throw new IllegalStateException("Internal Problem: Mapping Analyzer "
                            + getAnalyzer(key) + " does not match type " + Type.STRING_LC);
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
        NumericField idField = new NumericField(name, 6, store, true).setLongValue(id);
        idField.setIndexOptions(IndexOptions.DOCS_ONLY);
        return idField;
    }

    /** Creates a numerical identification */
<<<<<<< HEAD
    public Fieldable newIdField(String name, long id) {
        NumericField idField = new NumericField(name, 6, store, true).setLongValue(id);
        idField.setIndexOptions(IndexOptions.DOCS_ONLY);
=======
    public Field newIdField(String name, long id) {
        NumericField idField = new NumericField(name, 6, NumericField.TYPE_STORED).setLongValue(id);        
>>>>>>> 125c1a1... added more fine grained perf analysis
        return idField;
    }

    /** Creates a user specified identification */
    public Fieldable newUIdField(String name, String val) {
        return newStringField(name, val);
    }

    /** Necessary for keywordanalyzer */
    Fieldable newStringField(String name, String val) {
        Field field = new Field(name, val, store, Field.Index.NOT_ANALYZED_NO_NORMS);
        field.setIndexOptions(IndexOptions.DOCS_ONLY);
        return field;
    }

    Fieldable newAnalyzedStringField(String name, String val) {
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
        return type + ", fields:" + fieldToTypeMapping;
    }

    public Term toTerm(String fieldName, Object o) {
        Term t = new Term(fieldName);
        if (o instanceof String) {
            if (getAnalyzer(fieldName) == KEYWORD_ANALYZER_LC)
                return t.createTerm(((String) o).toLowerCase());
            else
                return t.createTerm((String) o);
        } else if (o instanceof Long)
            return t.createTerm(NumericUtils.longToPrefixCoded((Long) o));
        else if (o instanceof Double)
            return t.createTerm(NumericUtils.doubleToPrefixCoded((Double) o));
        else if (o instanceof Date)
            return t.createTerm(DateTools.timeToString(((Date) o).getTime(), DateTools.Resolution.MINUTE));
        else
            throw new UnsupportedOperationException("couldn't transform into string " + o);
    }

    public Query getQuery(String field, Object o) {
        Analyzer a = getAnalyzer(field);
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
