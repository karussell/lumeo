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
package de.jetsli.lumeo;

import java.io.File;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.NRTManager;
import org.apache.lucene.search.NRTManager.TrackingIndexWriter;
import org.apache.lucene.search.SearcherFactory;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Peter Karich, info@jetsli.de
 */
public class ReadESIndexViaLucene {

    public static void main(String[] args) throws Exception {
        Logger logger = LoggerFactory.getLogger(ReadESIndexViaLucene.class);
        IndexWriterConfig cfg = new IndexWriterConfig(Version.LUCENE_40, new StandardAnalyzer(Version.LUCENE_40));
        IndexWriter writer = new IndexWriter(FSDirectory.open(new File("../elasticsearch/data/jetwickcluster/nodes/0/indices/twindex/0/index")),
                cfg);     
        TrackingIndexWriter trackingWriter = new TrackingIndexWriter(writer);
        
        NRTManager nrtManager = new NRTManager(trackingWriter, new SearcherFactory() {
               // TODO warm up by getting some random vertices via getVertices?
        });
        IndexSearcher searcher = nrtManager.acquire();
        try {
            TopDocs td = searcher.search(new MatchAllDocsQuery(), 10);
            logger.info("results:" + td.totalHits);
            Document doc = searcher.doc(td.scoreDocs[0].doc);
            for (IndexableField f : doc.getFields()) {
                logger.info(f.name() + " " + f.stringValue());
            }
            logger.info(doc.get("tweet/tw"));
        } finally {
            nrtManager.release(searcher);
        }
    }
}
