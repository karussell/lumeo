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
package com.pannous.lumeo;

import java.io.File;
import java.io.IOException;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.NRTManager;
import org.apache.lucene.search.SearcherWarmer;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Peter Karich, info@jetsli.de
 */
public class Tester {

    public static void main(String[] args) throws Exception {
        Logger logger = LoggerFactory.getLogger(Tester.class);
        IndexWriterConfig cfg = new IndexWriterConfig(Version.LUCENE_34, new StandardAnalyzer(Version.LUCENE_34));
        IndexWriter writer = new IndexWriter(FSDirectory.open(new File("/home/peterk/Dokumente/quell/jetslide/es/data/jetwickcluster/nodes/0/indices/twindex/0/index")),
                cfg);        
        NRTManager nrtManager = new NRTManager(writer, new SearcherWarmer() {

            @Override
            public void warm(IndexSearcher s) throws IOException {
                // TODO get some random vertices via getVertices?
            }
        });
        IndexSearcher searcher = nrtManager.getSearcherManager(true).acquire();
        try {
            TopDocs td = searcher.search(new MatchAllDocsQuery(), 10);
            logger.info("results:" + td.totalHits);
            Document doc = searcher.doc(td.scoreDocs[0].doc);
            for (Fieldable f : doc.getFields()) {
                logger.info(f.name() + " " + f.stringValue());
            }
            logger.info(doc.get("tweet/tw"));
        } finally {
            nrtManager.getSearcherManager(true).release(searcher);
        }
    }
}
