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
import org.slf4j.Logger;
import com.tinkerpop.blueprints.pgm.CloseableSequence;
import de.jetsli.lumeo.util.Helper;
import org.junit.After;
import org.junit.Before;
import org.slf4j.LoggerFactory;
import static org.junit.Assert.*;

/**
 *
 * @author Peter Karich, info@jetsli.de
 */
public class SimpleLuceneTestBase {

    protected Logger logger = LoggerFactory.getLogger(SimpleLuceneTestBase.class);
    protected LuceneGraph g;

    @Before public void setUp() {
        g = new LuceneGraph();
    }

    @After public void tearDown() {
        if (g != null)
            g.shutdown();
    }

    protected void refresh() {
        g.refresh();
    }

    protected void reinitFileBasedGraph() {
        g.shutdown();
        Helper.deleteDir(new File("test-lumeo"));
        g = new LuceneGraph("test-lumeo");
    }

    public void assertCount(int exp, CloseableSequence seq) {
        int c = 0;
        while (seq.hasNext()) {
            seq.next();
            c++;
        }
        seq.close();
        assertEquals("length of sequence does not match", exp, c);
    }
}
