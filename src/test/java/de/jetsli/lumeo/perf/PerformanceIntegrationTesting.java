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
package de.jetsli.lumeo.perf;

import com.tinkerpop.blueprints.pgm.Vertex;
import de.jetsli.lumeo.LuceneGraph;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;
import de.jetsli.lumeo.SimpleLuceneTestBase;
import de.jetsli.lumeo.util.StopWatch;
import java.io.File;
import java.util.Random;

/**
 *
 * @author Peter Karich, info@jetsli.de
 */
public class PerformanceIntegrationTesting extends SimpleLuceneTestBase {

    List<Vertex> previousVertices;
    Random rand;
    int TRIALS = 10;

    @Override
    public void setUp() {
        super.setUp();
        previousVertices = new ArrayList<Vertex>();
        rand = new Random(1);
    }

    @Test public void testIndexing() {
        logger.info("starting");
        reinitFileBasedGraph();
        // until 50k => warming jvm
        for (int i = 0; i < 50000; i++) {
            connect(i);
        }
        float allSecs = 0;
        for (int trial = 0; trial < TRIALS; trial++) {            
            reinitFileBasedGraph();
            StopWatch sw = new StopWatch("perf" + trial).start();
            for (int i = 0; i < 100000; i++) {
                connect(i);
            }
            logger.info(sw.stop().toString());
            allSecs = sw.getSeconds();
        }
        logger.info("finished " + allSecs / TRIALS);
    }

    private void connect(int i) {
        Vertex v1;
        Vertex v2;

        if (previousVertices.isEmpty() || rand.nextInt(10) < 5)
            v1 = g.addVertex(null);
        else
            v1 = previousVertices.get(rand.nextInt(previousVertices.size()));

        if (previousVertices.isEmpty() || rand.nextInt(10) < 5)
            v2 = g.addVertex(null);
        else
            v2 = previousVertices.get(rand.nextInt(previousVertices.size()));

        previousVertices.add(v1);
        previousVertices.add(v2);
        if (rand.nextInt(5000) < 10)
            previousVertices.clear();

        g.addEdge(null, v1, v2, "e" + i);
    }
}
