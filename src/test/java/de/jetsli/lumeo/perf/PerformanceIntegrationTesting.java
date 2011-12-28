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
package de.jetsli.lumeo.perf;

import com.tinkerpop.blueprints.pgm.Edge;
import com.tinkerpop.blueprints.pgm.Vertex;
import de.jetsli.lumeo.RawLucene;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;
import static org.junit.Assert.*;
import de.jetsli.lumeo.SimpleLuceneTestBase;
import de.jetsli.lumeo.util.StopWatch;
import java.util.Random;
import org.apache.lucene.document.Document;

/**
 *
 * @author Peter Karich, info@jetsli.de
 */
public class PerformanceIntegrationTesting extends SimpleLuceneTestBase {

    Random rand;
    String exception;

    @Override
    public void setUp() {
        super.setUp();
        rand = new Random(1);
    }

    @Test public void testIndexing() {
        new PerfRunner(100000, 27f) {

            List<Vertex> previousVertices = new ArrayList<Vertex>();

            @Override public void reinit() {
                previousVertices.clear();
                super.reinit();
            }

            @Override public void innerRun(int trial, int i) {
                Vertex v1;
                Vertex v2;

                if (previousVertices.isEmpty() || rand.nextInt(10) < 5) {
                    v1 = g.addVertex(null);
                    vertices++;
                } else
                    v1 = previousVertices.get(rand.nextInt(previousVertices.size()));

                if (previousVertices.isEmpty() || rand.nextInt(10) < 5) {
                    v2 = g.addVertex(null);
                    vertices++;
                } else
                    v2 = previousVertices.get(rand.nextInt(previousVertices.size()));

                previousVertices.add(v1);
                previousVertices.add(v2);
                if (rand.nextInt(5000) < 10)
                    previousVertices.clear();

                g.addEdge(null, v1, v2, "e" + i);
                edges++;
            }

            @Override protected void finalAssert() {
                long vs1 = g.count(Vertex.class, RawLucene.TYPE, Vertex.class.getSimpleName());
                long es2 = g.count(Edge.class, RawLucene.TYPE, Edge.class.getSimpleName());

                logger.info("v:" + vs1 + " e:" + es2);
                // v:99838 e:100000
                assertEquals(vertices, vs1);
                assertEquals(edges, es2);
            }
        }.run();
    }

    @Test public void testFindByUserId() {
        new PerfRunner(300000, 8f) {

            StopWatch swPut;
            StopWatch swLong;
            StopWatch swStr;

            @Override public void reinit() {
                swPut = new StopWatch("put");
                swLong = new StopWatch("long");
                swStr = new StopWatch("str");
            }

            @Override public void innerRun(int trial, int ii) {
                String uId = "" + ii;
                Document doc = g.getRaw().createDocument(uId, ii, Vertex.class);
                swPut.start();
                g.getRaw().put(uId, ii, doc);
                swPut.stop();

                swLong.start();
                assertNotNull(g.getRaw().findById(ii));
                swLong.stop();
            }

            @Override
            protected void finalAssert() {
                for (int ii = 0; ii < items; ii++) {
                    String uId = "" + ii;
                    swStr.start();
                    assertNotNull(g.getRaw().findByUserId(uId));
                    swStr.stop();
                }
                logger.info(swLong + " " + swStr + " " + swPut);

            }
        }.run();
    }

    abstract class PerfRunner implements Runnable {

        private StopWatch sw = new StopWatch();
        protected int edges = 0;
        protected int vertices = 0;
        protected final int TRIALS = 3;
        protected final int items;
        protected final float expectedTime;

        PerfRunner(int items, float expectedTime) {
            this.expectedTime = expectedTime;
            this.items = items;
        }

        abstract void innerRun(int trial, int i);

        public void reinit() {
            reinitFileBasedGraph();
            rand = new Random(1);
            edges = 0;
            vertices = 0;
        }

        public void warmJvm() {
            logger.info("warming jvm");
            reinit();
            sw.start();
            for (int i = 0; i < items / 2; i++) {
                innerRun(-1, i);
            }
            g.getRaw().flush();
        }

        @Override public void run() {
            warmJvm();

            logger.info("starting benchmark " + sw.stop().getSeconds());
            float allSecs = 0;
            for (int trial = 0; trial < TRIALS; trial++) {
                reinit();

                sw = new StopWatch("perf" + trial).start();
                for (int i = 0; i < items; i++) {
                    innerRun(trial, i);
                }
                g.getRaw().flush();
                float indexingTime = sw.stop().getSeconds();
                sw = new StopWatch().start();
                finalAssert();
                logger.info("indexing:" + indexingTime + ", querying:" + sw.stop().getSeconds());
                allSecs += indexingTime;
                allSecs += sw.getSeconds();
            }
            float res = allSecs / TRIALS;
            logger.info("finished benchmark with " + res + " seconds");
            assertTrue("mean of benchmark should be less than " + expectedTime + " seconds but was " + res, res < expectedTime);
        }

        protected void finalAssert() {
        }
    }
}