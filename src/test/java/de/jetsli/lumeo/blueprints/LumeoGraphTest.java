package de.jetsli.lumeo.blueprints;

import com.tinkerpop.blueprints.pgm.AutomaticIndexTestSuite;
import com.tinkerpop.blueprints.pgm.Edge;
import com.tinkerpop.blueprints.pgm.EdgeTestSuite;
import com.tinkerpop.blueprints.pgm.Graph;
import com.tinkerpop.blueprints.pgm.GraphTestSuite;
import com.tinkerpop.blueprints.pgm.Index;
import com.tinkerpop.blueprints.pgm.IndexTestSuite;
import com.tinkerpop.blueprints.pgm.IndexableGraph;
import com.tinkerpop.blueprints.pgm.IndexableGraphTestSuite;
import com.tinkerpop.blueprints.pgm.TestSuite;
import com.tinkerpop.blueprints.pgm.TransactionalGraphTestSuite;
import com.tinkerpop.blueprints.pgm.Vertex;
import com.tinkerpop.blueprints.pgm.VertexTestSuite;
import com.tinkerpop.blueprints.pgm.impls.GraphTest;
import com.tinkerpop.blueprints.pgm.util.io.graphml.GraphMLReaderTestSuite;
//import com.tinkerpop.blueprints.pgm.util.graphml.GraphMLReaderTestSuite;

import de.jetsli.lumeo.LuceneGraph;
import java.lang.reflect.Method;
import java.util.Iterator;

/**
 * @author info@jetsli.de
 */
public class LumeoGraphTest extends GraphTest {

    public LumeoGraphTest() {
        this.allowsDuplicateEdges = false;
        this.allowsSelfLoops = false;
        this.isPersistent = true;
        this.isRDFModel = false;
        this.supportsVertexIteration = true;
        this.supportsEdgeIteration = true;
        this.supportsVertexIndex = true;
        this.supportsEdgeIndex = true;
        this.ignoresSuppliedIds = false;
        this.supportsTransactions = false;
    }

    public void testVertexTestSuite() throws Exception {
        this.stopWatch();
        doTestSuite(new VertexTestSuite(this));
        printTestPerformance("VertexTestSuite", this.stopWatch());
    }

    public void testEdgeTestSuite() throws Exception {
        this.stopWatch();
        doTestSuite(new EdgeTestSuite(this));
        printTestPerformance("EdgeTestSuite", this.stopWatch());
    }

    public void testGraphTestSuite() throws Exception {
        this.stopWatch();
        doTestSuite(new GraphTestSuite(this));
        printTestPerformance("GraphTestSuite", this.stopWatch());
    }

    public void testIndexableGraphTestSuite() throws Exception {
        this.stopWatch();
        doTestSuite(new IndexableGraphTestSuite(this));
        printTestPerformance("IndexableGraphTestSuite", this.stopWatch());
    }

    public void testIndexTestSuite() throws Exception {
        this.stopWatch();
        doTestSuite(new IndexTestSuite(this));
        printTestPerformance("IndexTestSuite", this.stopWatch());
    }

    public void testAutomaticIndexTestSuite() throws Exception {
        this.stopWatch();
        doTestSuite(new AutomaticIndexTestSuite(this));
        printTestPerformance("AutomaticIndexTestSuite", this.stopWatch());
    }

    public void testTransactionalGraphTestSuite() throws Exception {
        this.stopWatch();
        doTestSuite(new TransactionalGraphTestSuite(this));
        printTestPerformance("TransactionalGraphTestSuite", this.stopWatch());
    }

    public void testGraphMLReaderTestSuite() throws Exception {
        this.stopWatch();
        doTestSuite(new GraphMLReaderTestSuite(this));
        printTestPerformance("GraphMLReaderTestSuite", this.stopWatch());
    }

    public Graph getGraphInstance() {
        return new LuceneGraph();
    }

    public void doTestSuite(final TestSuite testSuite) throws Exception {
        String doTest = System.getProperty("testLuceneGraph");
        if (doTest == null || doTest.equals("true")) {
            for (Method method : testSuite.getClass().getDeclaredMethods()) {
                if (method.getName().startsWith("test")) {
                    System.out.println("Testing " + method.getName() + "...");
                    method.invoke(testSuite);
                }
            }
        }
    }

    public void testLongIdConversions() {
        String id1 = "100";  // good  100
        String id2 = "100.0"; // good 100
        String id3 = "100.1"; // good 100
        String id4 = "one"; // bad

        try {
            Double.valueOf(id1).longValue();
            assertTrue(true);
        } catch (NumberFormatException e) {
            assertFalse(true);
        }
        try {
            Double.valueOf(id2).longValue();
            assertTrue(true);
        } catch (NumberFormatException e) {
            assertFalse(true);
        }
        try {
            Double.valueOf(id3).longValue();
            assertTrue(true);
        } catch (NumberFormatException e) {
            assertFalse(true);
        }
        try {
            Double.valueOf(id4).longValue();
            assertTrue(false);
        } catch (NumberFormatException e) {
            assertFalse(false);
        }
    }

    public void testQueryIndex() throws Exception {
        IndexableGraph graph = (IndexableGraph) getGraphInstance();
        Vertex a = graph.addVertex(null);
        a.setProperty("name", "marko");
        Iterator itty = graph.getIndex(Index.VERTICES, Vertex.class).get("name", "*rko").iterator();
        int counter = 0;
        while (itty.hasNext()) {
            counter++;
            assertEquals(itty.next(), a);
        }
        assertEquals(counter, 1);

        Vertex b = graph.addVertex(null);
        Edge edge = graph.addEdge(null, a, b, "knows");
        edge.setProperty("weight", 0.75);
        itty = graph.getIndex(Index.EDGES, Edge.class).get("label", "k?ows").iterator();
        counter = 0;
        while (itty.hasNext()) {
            counter++;
            assertEquals(itty.next(), edge);
        }
        assertEquals(counter, 1);
        itty = graph.getIndex(Index.EDGES, Edge.class).get("weight", "[0.5 TO 1.0]").iterator();
        counter = 0;
        while (itty.hasNext()) {
            counter++;
            assertEquals(itty.next(), edge);
        }
        assertEquals(counter, 1);
        assertEquals(count(graph.getIndex(Index.EDGES, Edge.class).get("weight", "[0.1 TO 0.5]")), 0);

        graph.shutdown();
    }
}
