package de.jetsli.lumeo.blueprints;

import java.io.FileInputStream;

import com.tinkerpop.blueprints.BaseTest;
import com.tinkerpop.blueprints.pgm.Edge;
import com.tinkerpop.blueprints.pgm.Graph;
import com.tinkerpop.blueprints.pgm.TestSuite;
import com.tinkerpop.blueprints.pgm.Vertex;
import com.tinkerpop.blueprints.pgm.util.io.graphml.GraphMLReader;
//import com.tinkerpop.blueprints.pgm.util.graphml.GraphMLReader;
import org.junit.Before;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class LumeoBenchmarkTestSuite extends TestSuite {

    private static final int TOTAL_RUNS = 10;

    @Before
    @Override public void setUp() {
        graphTest = new LumeoGraphTest();
    }

    public void testLuceneGraph() throws Exception {
        double totalTime = 0.0d;
        Graph graph = graphTest.getGraphInstance();
//        GraphMLReader.inputGraph(graph, GraphMLReader.class.getResourceAsStream("graph-example-2.xml"));
        GraphMLReader.inputGraph(graph, new FileInputStream("data/foo.graphml"));
        graph.shutdown();

        for (int i = 0; i < TOTAL_RUNS; i++) {
            graph = graphTest.getGraphInstance();
            this.stopWatch();
            int counter = 0;
            for (final Vertex vertex : graph.getVertices()) {
                counter++;
                for (final Edge edge : vertex.getOutEdges()) {
                    counter++;
                    final Vertex vertex2 = edge.getInVertex();
                    counter++;
                    for (final Edge edge2 : vertex2.getOutEdges()) {
                        counter++;
                        final Vertex vertex3 = edge2.getInVertex();
                        counter++;
                        for (final Edge edge3 : vertex3.getOutEdges()) {
                            counter++;
                            edge3.getOutVertex();
                            counter++;
                        }
                    }
                }
            }
            double currentTime = this.stopWatch();
            totalTime = totalTime + currentTime;
            BaseTest.printPerformance(graph.toString(), counter, "LuceneGraph elements touched", currentTime);
            graph.shutdown();
        }
        BaseTest.printPerformance("LuceneGraph", 1, "LuceneGraph experiment average", totalTime / (double) TOTAL_RUNS);
    }
}
