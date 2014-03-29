package com.jnj.fluxgraph;

import com.tinkerpop.blueprints.*;
import com.tinkerpop.blueprints.impls.GraphTest;
import com.tinkerpop.blueprints.util.io.graphml.GraphMLReader;

import java.io.File;
import java.io.FileWriter;

/**
 * @author Davy Suvee (http://datablend.be)
 */
public class FluxBenchmarkTestSuite extends TestSuite {

    private static final int TOTAL_RUNS = 1;

    public FluxBenchmarkTestSuite() {
    }

    public FluxBenchmarkTestSuite(final GraphTest graphTest) {
        super(graphTest);
    }

    public void testDatomicGraph() throws Exception {
        double totalTime = 0.0d;
        int bufferSize = 1000;
        System.out.println("Testing benchmark with runs: " + TOTAL_RUNS);
        Graph graph = graphTest.generateGraph();
        try {
            new GraphMLReader(graph)
                    .inputGraph(
                            GraphMLReader.class.getResourceAsStream("graph-example-2.xml"),
                            bufferSize);
            graph.shutdown();
        } catch (Exception e) {
            File file = new File("logdump.txt");
            FileWriter fileWriter = new FileWriter(file);
            try {
                ((FluxGraph) graph).dumpPendingOps(fileWriter);
            } catch (Exception e2) {
                fileWriter.flush();
                fileWriter.close();
                throw e2;
            }
            throw e;
        }

        for (int i = 0; i < TOTAL_RUNS; i++) {
            //graph = graphTest.generateGraph();
            this.stopWatch();
            int counter = 0;
            CloseableIterable<Vertex> vv = (CloseableIterable<Vertex>) graph.getVertices();
            for (final Vertex vertex : vv) {
                counter++;
                CloseableIterable<Edge> ee = (CloseableIterable<Edge>) vertex.getEdges(Direction.OUT);
                for (final Edge edge : ee) {
                    counter++;
                    final Vertex vertex2 = edge.getVertex(Direction.IN);
                    counter++;
                    CloseableIterable<Edge> ee2 = (CloseableIterable<Edge>) vertex2.getEdges(Direction.OUT);
                    for (final Edge edge2 : ee2) {
                        counter++;
                        final Vertex vertex3 = edge2.getVertex(Direction.IN);
                        counter++;
                        CloseableIterable<Edge> ee3 = (CloseableIterable<Edge>) vertex3.getEdges(Direction.OUT);
                        for (final Edge edge3 : ee3) {
                            counter++;
                            edge3.getVertex(Direction.OUT);
                            counter++;
                        }
                        ee3.close();
                    }
                    ee2.close();
                }
                ee.close();
            }
            vv.close();
            double currentTime = this.stopWatch();
            totalTime = totalTime + currentTime;
            BaseTest.printPerformance(graph.toString(), counter, "FluxGraph elements touched (run=" + i + ")", currentTime);
            graph.shutdown();
        }
        BaseTest.printPerformance("FluxGraph", 1, "FluxGraph experiment average", totalTime / (double) TOTAL_RUNS);
    }
}