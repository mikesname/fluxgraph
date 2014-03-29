package com.jnj.fluxgraph;

import com.google.common.collect.Iterables;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import static com.tinkerpop.blueprints.impls.GraphTest.count;
import static com.tinkerpop.blueprints.impls.GraphTest.printPerformance;


/**
 * @author Mike Bryant (http://github.com/mikesname)
 */
public class FluxGraphSpecificTest {

    private FluxGraph graph;

    double timer = -1.0d;

    public double stopWatch() {
        if (this.timer == -1.0d) {
            this.timer = System.nanoTime() / 1000000.0d;
            return -1.0d;
        } else {
            double temp = (System.nanoTime() / 1000000.0d) - this.timer;
            this.timer = -1.0d;
            return temp;
        }
    }

    @Before
    public void setUp() throws Exception {
        graph = new FluxGraph("datomic:mem://tinkerpop" + UUID.randomUUID());
    }

    @After
    public void tearDown() throws Exception {
        graph.shutdown();
    }

    @Test
    public void testAddVertex() throws Exception {
        Vertex v = graph.addVertex(null);
        v.setProperty("foo", "bar");
        graph.commit();

        Vertex v2 = graph.getVertex(v.getId());
        assertEquals(v.getId(), v2.getId());
        assertEquals(v.getProperty("foo"), v2.getProperty("foo"));
    }

    @Test
    public void testAddEdge() throws Exception {
        Vertex v1 = graph.addVertex(null);
        Vertex v2 = graph.addVertex(null);
        Edge edge = v1.addEdge("test", v2);

        assertEquals(v1.getId(), edge.getVertex(Direction.OUT).getId());
        assertEquals(v2.getId(), edge.getVertex(Direction.IN).getId());
    }

    @Test
    public void testAddProperties() throws Exception {
        Vertex v1 = graph.addVertex(null);
        v1.setProperty("foo", "bar");

        assertEquals("bar", v1.getProperty("foo"));
    }

    @Test
    public void testAddVerticesInMultipleTransactions() throws Exception {
        Vertex v1 = graph.addVertex(null);
        v1.setProperty("foo", "bar");
        graph.commit();
        Vertex v2 = graph.addVertex(null);
        v2.setProperty("foo", "bar");
        graph.commit();
        Vertex v3 = graph.addVertex(null);
        v3.setProperty("foo", "bar");
        graph.commit();
        v1.addEdge("knows", v2);
        v2.addEdge("knows", v3);
        graph.commit();

        assertEquals(3L, Iterables.size(graph.getVertices()));
        Vertex v4 = graph.addVertex(null);
        assertEquals(4L, Iterables.size(graph.getVertices()));
        graph.rollback();
        assertEquals(3L, Iterables.size(graph.getVertices()));
        assertNull(graph.getVertex(v4.getId()));

        assertEquals(v3, v1
                .getVertices(Direction.OUT, "knows").iterator().next()
                .getVertices(Direction.OUT, "knows").iterator().next());
    }

    private int treeBranchSize = 11;

    @Test
    public void testTreeConnectivity() {
        this.stopWatch();
        Vertex start = graph.addVertex(null);
        setupTree(treeBranchSize, start);

        graph.commit();
        testTreeIteration(treeBranchSize, start, "out-tx");
    }

    @Test
    public void testTreeConnectivityInTransaction() {
        this.stopWatch();
        Vertex start = graph.addVertex(null);
        setupTree(treeBranchSize, start);
        testTreeIteration(treeBranchSize, start, "in-tx");
    }

    private void testTreeIteration(int branchSize, Vertex start, String label) {
        assertEquals(0, count(start.getEdges(Direction.IN)));
        assertEquals(branchSize, count(start.getEdges(Direction.OUT)));
        for (Edge e : start.getEdges(Direction.OUT)) {
            assertEquals("test1", e.getLabel());
            assertEquals(branchSize, count(e.getVertex(Direction.IN).getEdges(Direction.OUT)));
            assertEquals(1, count(e.getVertex(Direction.IN).getEdges(Direction.IN)));
            for (Edge f : e.getVertex(Direction.IN).getEdges(Direction.OUT)) {
                assertEquals("test2", f.getLabel());
                assertEquals(branchSize, count(f.getVertex(Direction.IN).getEdges(Direction.OUT)));
                assertEquals(1, count(f.getVertex(Direction.IN).getEdges(Direction.IN)));
                for (Edge g : f.getVertex(Direction.IN).getEdges(Direction.OUT)) {
                    assertEquals("test3", g.getLabel());
                    assertEquals(0, count(g.getVertex(Direction.IN).getEdges(Direction.OUT)));
                    assertEquals(1, count(g.getVertex(Direction.IN).getEdges(Direction.IN)));
                }
            }
        }

        int totalVertices = 0;
        for (int i = 0; i < 4; i++) {
            totalVertices = totalVertices + (int) Math.pow(branchSize, i);
        }
        printPerformance(graph.toString(), totalVertices, "vertices added in a tree structure [" + label + "]",
                this.stopWatch());

        if (graph.getFeatures().supportsVertexIteration) {
            this.stopWatch();
            Set<Vertex> vertices = new HashSet<Vertex>();
            for (Vertex v : graph.getVertices()) {
                vertices.add(v);
            }
            assertEquals(totalVertices, vertices.size());
            printPerformance(graph.toString(), totalVertices, "vertices iterated [" + label + "]", this.stopWatch());
        }

        if (graph.getFeatures().supportsEdgeIteration) {
            this.stopWatch();
            Set<Edge> edges = new HashSet<Edge>();
            for (Edge e : graph.getEdges()) {
                edges.add(e);
            }
            assertEquals(totalVertices - 1, edges.size());
            printPerformance(graph.toString(), totalVertices - 1, "edges iterated [" + label + "]", this.stopWatch());
        }
    }

    private void setupTree(int branchSize, Vertex start) {
        for (int i = 0; i < branchSize; i++) {
            Vertex a = graph.addVertex(null);
            graph.addEdge(null, start, a, "test1");
            for (int j = 0; j < branchSize; j++) {
                Vertex b = graph.addVertex(null);
                graph.addEdge(null, a, b, "test2");
                for (int k = 0; k < branchSize; k++) {
                    Vertex c = graph.addVertex(null);
                    graph.addEdge(null, b, c, "test3");
                }
            }
        }
    }


}
