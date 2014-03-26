package com.jnj.fluxgraph;

import com.google.common.collect.Iterables;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * @author Mike Bryant (http://github.com/mikesname)
 */
public class FluxGraphSpecificTest {

    private FluxGraph graph;

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

}
