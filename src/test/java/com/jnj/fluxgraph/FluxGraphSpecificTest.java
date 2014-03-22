package com.jnj.fluxgraph;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.UUID;

import static org.junit.Assert.assertEquals;

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
}
