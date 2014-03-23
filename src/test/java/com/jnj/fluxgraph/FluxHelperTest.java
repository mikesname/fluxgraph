package com.jnj.fluxgraph;

import clojure.lang.Keyword;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import datomic.Connection;
import datomic.Entity;
import datomic.Peer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.net.URL;
import java.util.*;

import static datomic.Connection.TX_DATA;
import static org.junit.Assert.*;

import static com.jnj.fluxgraph.FluxHelper.Addition;

/**
 * @author Mike Bryant (http://github.com/mikesname)
 */
public class FluxHelperTest {

    public static String TEST_SCHEMA = "property-schema.edn";
    public static String TEST_DATA = "graph.edn";

    private Connection connection;
    private FluxHelper helper;

    // Data from the fixtures...
    public static final UUID MARKO_ID = UUID.fromString("550e8400-e29b-41d4-a716-446655440000");
    public static final UUID STEPHEN_ID = UUID.fromString("550e8400-e29b-41d4-a716-446655440001");
    public static final UUID EDGE_ID = UUID.fromString("550e8400-e29b-41d4-a716-446655440002");

    public Map loadDataFile(String name) throws Exception {
        URL resource = FluxHelper.class.getClassLoader().getResource(name);
        if (resource == null) {
            throw new RuntimeException("Unable to load resource: " + name);
        }
        return helper.loadFile(new File(resource.toURI()));
    }

    public void loadTestData() throws Exception {
        helper.loadMetaModel();
        loadDataFile(TEST_SCHEMA);
        loadDataFile(TEST_DATA);
    }

    @Before
    public void setUp() throws Exception {
        String uri = "datomic:mem://tinkerpop" + UUID.randomUUID();
        Peer.createDatabase(uri);
        connection = Peer.connect(uri);
        helper = new FluxHelper(connection);
    }

    @After
    public void tearDown() throws Exception {

    }

    @Test
    public void testLoadMetaModel() throws Exception {
        Map map = helper.loadMetaModel();
        assertFalse(((List) map.get(TX_DATA)).isEmpty());
    }

    @Test
    public void testLoadTestDataWithSchema() throws Exception {
        helper.loadMetaModel();
        Map map1 = loadDataFile(TEST_SCHEMA);
        assertFalse(((List)map1.get(TX_DATA)).isEmpty());
        Map map2 = loadDataFile(TEST_DATA);
        List txData = (List) map2.get(TX_DATA);
        assertFalse(txData.isEmpty());
    }

    @Test
    public void testLoadTestDataWithDynamicSchema() throws Exception {
        helper.loadMetaModel();
        Map<String,Class> props = ImmutableMap.of("name", (Class)String.class);
        Map map1 = helper.installElementProperties(props, Vertex.class);
        assertFalse(((List)map1.get(TX_DATA)).isEmpty());
        Map map2 = loadDataFile(TEST_DATA);
        List txData = (List) map2.get(TX_DATA);
        assertFalse(txData.isEmpty());
    }

    @Test
    public void testListVertices() throws Exception {
        loadTestData();
        ArrayList<List<Object>> verts = Lists.newArrayList(helper.listVertices());
        assertEquals(2L, verts.size());
        List<Object> first = verts.get(0);
        assertEquals(2L, first.size());
        Object id = first.get(0);
        Entity entity = connection.db().entity(id);
        Object name = entity.get(Keyword.intern("name.string.vertex"));
        assertEquals("Marko", name);
    }

    @Test
    public void testListEdges() throws Exception {
        loadTestData();
        ArrayList<List<Object>> edges = Lists.newArrayList(helper.listEdges());
        assertEquals(1L, edges.size());
        List<Object> first = edges.get(0);
        assertEquals(2L, first.size());
        Object id = first.get(0);
        Entity entity = connection.db().entity(id);
        Object label = entity.get(FluxHelper.EDGE_LABEL);
        assertEquals("knows", label);
    }

    @Test
    public void testIdFromUuid() throws Exception {
        loadTestData();
        Object marko = helper.idFromUuid(MARKO_ID);
        Entity entity = connection.db().entity(marko);
        Object uuid = entity.get(FluxHelper.ELEMENT_ID);
        assertEquals(MARKO_ID, uuid);
    }

    @Test(expected = NoSuchElementException.class)
    public void testIdFromUuidThrowsNSEE() throws Exception {
        loadTestData();
        UUID badId = UUID.fromString("550e8400-e29b-41d4-a716-000000000000");
        helper.idFromUuid(badId);
    }

    @Test
    public void testEntityFromUuid() throws Exception {
        loadTestData();
        Entity marko = helper.entityFromUuid(MARKO_ID);
        assertEquals("Marko", marko.get(Keyword.intern("name.string.vertex")));
    }

    @Test
    public void testGetOutVertex() throws Exception {
        loadTestData();
        Object marko = helper.idFromUuid(MARKO_ID);
        Object edge = helper.idFromUuid(EDGE_ID);
        List<Object> outVertex = helper.getOutVertex(edge);
        assertEquals(2L, outVertex.size());
        assertEquals(marko, outVertex.get(0));
    }

    @Test
    public void testGetInVertex() throws Exception {
        loadTestData();
        Object stephen = helper.idFromUuid(STEPHEN_ID);
        Object edge = helper.idFromUuid(EDGE_ID);
        List<Object> inVertex = helper.getInVertex(edge);
        assertEquals(2L, inVertex.size());
        assertEquals(stephen, inVertex.get(0));
    }

    @Test
    public void testGetOutVertices() throws Exception {
        loadTestData();
        Object marko = helper.idFromUuid(MARKO_ID);
        Iterable<List<Object>> knows = helper.getOutVertices(marko, "knows");
        assertTrue(knows.iterator().hasNext());
        Object knowsId = knows.iterator().next().get(0);
        assertEquals("Stephen", helper.getProperty(knowsId, Vertex.class, "name", String.class));
    }

    @Test
    public void testGetOutVerticesInTx() throws Exception {
        loadTestData();
        Object marko = helper.idFromUuid(MARKO_ID);
        UUID newUuid = Peer.squuid();
        Addition newVertex = helper.addVertex(newUuid);
        Addition newVertexProp = helper.addProperty(newVertex.tempId,
                Vertex.class, "name", "Bob");
        UUID newEdgeUuid = Peer.squuid();
        Addition newVertexAddEdge = helper.addEdge(newEdgeUuid, "knows", marko, newVertex.tempId);

        FluxHelper txHelper = helper
                .addStatements(newVertex.statements)
                .addStatements(newVertexProp.statements)
                .addStatements(newVertexAddEdge.statements);
        Iterable<List<Object>> knows = txHelper
                .getOutVertices(marko, "knows");
        ArrayList<List<Object>> knowsList = Lists.newArrayList(knows);
        assertEquals(2L, knowsList.size());
        for (List<Object> i : knowsList) {
            for (Object n : i) {
                Object name = txHelper.getProperty(n, Vertex.class, "name", String.class);
                System.out.println(name);
            }
        }
    }

    @Test
    public void testGetInVertices() throws Exception {
        loadTestData();
        Object stephen = helper.idFromUuid(STEPHEN_ID);
        Iterable<List<Object>> knows = helper.getInVertices(stephen, "knows");
        assertTrue(knows.iterator().hasNext());
        Object knowsId = knows.iterator().next().get(0);
        assertEquals("Marko", helper.getProperty(knowsId, Vertex.class, "name", String.class));
    }

    @Test
    public void testGetBothVertices() throws Exception {
        loadTestData();
        Object stephen = helper.idFromUuid(STEPHEN_ID);
        Iterable<List<Object>> knows = helper.getVertices(stephen, Direction.BOTH, "knows");
        System.out.println(Lists.newArrayList(knows));
        for (List<Object> i : knows) {
            for (Object n : i) {
                Object name = helper.getProperty(n, Vertex.class, "name", String.class);
                System.out.println(name);
            }
        }
    }

    @Test
    public void testGetBothVerticesWithSelfReference() throws Exception {
        loadTestData();
        Object stephen = helper.idFromUuid(STEPHEN_ID);
        UUID edgeUuid = Peer.squuid();
        Addition edgeAddition = helper.addEdge(edgeUuid, "knows", stephen, stephen);
        FluxHelper txHelper = helper
                .addStatements(edgeAddition.statements);
        Iterable<List<Object>> knows = txHelper
                .getVertices(stephen, Direction.BOTH, "knows");
        assertEquals(2L, Iterables.size(knows));
    }

    @Test
    public void testGetBothVerticesWithSelfReferenceForAllLabels() throws Exception {
        loadTestData();
        Object stephen = helper.idFromUuid(STEPHEN_ID);
        UUID edgeUuid = Peer.squuid();
        Addition edgeAddition = helper.addEdge(edgeUuid, "likes", stephen, stephen);
        FluxHelper txHelper = helper
                .addStatements(edgeAddition.statements);
        Iterable<List<Object>> knows = txHelper
                .getVertices(stephen, Direction.BOTH);
        assertEquals(2L, Iterables.size(knows));
    }

    @Test
    public void testGetBothVerticesWithDuplicates() throws Exception {
        loadTestData();
        Object stephen = helper.idFromUuid(STEPHEN_ID);
        UUID edgeUuid1 = Peer.squuid();
        Addition edgeAddition1 = helper.addEdge(edgeUuid1, "likes", stephen, stephen);
        UUID edgeUuid2 = Peer.squuid();
        Addition edgeAddition2 = helper.addEdge(edgeUuid2, "knows", stephen, stephen);
        FluxHelper txHelper = helper
                .addStatements(edgeAddition1.statements)
                .addStatements(edgeAddition2.statements);
        Iterable<List<Object>> knows = txHelper
                .getVertices(stephen, Direction.BOTH);
        List<List<Object>> listKnows = Lists.newArrayList(knows);
        for (List<Object> item : listKnows) {
            System.out.println(txHelper.getProperty(item.get(0),
                    Vertex.class, "name", String.class));
        }
        assertEquals(3L, listKnows.size());
    }

    @Test
    public void testGetEdges() throws Exception {
        loadTestData();
        Object marko = helper.idFromUuid(MARKO_ID);
        Iterable<List<Object>> edges = helper
                .getEdges(marko, Direction.BOTH);
        List<List<Object>> listEdges = Lists.newArrayList(edges);
        assertEquals(1L, listEdges.size());
        assertEquals(helper.idFromUuid(EDGE_ID), listEdges.get(0).get(0));

        Iterable<List<Object>> edges2 = helper
                .getEdges(marko, Direction.BOTH, "knows");
        List<List<Object>> listEdges2 = Lists.newArrayList(edges2);
        assertEquals(1L, listEdges2.size());
        assertEquals(helper.idFromUuid(EDGE_ID), listEdges.get(0).get(0));

        Iterable<List<Object>> edges3 = helper
                .getEdges(marko, Direction.BOTH, "UNKNOWN");
        List<List<Object>> listEdges3 = Lists.newArrayList(edges3);
        assertEquals(0L, listEdges3.size());

    }

    @Test
    public void testGetProperty() throws Exception {
        loadTestData();
        Object marko = helper.idFromUuid(MARKO_ID);
        Object property = helper.getProperty(marko, Vertex.class, "name", String.class);
        assertEquals("Marko", property);
    }

    @Test
    public void testAddProperty() throws Exception {
        loadTestData();
        Map<String,Class> props = ImmutableMap.of("age", (Class) Long.class);
        helper.installElementProperties(props, Vertex.class);
        Object marko = helper.idFromUuid(MARKO_ID);
        Addition addProp = helper.addProperty(marko, Vertex.class, "age", 30);
        connection.transact(addProp.statements);
        Object property = helper.getProperty(marko, Vertex.class, "age", Long.class);
        assertEquals(30, property);
    }

    @Test
    public void testAddPropertyOnEdge() throws Exception {
        loadTestData();
        Map<String,Class> props = ImmutableMap.of("date", (Class)Long.class);
        helper.installElementProperties(props, Edge.class);
        Object edge = helper.idFromUuid(EDGE_ID);
        Long testDate = new Date(0).getTime();
        Addition addProp = helper.addProperty(edge, Edge.class, "date", testDate);
        connection.transact(addProp.statements);
        Object property = helper.getProperty(edge, Edge.class, "date", Long.class);
        assertEquals(testDate, property);
    }

    @Test
    public void testRemoveProperty() throws Exception {
        loadTestData();
        testAddProperty();
        Object marko = helper.idFromUuid(MARKO_ID);
        List statements = helper.removeProperty(marko, Vertex.class, "age", Long.class);
        connection.transact(statements);
        Object property = helper.getProperty(marko, Vertex.class, "age", Long.class);
        assertNull(property);
    }

    @Test
    public void testGetPropertyKeys() throws Exception {
        loadTestData();
        Object marko = helper.idFromUuid(MARKO_ID);
        Set<String> keys = helper.getPropertyKeys(marko);
        assertEquals(Sets.newHashSet("name"), keys);
        testAddProperty();
        Set<String> keys2 = helper.getPropertyKeys(marko);
        assertEquals(Sets.newHashSet("name", "age"), keys2);
        testRemoveProperty();
        Set<String> keys3 = helper.getPropertyKeys(marko);
        assertEquals(Sets.newHashSet("name"), keys3);
    }

    @Test
    public void testAddVertex() throws Exception {
        loadTestData();
        UUID newUuid = Peer.squuid();
        Addition addition = helper.addVertex(newUuid);
        connection.transact(addition.statements);
        Object bob = helper.idFromUuid(newUuid);
        Addition addition2 = helper.addProperty(bob, Vertex.class, "name", "Bob");
        connection.transact(addition2.statements);
        Set<String> keys = helper.getPropertyKeys(bob);
        assertEquals(Sets.newHashSet("name"), keys);
    }

    @Test
    public void testAddVertexAndProperty() throws Exception {
        loadTestData();
        UUID newUuid = Peer.squuid();
        Addition addition = helper.addVertex(newUuid);
        Addition addition2 = helper.addProperty(addition.tempId, Vertex.class, "name", "Bob");
        connection.transact(addition.withStatements(addition2.statements).statements);
        Object bob = helper.idFromUuid(newUuid);
        Set<String> keys = helper.getPropertyKeys(bob);
        assertEquals(Sets.newHashSet("name"), keys);
    }
}
