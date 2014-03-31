package com.jnj.fluxgraph;

import clojure.lang.Keyword;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import datomic.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.net.URL;
import java.util.*;

import static datomic.Connection.DB_AFTER;
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
    
    public Database getDb(List... statements) {
        return (Database)connection.db()
                .with(Lists.newArrayList(Iterables.concat(statements)))
                .get(DB_AFTER);
    }

    public Map loadDataFile(String name) throws Exception {
        URL resource = FluxHelper.class.getClassLoader().getResource(name);
        if (resource == null) {
            throw new RuntimeException("Unable to load resource: " + name);
        }
        return helper.loadFile(connection, new File(resource.toURI()));
    }

    public void loadTestData() throws Exception {
        helper.loadMetaModel(connection);
        loadDataFile(TEST_SCHEMA);
        loadDataFile(TEST_DATA);
    }

    @Before
    public void setUp() throws Exception {
        String uri = "datomic:mem://tinkerpop" + UUID.randomUUID();
        Peer.createDatabase(uri);
        connection = Peer.connect(uri);
        helper = new FluxHelper();
    }

    @After
    public void tearDown() throws Exception {

    }

    @Test
    public void testloadMetaModel() throws Exception {
        Map map = helper.loadMetaModel(connection);
        assertFalse(((List) map.get(TX_DATA)).isEmpty());
    }

    @Test
    public void testLoadTestDataWithSchema() throws Exception {
        helper.loadMetaModel(connection);
        Map map1 = loadDataFile(TEST_SCHEMA);
        assertFalse(((List)map1.get(TX_DATA)).isEmpty());
        Map map2 = loadDataFile(TEST_DATA);
        List txData = (List) map2.get(TX_DATA);
        assertFalse(txData.isEmpty());
    }

    @Test
    public void testLoadTestDataWithDynamicSchema() throws Exception {
        helper.loadMetaModel(connection);
        Map<String,Class> props = ImmutableMap.of("name", (Class)String.class);
        Map map1 = helper.installElementProperties(connection, props, Vertex.class);
        assertFalse(((List)map1.get(TX_DATA)).isEmpty());
        Map map2 = loadDataFile(TEST_DATA);
        List txData = (List) map2.get(TX_DATA);
        assertFalse(txData.isEmpty());
    }

    @Test
    public void testListVertices() throws Exception {
        loadTestData();
        ArrayList<List<Object>> verts = Lists.newArrayList(helper.listVertices(getDb()));
        assertEquals(2L, verts.size());
        List<Object> first = verts.get(0);
        assertEquals(2L, first.size());
        Object id = first.get(0);
        Entity entity = getDb().entity(id);
        Object name = entity.get(Keyword.intern("name.string.vertex"));
        assertEquals("Marko", name);
    }

    @Test
    public void testListEdges() throws Exception {
        loadTestData();
        ArrayList<List<Object>> edges = Lists.newArrayList(helper.listEdges(getDb()));
        assertEquals(1L, edges.size());
        List<Object> first = edges.get(0);
        assertEquals(3L, first.size());
        Object id = first.get(0);
        Entity entity = getDb().entity(id);
        Object label = entity.get(FluxHelper.EDGE_LABEL);
        assertEquals("knows", label);
    }

    @Test
    public void testIdFromUuid() throws Exception {
        loadTestData();
        Object marko = helper.idFromUuid(getDb(), MARKO_ID);
        Entity entity = getDb().entity(marko);
        Object uuid = entity.get(FluxHelper.ELEMENT_ID);
        assertEquals(MARKO_ID, uuid);
    }

    @Test(expected = NoSuchElementException.class)
    public void testIdFromUuidThrowsNSEE() throws Exception {
        loadTestData();
        UUID badId = UUID.fromString("550e8400-e29b-41d4-a716-000000000000");
        helper.idFromUuid(getDb(), badId);
    }

    @Test
    public void testEntityFromUuid() throws Exception {
        loadTestData();
        Entity marko = helper.entityFromUuid(getDb(), MARKO_ID);
        assertEquals("Marko", marko.get(Keyword.intern("name.string.vertex")));
    }

    @Test
    public void testGetOutVertex() throws Exception {
        loadTestData();
        Object marko = helper.idFromUuid(getDb(), MARKO_ID);
        List<Object> outVertex = helper.getOutVertex(getDb(), EDGE_ID);
        assertEquals(2L, outVertex.size());
        assertEquals(marko, outVertex.get(0));
    }

    @Test
    public void testGetInVertex() throws Exception {
        loadTestData();
        Object stephen = helper.idFromUuid(getDb(), STEPHEN_ID);
        List<Object> inVertex = helper.getInVertex(getDb(), EDGE_ID);
        assertEquals(2L, inVertex.size());
        assertEquals(stephen, inVertex.get(0));
    }

    @Test
    public void testGetInVertexInTx() throws Exception {
        loadTestData();
        Object stephen = helper.idFromUuid(getDb(), STEPHEN_ID);
        Object marko = helper.idFromUuid(getDb(), MARKO_ID);
        UUID newEdgeId = Peer.squuid();
        Addition addition = helper.addEdge(getDb(), newEdgeId, "knows", stephen, marko);
        Object outV = helper
                .getOutVertex(getDb(addition.statements), newEdgeId).get(0);
        assertEquals(stephen, outV);
        Object inV = helper
                .getInVertex(getDb(addition.statements), newEdgeId).get(0);
        assertEquals(marko, inV);
    }

    @Test
    public void testGetOutVertices() throws Exception {
        loadTestData();
        Object marko = helper.idFromUuid(getDb(), MARKO_ID);
        Iterable<List<Object>> knows = helper.getOutVertices(getDb(), marko, "knows");
        assertTrue(knows.iterator().hasNext());
        Object knowsId = knows.iterator().next().get(0);
        assertEquals("Stephen", helper.getProperty(getDb(), knowsId, Vertex.class, "name", String.class));
    }

    @Test
    public void testGetOutVerticesInTx() throws Exception {
        loadTestData();
        Object marko = helper.idFromUuid(getDb(), MARKO_ID);
        UUID newUuid = Peer.squuid();
        Addition newVertex = helper.addVertex(getDb(), newUuid);
        Addition newVertexProp = helper.addProperty(getDb(), newVertex.tempId,
                Vertex.class, "name", "Bob");
        Object bobNameAfterAdd = helper
                .getProperty(getDb(newVertex.statements, newVertexProp.statements),
                        newVertex.tempId, Vertex.class, "name",
                        String.class);
        System.out.println("Bob name after add: " + bobNameAfterAdd);

        Object bobNameAfterAddByUuid = helper
                .getPropertyByUuid(getDb(newVertex.statements, newVertexProp.statements),
                        newUuid, Vertex.class, "name",
                        String.class);
        System.out.println("Bob name after add by UUID: " + bobNameAfterAddByUuid);

        System.out.println("Bob ID: " + newUuid + " (int id) " + newVertex.tempId);
        UUID newEdgeUuid = Peer.squuid();
        Addition newVertexAddEdge = helper.addEdge(getDb(), newEdgeUuid, "knows", marko, newVertex.tempId);

        Database dbWithTx = getDb(newVertex.statements, newVertexProp.statements, newVertexAddEdge.statements);
        Object bobNameAfterAddEdge = helper
                .getProperty(dbWithTx,
                        newVertex.tempId,
                        Vertex.class, "name",
                        String.class);
        System.out.println("Bob name after add edge: " + bobNameAfterAddEdge);

        Iterable<List<Object>> knows = helper
                .getOutVertices(dbWithTx,
                        marko, "knows");
        ArrayList<List<Object>> knowsList = Lists.newArrayList(knows);
        System.out.println("Knows list: " + knowsList);
        assertEquals(2L, knowsList.size());
        for (List<Object> i : knowsList) {
            Object name = helper.getProperty(dbWithTx, i.get(0), Vertex.class, "name", String.class);
            System.out.println("Name: " + name + " (id :" + i.get(0) + ")");
        }
    }

    @Test
    public void testGetInVertices() throws Exception {
        loadTestData();
        Object stephen = helper.idFromUuid(getDb(), STEPHEN_ID);
        Iterable<List<Object>> knows = helper.getInVertices(getDb(), stephen, "knows");
        assertTrue(knows.iterator().hasNext());
        Object knowsId = knows.iterator().next().get(0);
        assertEquals("Marko", helper.getProperty(getDb(), knowsId, Vertex.class, "name", String.class));
    }

    @Test
    public void testGetBothVertices() throws Exception {
        loadTestData();
        Object stephen = helper.idFromUuid(getDb(), STEPHEN_ID);
        Iterable<List<Object>> knows = helper.getVertices(getDb(), stephen, Direction.BOTH, "knows");
        System.out.println(Lists.newArrayList(knows));
        for (List<Object> i : knows) {
            Object name = helper.getProperty(getDb(), i.get(0), Vertex.class, "name", String.class);
            System.out.println(name);
        }
    }

    @Test
    public void testGetBothVerticesWithSelfReference() throws Exception {
        loadTestData();
        Object stephen = helper.idFromUuid(getDb(), STEPHEN_ID);
        UUID edgeUuid = Peer.squuid();
        Addition edgeAddition = helper.addEdge(getDb(), edgeUuid, "knows", stephen, stephen);
        Database txDb = getDb(edgeAddition.statements);
        Iterable<List<Object>> knows = helper
                .getVertices(txDb, stephen, Direction.BOTH, "knows");
        assertEquals(3L, Iterables.size(knows));
    }

    @Test
    public void testGetBothVerticesWithSelfReferenceForAllLabels() throws Exception {
        loadTestData();
        Object stephen = helper.idFromUuid(getDb(), STEPHEN_ID);
        UUID edgeUuid = Peer.squuid();
        Addition edgeAddition = helper.addEdge(getDb(), edgeUuid, "likes", stephen, stephen);
        Iterable<List<Object>> knows = helper
                .getVertices(getDb(edgeAddition.statements), stephen, Direction.BOTH);
        assertEquals(3L, Iterables.size(knows));
    }

    @Test
    public void testGetBothVerticesWithDuplicates() throws Exception {
        loadTestData();
        Object stephen = helper.idFromUuid(getDb(), STEPHEN_ID);
        UUID edgeUuid1 = Peer.squuid();
        Addition edgeAddition1 = helper.addEdge(getDb(), edgeUuid1, "likes", stephen, stephen);
        UUID edgeUuid2 = Peer.squuid();
        Addition edgeAddition2 = helper.addEdge(getDb(), edgeUuid2, "knows", stephen, stephen);
        Database txDb = getDb(edgeAddition1.statements, edgeAddition2.statements);
        Iterable<List<Object>> knows = helper
                .getVertices(txDb, stephen, Direction.BOTH);
        List<List<Object>> listKnows = Lists.newArrayList(knows);
        for (List<Object> item : listKnows) {
            System.out.println(helper.getProperty(txDb, item.get(0),
                    Vertex.class, "name", String.class));
        }
        assertEquals(5L, listKnows.size());
    }

    @Test
    public void testGetEdges() throws Exception {
        loadTestData();
        Iterable<List<Object>> edges = helper
                .getEdges(getDb(), MARKO_ID, Direction.BOTH);
        List<List<Object>> listEdges = Lists.newArrayList(edges);
        assertEquals(1L, listEdges.size());
        assertEquals(helper.idFromUuid(getDb(), EDGE_ID), listEdges.get(0).get(0));

        Iterable<List<Object>> edges2 = helper
                .getEdges(getDb(), MARKO_ID, Direction.BOTH, "knows");
        List<List<Object>> listEdges2 = Lists.newArrayList(edges2);
        assertEquals(1L, listEdges2.size());
        assertEquals(helper.idFromUuid(getDb(), EDGE_ID), listEdges.get(0).get(0));

        Iterable<List<Object>> edges3 = helper
                .getEdges(getDb(), MARKO_ID, Direction.BOTH, "UNKNOWN");
        List<List<Object>> listEdges3 = Lists.newArrayList(edges3);
        assertEquals(0L, listEdges3.size());

    }

    @Test
    public void testGetProperty() throws Exception {
        loadTestData();
        Object property = helper
                .getPropertyByUuid(getDb(), MARKO_ID, Vertex.class, "name", String.class);
        assertEquals("Marko", property);
    }

    @Test
    public void testGetPropertyKeysByUuid() throws Exception {
        loadTestData();
        Set<String> propertyKeys = helper.getPropertyKeysByUuid(getDb(), MARKO_ID);
        assertEquals(1L, propertyKeys.size());
        assertEquals("name", propertyKeys.iterator().next());
    }

    @Test
    public void testGetPropertyKeysByUuidInTx() throws Exception {
        loadTestData();
        UUID newVertexId = Peer.squuid();
        Addition addition = helper.addVertex(getDb(), newVertexId);
        Addition addProp = helper.addProperty(getDb(), addition.tempId, Vertex.class, "name", "Bob");
        Database txDb = getDb(addition.statements, addProp.statements);
        Set<String> propertyKeys = helper
                .getPropertyKeysByUuid(txDb, newVertexId);
        assertEquals(1L, propertyKeys.size());
        assertEquals("name", propertyKeys.iterator().next());
    }

    @Test
    public void testAddProperty() throws Exception {
        loadTestData();
        Map<String,Class> props = ImmutableMap.of("age", (Class) Long.class);
        helper.installElementProperties(connection, props, Vertex.class);
        Addition addProp = helper.addPropertyByUuid(getDb(), MARKO_ID, Vertex.class, "age", 30);
        connection.transact(addProp.statements);
        Object property = helper
                .getPropertyByUuid(getDb(), MARKO_ID, Vertex.class, "age", Long.class);
        assertEquals(30, property);
    }

    @Test
    public void testShortGetProperty() throws Exception {
        loadTestData();
        Map<String,Class> props = ImmutableMap.of("age", (Class) Long.class);
        helper.installElementProperties(connection, props, Vertex.class);
        Addition addProp = helper.addPropertyByUuid(getDb(), MARKO_ID, Vertex.class, "age", 30);
        connection.transact(addProp.statements);
        Object property = helper
                .getPropertyByUuid(getDb(), MARKO_ID, "age");
        assertEquals(30, property);
    }

    @Test
    public void testAddPropertyInTransaction() throws Exception {
        loadTestData();
        Map<String,Class> props = ImmutableMap.of("age", (Class) Long.class);
        helper.installElementProperties(connection, props, Vertex.class);
        Addition addProp = helper.addPropertyByUuid(getDb(), MARKO_ID, Vertex.class, "age", 30);
        Object property = helper
                .getPropertyByUuid(getDb(addProp.statements), MARKO_ID, Vertex.class, "age", Long.class);
        assertEquals(30, property);
    }

    @Test
    public void testAddPropertyByUuidInTransaction() throws Exception {
        loadTestData();
        Map<String,Class> props = ImmutableMap.of("age", (Class) Long.class);
        helper.installElementProperties(connection, props, Vertex.class);
        Addition addProp = helper.addPropertyByUuid(getDb(), MARKO_ID, Vertex.class, "age", 30);
        //connection.transact(addProp.statements);
        Object property = helper
                .getPropertyByUuid(getDb(addProp.statements), MARKO_ID, Vertex.class, "age", Long.class);
        assertEquals(30, property);
    }

    @Test
    public void testAddPropertyToNewVertexInTransaction() throws Exception {
        loadTestData();
        Map<String,Class> props = ImmutableMap.of("age", (Class) Long.class);
        helper.installElementProperties(connection, props, Vertex.class);
        UUID newVertexId = Peer.squuid();
        Addition addition = helper.addVertex(getDb(), newVertexId);
        Addition addProp = helper.addProperty(getDb(addition.statements),
                addition.tempId, Vertex.class, "age", 30);
        //Map map = connection.transact(addProp.statements).get();
        Database txDb = getDb(addition.statements, addProp.statements);
        Object property = helper
                .getPropertyByUuid(txDb, newVertexId, "age");
        assertEquals(30, property);
    }

    @Test
    public void testAddAndRemovePropertyToNewVertexInTransaction() throws Exception {
        loadTestData();
        Map<String,Class> props = ImmutableMap.of("age", (Class) Long.class);
        helper.installElementProperties(connection, props, Vertex.class);
        UUID newVertexId = Peer.squuid();
        Addition addition = helper.addVertex(getDb(), newVertexId);
        Addition addProp = helper.addProperty(getDb(), addition.tempId, Vertex.class, "age", 30);
        //Map map = connection.transact(addProp.delStatements).get();
        Database txDb = getDb(addition.statements, addProp.statements);
        Object property = helper
                .getPropertyByUuid(txDb, newVertexId, Vertex.class, "age", Long.class);
        assertEquals(30, property);
    }

    @Test
    public void testAddPropertyOnEdge() throws Exception {
        loadTestData();
        Map<String,Class> props = ImmutableMap.of("date", (Class)Long.class);
        helper.installElementProperties(connection, props, Edge.class);
        Long testDate = new Date(0).getTime();
        Addition addProp = helper.addPropertyByUuid(getDb(), EDGE_ID, Edge.class, "date", testDate);
        connection.transact(addProp.statements);
        Object property = helper.getPropertyByUuid(getDb(), EDGE_ID, Edge.class, "date", Long.class);
        assertEquals(testDate, property);
    }

    @Test
    public void testRemoveProperty() throws Exception {
        loadTestData();
        testAddProperty();
        Object marko = helper.idFromUuid(getDb(), MARKO_ID);
        List statements = helper.removeProperty(getDb(), marko, Vertex.class, "age", Long.class);
        connection.transact(statements);
        Object property = helper
                .getProperty(getDb(), marko, Vertex.class, "age", Long.class);
        assertNull(property);
    }

    @Test
    public void testRemovePropertyInTx() throws Exception {
        loadTestData();
        testAddProperty();
        Object marko = helper.idFromUuid(getDb(), MARKO_ID);
        assertEquals(30, helper.getProperty(getDb(), marko, Vertex.class, "age", Long.class));
        List statements = helper.removePropertyByUuid(getDb(), MARKO_ID, Vertex.class, "age", Long.class);
        Object property = helper
                .getPropertyByUuid(getDb(statements), MARKO_ID, Vertex.class, "age", Long.class);
        assertNull(property);
    }

    @Test
    public void testGetPropertyKeys() throws Exception {
        loadTestData();
        Object marko = helper.idFromUuid(getDb(), MARKO_ID);
        Set<String> keys = helper.getPropertyKeys(getDb(), marko);
        assertEquals(Sets.newHashSet("name"), keys);
        testAddProperty();
        Set<String> keys2 = helper.getPropertyKeys(getDb(), marko);
        assertEquals(Sets.newHashSet("name", "age"), keys2);
        testRemoveProperty();
        Set<String> keys3 = helper.getPropertyKeys(getDb(), marko);
        assertEquals(Sets.newHashSet("name"), keys3);
    }

    @Test
    public void testAddVertex() throws Exception {
        loadTestData();
        UUID newUuid = Peer.squuid();
        Addition addition = helper.addVertex(getDb(), newUuid);
        connection.transact(addition.statements);
        Object bob = helper.idFromUuid(getDb(), newUuid);
        Addition addition2 = helper.addProperty(getDb(), bob, Vertex.class, "name", "Bob");
        connection.transact(addition2.statements);
        Set<String> keys = helper.getPropertyKeys(getDb(), bob);
        assertEquals(Sets.newHashSet("name"), keys);
    }

    @Test
    public void testAddVertexAndProperty() throws Exception {
        loadTestData();
        UUID newUuid = Peer.squuid();
        Addition addition = helper.addVertex(getDb(), newUuid);
        Addition addition2 = helper.addProperty(getDb(), addition.tempId, Vertex.class, "name", "Bob");
        connection.transact(addition.withStatements(addition2.statements).statements);
        Object bob = helper.idFromUuid(getDb(), newUuid);
        Set<String> keys = helper.getPropertyKeys(getDb(), bob);
        assertEquals(Sets.newHashSet("name"), keys);
    }
}
