package com.jnj.fluxgraph;

import clojure.lang.Keyword;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.tinkerpop.blueprints.Direction;
import datomic.*;

import static datomic.Connection.DB_AFTER;

import java.io.*;
import java.net.URL;
import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * @author Mike Bryant (http://github.com/mikesname)
 */
public final class FluxHelper {

    public static final Keyword VERTEX_TYPE = Keyword.intern("graph.element.type/vertex");
    public static final Keyword EDGE_TYPE = Keyword.intern("graph.element.type/edge");
    public static final Keyword EDGE_LABEL = Keyword.intern("graph.edge/label");
    public static final Keyword ELEMENT_ID = Keyword.intern("graph.element/id");
    public static final Keyword IN_VERTEX = Keyword.intern("graph.edge/inVertex");
    public static final Keyword OUT_VERTEX = Keyword.intern("graph.edge/outVertex");
    public static final Keyword IN_DIRECTION = Keyword.intern("in");
    public static final Keyword OUT_DIRECTION = Keyword.intern("out");

    public static class Addition {
        public Object tempId;
        public List statements;

        public Addition(Object tempId, List statements) {
            this.tempId = tempId;
            this.statements = statements;
        }

        public Addition withStatements(List statements) {
            return new Addition(tempId, Lists.newArrayList(
                    Iterables.concat(this.statements, statements)));
        }
    }

    private final Connection connection;

    private final List<Object> statements;

    /**
     * Constructor.
     *
     * @param connection The DB connection
     * @param statements A set of additional statements, such as uncommitted facts
     */
    public FluxHelper(Connection connection, List<Object> statements) {
        this.connection = connection;
        this.statements = statements;
    }

    /**
     * Constructor.
     *
     * @param connection The DB connection.
     */
    public FluxHelper(Connection connection) {
        this(connection, Lists.newArrayList());
    }

    /**
     * Obtain a new helper with the given additional statements.
     *
     * @param statements A set of additional statements, such as uncommitted facts
     * @return A new helper
     */
    public FluxHelper addStatements(List statements) {
        List<Object> tmp = Lists.newArrayList();
        tmp.addAll(this.statements);
        tmp.addAll(statements);
        return new FluxHelper(connection, tmp);
    }

    private Database getDatabase() {
        return (Database)connection.db().with(statements).get(DB_AFTER);
    }

    private Database getDatabase(List statements) {
        return (Database)connection.db().with(statements).get(DB_AFTER);
    }

    /**
     * Load the graph's meta model, specified in the graph.schema.edn resource.
     *
     * @return The connection's transaction data
     * @throws Exception
     */
    public Map loadMetaModel() throws Exception {

        URL resource = FluxHelper.class.getClassLoader().getResource("graph-schema.edn");
        if (resource == null) {
            throw new RuntimeException("Unable to load graph-schema.edn resource");
        }
        return loadFile(new File(resource.toURI()));
    }

    /**
     * Load a file containing a set of datoms into the database.
     *
     * @param file A file object
     * @return The connection's transaction data
     * @throws IOException
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public Map loadFile(File file) throws IOException, ExecutionException, InterruptedException {
        FileReader reader = new FileReader(file);
        try {
            List statements = (List) Util.readAll(reader).get(0);
            return connection.transact(statements).get();
        } finally {
            reader.close();
        }
    }

    /**
     * Install a set of property definitions in the graph
     *
     * @param properties  A mapping from property name to the a Java
     *                    class representing the property's type
     * @param elementType A class assignable to either Vertex or Edge
     * @return The connection's transaction data
     */
    public Map installElementProperties(Map<String, Class> properties,
            Class<?> elementType) throws ExecutionException, InterruptedException {
        ArrayList<Object> statements = Lists.newArrayList();
        for (Map.Entry<String, Class> prop : properties.entrySet()) {
            statements.add(Util.map(":db/id", Peer.tempid(":db.part/db"),
                    ":db/ident", FluxUtil.createKey(prop.getKey(), prop.getValue(), elementType),
                    ":db/valueType", FluxUtil.mapJavaTypeToDatomicType(prop.getValue()),
                    ":db/cardinality", ":db.cardinality/one",
                    ":db.install/_attribute", ":db.part/db"));
        }
        return connection.transact(statements).get();
    }

    /**
     * Obtain an iterable of vertex data, comprising a pair of the internal
     * graph ID and the element's UUID.
     *
     * @return An iterable of ID-UUID pairs
     */
    public Iterable<List<Object>> listVertices() {
        return Peer.q("[:find ?v ?uuid :in $ :where " +
                "[?v :graph.element/type :graph.element.type/vertex] " +
                "[?v :graph.element/id ?uuid]]",
                getDatabase());
    }

    public Iterable<List<Object>> listVertices(Keyword key, Object value) {
        return Peer.q("[:find ?v ?uuid :in $ ?key ?val :where " +
                "[?v :graph.element/type :graph.element.type/vertex] " +
                "[?v :graph.element/id ?uuid]" +
                "[?v ?key ?val]]",
                getDatabase(), key, value);
    }

    public List<Object> getVertex(UUID id) {
        return Peer.q("[:find ?v ?uuid :in $ $uuid :where " +
                "[?v :graph.element/type :graph.element.type/vertex] " +
                "[?v :graph.element/id ?uuid]]",
                getDatabase(), id).iterator().next();
    }

    public List<Object> getEdge(UUID id) {
        return Peer.q("[:find ?v ?uuid ?label :in $ $uuid :where " +
                "[?v :graph.element/type :graph.element.type/edge] " +
                "[?v :graph.element/id ?uuid] " +
                "[?v :graph.edge/label ?label]]",
                getDatabase(), id).iterator().next();
    }

    public Iterable<List<Object>> listEdges() {
        return Peer.q("[:find ?v ?uuid ?label :in $ :where " +
                "[?v :graph.element/type :graph.element.type/edge] " +
                "[?v :graph.element/id ?uuid] " +
                "[?v :graph.edge/label ?label]]",
                getDatabase());
    }

    public Iterable<List<Object>> listEdges(Keyword key, Object value) {
        return Peer.q("[:find ?v ?uuid ?label :in $ ?key ?val :where " +
                "[?v :graph.element/type :graph.element.type/edge] " +
                "[?v :graph.element/id ?uuid] " +
                "[?v :graph.edge/label ?label ]" +
                "[?v ?key ?val]]",
                getDatabase(), key, value);
    }

    /**
     * Fetch the internal ID for an element given its UUID.
     *
     * @param uuid The external UUID of the element
     * @return The entity's interal ID
     * @throws NoSuchElementException
     */
    public Object idFromUuid(UUID uuid) throws NoSuchElementException {
        List<Object> obj = Peer.q("[:find ?v :in $ ?uuid :where [?v :graph.element/id ?uuid]]",
                getDatabase(), uuid).iterator().next();
        return obj.get(0);
    }

    /**
     * Fetch an entity given its UUID.
     *
     * @param uuid The external UUID of the element
     * @return The entity
     * @throws NoSuchElementException
     */
    public Entity entityFromUuid(UUID uuid) throws NoSuchElementException {
        return getDatabase().entity(idFromUuid(uuid));
    }

    /**
     * Get all out vertices connected with a vertex.
     *
     * @param vertexId The vertex ID
     * @param labels   The labels to follow
     * @return An iterable of ID/Uuid pairs
     */
    public Iterable<List<Object>> getOutVertices(Object vertexId, String... labels) {
        return getVertices(vertexId, Direction.OUT, labels);
    }

    /**
     * Get all in vertices connected with a vertex.
     *
     * @param vertexId The vertex ID
     * @param labels   The labels to follow
     * @return An iterable of ID/Uuid pairs
     */
    public Iterable<List<Object>> getInVertices(Object vertexId, String... labels) {
        return getVertices(vertexId, Direction.IN, labels);
    }

    /**
     * Get a vertex's edges for in, out, or both directions, with the given label(s).
     *
     * @param vertexId  The vertex ID
     * @param direction The direction
     * @param labels    The label(s)
     * @return An iterable of ID/Uuid edge pairs
     */
    public Iterable<List<Object>> getEdges(UUID vertexId, Direction direction, String... labels) {
        if (labels.length > 0) {
            switch (direction) {
                case OUT:
                    return Peer.q("[:find ?e ?uuid ?dir ?label" +
                            " :in $ ?vuuid [?label ...] ?dir" +
                            " :where [?v :graph.element/id ?vuuid] " +
                            " [?e :graph.edge/outVertex ?v]" +
                            " [?e :graph.edge/label ?label]" +
                            " [?e :graph.element/id ?uuid] ]",
                            getDatabase(), vertexId, labels, OUT_DIRECTION);
                case IN:
                    return Peer.q("[:find ?e ?uuid ?dir ?label" +
                            " :in $ ?vuuid [?label ...] ?dir" +
                            " :where [?v :graph.element/id ?vuuid] " +
                            " [?e :graph.edge/inVertex ?v] " +
                            " [?e :graph.edge/label ?label]" +
                            " [?e :graph.element/id ?uuid] ]",
                            getDatabase(), vertexId, labels, IN_DIRECTION);
                case BOTH:
                    return Iterables.concat(
                            getEdges(vertexId, Direction.OUT, labels), getEdges(vertexId, Direction.IN, labels));
                default:
                    throw new UnknownError("Unexpected edge direction!");
            }
        } else {
            switch (direction) {
                case OUT:
                    return Peer.q("[:find ?e ?uuid ?dir ?label" +
                            " :in $ ?vuuid ?dir" +
                            " :where [?v :graph.element/id ?vuuid] " +
                            " [?e :graph.edge/outVertex ?v] " +
                            " [?e :graph.edge/label ?label]" +
                            " [?e :graph.element/id ?uuid] ]",
                            getDatabase(), vertexId, OUT_DIRECTION);
                case IN:
                    return Peer.q("[:find ?e ?uuid ?dir ?label" +
                            " :in $ ?vuuid ?dir" +
                            " :where [?v :graph.element/id ?vuuid] " +
                            " [?e :graph.edge/inVertex ?v] " +
                            " [?e :graph.edge/label ?label]" +
                            " [?e :graph.element/id ?uuid] ]",
                            getDatabase(), vertexId, IN_DIRECTION);
                case BOTH:
                    return Iterables.concat(
                            getEdges(vertexId, Direction.OUT), getEdges(vertexId, Direction.IN));
                default:
                    throw new UnknownError("Unexpected edge direction!");
            }
        }
    }

    /**
     * Get all in vertices connected with a vertex.
     *
     * @param vertexId The vertex ID
     * @param labels   The label(s) to follow
     * @return An iterable of ID/Uuid pairs
     */
    public Iterable<List<Object>> getVertices(Object vertexId, Direction direction, String... labels) {
        // NB: This is REALLY crap right now...
        if (labels.length > 0) {
            switch (direction) {
                case OUT:
                    return Peer.q("[:find ?other ?uuid ?dir ?label" +
                            " :in $ ?v [?label ...] ?dir" +
                            " :where [?e :graph.edge/inVertex ?other] " +
                            " [?e :graph.edge/outVertex ?v]" +
                            " [?e :graph.edge/label ?label]" +
                            " [?other :graph.element/id ?uuid] ]",
                            getDatabase(), vertexId, labels, OUT_DIRECTION);
                case IN:
                    return Peer.q("[:find ?other ?uuid ?dir ?label" +
                                    " :in $ ?v [?label ...] ?dir" +
                                    " :where [?e :graph.edge/outVertex ?other] " +
                                    " [?e :graph.edge/inVertex ?v]" +
                                    " [?e :graph.edge/label ?label]" +
                                    " [?other :graph.element/id ?uuid] ]",
                            getDatabase(), vertexId, labels, IN_DIRECTION);
                case BOTH:
                    return Iterables.concat(
                            getInVertices(vertexId), getOutVertices(vertexId));
                default:
                    throw new UnknownError("Unexpected edge direction!");
            }
        } else {
            switch (direction) {
                case OUT:
                    return Peer.q("[:find ?other ?uuid ?dir ?label" +
                            " :in $ ?v ?dir" +
                            " :where [?e :graph.edge/inVertex ?other] " +
                            " [?e :graph.edge/outVertex ?v]" +
                            " [?e :graph.edge/label ?label] " +
                            " [?other :graph.element/id ?uuid] ]",
                            getDatabase(), vertexId, OUT_DIRECTION);
                case IN:
                    return Peer.q("[:find ?other ?uuid ?dir ?label" +
                            " :in $ ?v ?dir" +
                            " :where [?e :graph.edge/outVertex ?other] " +
                            " [?e :graph.edge/inVertex ?v]" +
                            " [?e :graph.edge/label ?label] " +
                            " [?other :graph.element/id ?uuid] ]",
                            getDatabase(), vertexId, IN_DIRECTION);
                case BOTH:
                    return Iterables.concat(
                            getInVertices(vertexId), getOutVertices(vertexId));
                default:
                    throw new UnknownError("Unexpected edge direction!");
            }
        }
    }

    /**
     * Get all in vertices connected with a vertex.
     *
     * @param vertexId The vertex ID
     * @param labels   The label(s) to follow
     * @return An iterable of ID/Uuid pairs
     */
    public Iterable<List<Object>> getVerticesByUuid(UUID vertexId, Direction direction, String... labels) {
        // NB: This is REALLY crap right now...
        if (labels.length > 0) {
            switch (direction) {
                case OUT:
                    return Peer.q("[:find ?other ?uuid ?dir ?label" +
                            " :in $ ?vuuid [?label ...] ?dir" +
                            " :where [?e :graph.edge/inVertex ?other] " +
                            " [?v :graph.element/id ?vuuid ]" +
                            " [?e :graph.edge/outVertex ?v]" +
                            " [?e :graph.edge/label ?label]" +
                            " [?other :graph.element/id ?uuid] ]",
                            getDatabase(), vertexId, labels, OUT_DIRECTION);
                case IN:
                    return Peer.q("[:find ?other ?uuid ?dir ?label" +
                            " :in $ ?vuuid [?label ...] ?dir" +
                            " :where [?e :graph.edge/outVertex ?other] " +
                            " [?v :graph.element/id ?vuuid ]" +
                            " [?e :graph.edge/inVertex ?v]" +
                            " [?e :graph.edge/label ?label]" +
                            " [?other :graph.element/id ?uuid] ]",
                            getDatabase(), vertexId, labels, IN_DIRECTION);
                case BOTH:
                    return Iterables.concat(
                            getVerticesByUuid(vertexId, Direction.OUT, labels),
                            getVerticesByUuid(vertexId, Direction.IN, labels));
                default:
                    throw new UnknownError("Unexpected edge direction!");
            }
        } else {
            switch (direction) {
                case OUT:
                    return Peer.q("[:find ?other ?uuid ?dir ?label" +
                            " :in $ ?vuuid ?dir" +
                            " :where [?e :graph.edge/inVertex ?other] " +
                            " [?v :graph.element/id ?vuuid ]" +
                            " [?e :graph.edge/outVertex ?v]" +
                            " [?e :graph.edge/label ?label] " +
                            " [?other :graph.element/id ?uuid] ]",
                            getDatabase(), vertexId, OUT_DIRECTION);
                case IN:
                    return Peer.q("[:find ?other ?uuid ?dir ?label" +
                            " :in $ ?vuuid ?dir" +
                            " :where [?e :graph.edge/outVertex ?other] " +
                            " [?v :graph.element/id ?vuuid ]" +
                            " [?e :graph.edge/inVertex ?v]" +
                            " [?e :graph.edge/label ?label] " +
                            " [?other :graph.element/id ?uuid] ]",
                            getDatabase(), vertexId, IN_DIRECTION);
                case BOTH:
                    return Iterables.concat(
                            getVerticesByUuid(vertexId, Direction.OUT),
                            getVerticesByUuid(vertexId, Direction.IN));
                default:
                    throw new UnknownError("Unexpected edge direction!");
            }
        }
    }

    /**
     * Return an ID/UUID pair for a given edge's out vertex.
     *
     * @param edge An edge ID
     * @return An ID/UUID pair
     */
    public List<Object> getOutVertex(UUID edge) {
        return getEdgeVertex(edge, OUT_VERTEX);

    }

    /**
     * Return an ID/UUID pair for a given edge's in vertex.
     *
     * @param edge An edge ID
     * @return An ID/UUID pair
     */
    public List<Object> getInVertex(UUID edge) {
        return getEdgeVertex(edge, IN_VERTEX);

    }

    private List<Object> getEdgeVertex(UUID edge, Keyword direction) {
        return Peer.q("[:find ?v ?uuid :in $ ?euuid ?d :where " +
                "[?e :graph.element/id ?euuid ] " +
                "[?e ?d ?v] " +
                "[?v :graph.element/id ?uuid]]",
                getDatabase(), edge, direction).iterator().next();
    }

    /**
     * Fetch a property from an element
     *
     * @param uuid           The graph UUID
     * @param elementClass The class of the element, either Vertex or Edge
     * @param key          The property key
     * @param valueClass   The property's value class
     * @return The property value, or null if it doesn't exist
     */
    public Object getPropertyByUuid(UUID uuid, Class elementClass, String key, Class valueClass) {
        Keyword keyName = FluxUtil.createKey(key, valueClass, elementClass);
        Collection<List<Object>> lists
                = Peer.q("[:find ?p :in $ ?e ?k :where [?e ?k ?p] ]",
                getDatabase(), Keyword.intern(uuid.toString()), keyName);
        Iterator<List<Object>> iterator = lists.iterator();
        return iterator.hasNext() ? iterator.next().get(0) : null;
    }

    public Object getPropertyByUuid(UUID uuid, String key) {
        Entity entity = getDatabase().entity(Keyword.intern(uuid.toString()));
        if (!FluxUtil.isReservedKey(key)) {
            for(String property : entity.keySet()) {
                String propertyName = FluxUtil.getPropertyName(property).get();
                if (key.equals(propertyName)) {
                    return entity.get(property);
                }
            }
            // We didn't find the value
            return null;
        }
        else {
            return entity.get(key);
        }
    }

    /**
     * Fetch a property from an element
     *
     * @param id           The graph internal id
     * @param elementClass The class of the element, either Vertex or Edge
     * @param key          The property key
     * @param valueClass   The property's value class
     * @return The property value, or null if it doesn't exist
     */
    public Object getProperty(Object id, Class elementClass, String key, Class valueClass) {
        Keyword keyword = FluxUtil.createKey(key, valueClass, elementClass);
        Collection<List<Object>> lists = Peer.q("[:find ?p :in $ ?e ?k :where [?e ?k ?p]]",
                getDatabase(), id, keyword);
        Iterator<List<Object>> iterator = lists.iterator();
        return iterator.hasNext() ? iterator.next().get(0) : null;
    }

    /**
     * Add a property to an id, returning the uncommitted statements.
     *
     * @param id           The graph internal ID
     * @param elementClass The class of the element, either Vertex or Edge
     * @param key          The property key
     * @param value        The property value
     * @return A set of database-altering statements, ready to be committed
     */
    public Addition addProperty(Object id, Class elementClass, String key, Object value) {
        return new Addition(id, Util.list(Util.map(":db/id", id,
                FluxUtil.createKey(key, value.getClass(), elementClass), value)));
    }

    /**
     * Add a property to an id, returning the uncommitted statements.
     *
     * @param uuid           The graph UUID
     * @param elementClass The class of the element, either Vertex or Edge
     * @param key          The property key
     * @param value        The property value
     * @return A set of database-altering statements, ready to be committed
     */
    public Addition addPropertyByUuid(UUID uuid, Class elementClass, String key, Object value) {
        return new Addition(uuid, Util.list(Util.map(":db/id", Keyword.intern(uuid.toString()),
                FluxUtil.createKey(key, value.getClass(), elementClass), value)));
    }

    /**
     * Remove a property from an id, returning the uncommitted statements.
     *
     * @param id           The graph internal ID
     * @param elementClass The class of the element, either Vertex or Edge
     * @param key          The property key
     * @param valueClass   The property's value class
     * @return A set of database-altering statements, ready to be committed
     */
    public List removeProperty(Object id, Class elementClass, String key, Class valueClass) {
        Object currentValue = getProperty(id, elementClass, key, valueClass);
        if (currentValue != null) {
            return Util.list(Util.list(":db/retract", id,
                    FluxUtil.createKey(key, valueClass, elementClass), currentValue));
        } else {
            return Util.list();
        }
    }

    /**
     * Remove a property from an id, returning the uncommitted statements.
     *
     * @param id           The graph internal ID
     * @param elementClass The class of the element, either Vertex or Edge
     * @param key          The property key
     * @param valueClass   The property's value class
     * @return A set of database-altering statements, ready to be committed
     */
    public List removePropertyByUuid(UUID id, Class elementClass, String key, Class valueClass) {
        Object currentValue = getPropertyByUuid(id, elementClass, key, valueClass);
        if (currentValue != null) {
            return Util.list(Util.list(":db/retract", Keyword.intern(id.toString()),
                    FluxUtil.createKey(key, valueClass, elementClass), currentValue));
        } else {
            return Util.list();
        }
    }

    public Set<String> getPropertyKeys(Object id) {
        Entity entity = getDatabase().entity(id);
        Set<String> filtered = Sets.newHashSet();
        for (String property : entity.keySet()) {
            if (!FluxUtil.isReservedKey(property)) {
                filtered.add(FluxUtil.getPropertyName(property).get());
            }
        }
        return filtered;
    }

    public Set<String> getPropertyKeysByUuid(UUID id) {
        Entity entity = getDatabase().entity(Keyword.intern(id.toString()));
        Set<String> filtered = Sets.newHashSet();
        for (String property : entity.keySet()) {
            if (!FluxUtil.isReservedKey(property)) {
                filtered.add(FluxUtil.getPropertyName(property).get());
            }
        }
        return filtered;
    }

    public Addition addVertex(UUID uuid) {
        Object tempId = Peer.tempid(":graph");
        return new Addition(tempId, Util.list(Util.map(
                ":db/id", tempId,
                ":db/ident", Keyword.intern(uuid.toString()),
                ":graph.element/type", VERTEX_TYPE,
                ELEMENT_ID, uuid
        )));
    }

    public Addition addEdge(UUID uuid, String label, Object outVertex, Object inVertex) {
        Object tempid = Peer.tempid(":graph");
        return new Addition(tempid, Util.list(Util.map(
                ":db/id", tempid,
                ":db/ident", Keyword.intern(uuid.toString()),
                ":graph.element/type", EDGE_TYPE,
                EDGE_LABEL, label,
                OUT_VERTEX, outVertex,
                IN_VERTEX, inVertex,
                ELEMENT_ID, uuid
        )));
    }
}
