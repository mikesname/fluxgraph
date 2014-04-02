package com.jnj.fluxgraph;

import clojure.lang.ExceptionInfo;
import clojure.lang.Keyword;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.Iterables;
import com.tinkerpop.blueprints.*;
import com.tinkerpop.blueprints.util.DefaultGraphQuery;
import com.tinkerpop.blueprints.util.ExceptionFactory;
import com.tinkerpop.blueprints.util.StringFactory;

import static datomic.Connection.TEMPIDS;
import static datomic.Connection.DB_AFTER;

import com.tinkerpop.blueprints.util.WrappingCloseableIterable;
import datomic.*;

import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * A Blueprints implementation of a graph on top of Datomic
 *
 * @author Davy Suvee (http://datablend.be)
 * @author Mike Bryant (http://github.com/mikesname)
 */
public class FluxGraph implements MetaGraph<Database>, TimeAwareGraph, TransactionalGraph, KeyIndexableGraph {

    private final String graphURI;
    private final Connection connection;
    public static final String DATOMIC_ERROR_EXCEPTION_MESSAGE = "An error occured within the Datomic datastore";

    public final Object GRAPH_ELEMENT_TYPE;
    public final Object GRAPH_ELEMENT_TYPE_VERTEX;
    public final Object GRAPH_ELEMENT_TYPE_EDGE;
    public final Object GRAPH_EDGE_IN_VERTEX;
    public final Object GRAPH_EDGE_OUT_VERTEX;
    public final Object GRAPH_EDGE_LABEL;

    protected final ThreadLocal<TxManager> tx = new ThreadLocal<TxManager>() {
        protected TxManager initialValue() {
            return new TxManager(connection);
        }
    };
    protected final ThreadLocal<Long> checkpointTime = new ThreadLocal<Long>() {
        protected Long initialValue() {
            return null;
        }
    };
    protected final ThreadLocal<Date> transactionTime = new ThreadLocal<Date>() {
        protected Date initialValue() {
            return null;
        }
    };
    protected final ThreadLocal<IdResolver> idResolver = new ThreadLocal<IdResolver>() {
        protected IdResolver initialValue() {
            return new IdResolver();
        }
    };

    private FluxHelper helper = new FluxHelper();

    public FluxHelper getHelper() {
        return helper;
    }

    public TxManager getTxManager() {
        return tx.get();
    }

    private static final Features FEATURES = new Features();

    static {
        FEATURES.supportsDuplicateEdges = true;
        FEATURES.supportsSelfLoops = true;
        FEATURES.isPersistent = false;
        FEATURES.supportsVertexIteration = true;
        FEATURES.supportsEdgeIteration = true;
        FEATURES.supportsVertexIndex = false;
        FEATURES.supportsEdgeIndex = false;
        FEATURES.ignoresSuppliedIds = true;
        FEATURES.supportsEdgeRetrieval = true;
        FEATURES.supportsVertexProperties = true;
        FEATURES.supportsEdgeProperties = true;
        FEATURES.supportsTransactions = true;
        FEATURES.supportsIndices = false;

        FEATURES.supportsSerializableObjectProperty = false;
        FEATURES.supportsBooleanProperty = true;
        FEATURES.supportsDoubleProperty = true;
        FEATURES.supportsFloatProperty = true;
        FEATURES.supportsIntegerProperty = true;
        FEATURES.supportsPrimitiveArrayProperty = false;
        FEATURES.supportsUniformListProperty = false;
        FEATURES.supportsMixedListProperty = false;
        FEATURES.supportsLongProperty = true;
        FEATURES.supportsMapProperty = false;
        FEATURES.supportsStringProperty = true;

        FEATURES.isWrapper = true;
        FEATURES.supportsKeyIndices = false;
        FEATURES.supportsVertexKeyIndex = false;
        FEATURES.supportsEdgeKeyIndex = false;
        FEATURES.supportsThreadedTransactions = false;
        FEATURES.ignoresSuppliedIds = true;
    }

    public FluxGraph(final String graphURI) {
        this.graphURI = graphURI;
        Peer.createDatabase(graphURI);
        // Retrieve the connection
        this.connection = Peer.connect(graphURI);

        try {
            // Setup the meta model for the graph
            if (requiresMetaModel(connection.db())) {
                setupMetaModel(connection);
            }
            // Retrieve the relevant ids for the properties (for raw index access later on)
            GRAPH_ELEMENT_TYPE = FluxUtil.getIdForAttribute(connection.db(), "graph.element/type");
            GRAPH_ELEMENT_TYPE_VERTEX = FluxUtil.getIdForAttribute(connection.db(), "graph.element.type/vertex");
            GRAPH_ELEMENT_TYPE_EDGE = FluxUtil.getIdForAttribute(connection.db(), "graph.element.type/edge");
            GRAPH_EDGE_IN_VERTEX = FluxUtil.getIdForAttribute(connection.db(), "graph.edge/inVertex");
            GRAPH_EDGE_OUT_VERTEX = FluxUtil.getIdForAttribute(connection.db(), "graph.edge/outVertex");
            GRAPH_EDGE_LABEL = FluxUtil.getIdForAttribute(connection.db(), "graph.edge/label");
        } catch (Exception e) {
            throw new RuntimeException(FluxGraph.DATOMIC_ERROR_EXCEPTION_MESSAGE, e);
        }
    }

    @Override
    public Features getFeatures() {
        return FEATURES;
    }

    @Override
    public void stopTransaction(Conclusion conclusion) {
        if (conclusion.equals(Conclusion.FAILURE)) {
            rollback();
        } else {
            commit();
        }
    }

    @Override
    public void shutdown() {
        commit();
    }

    @Override
    public void commit() {
        FluxUtil.debugLog("Commit... tx events: " + tx.get().size());
        transact();
    }

    @Override
    public void rollback() {
        FluxUtil.debugLog("Rollback... tx events: " + tx.get().size());
        tx.get().flush();
        idResolver.get().clear();
    }

    @Override
    public TimeAwareEdge getEdge(final Object id) {
        if (null == id) {
            throw ExceptionFactory.edgeIdCanNotBeNull();
        }
        try {
            UUID uuid = FluxUtil.externalIdToUuid(id);
            List<Object> data = getHelper().getEdge(getTxManager().getDatabase(), uuid);
            return new FluxEdge(this, Optional.<Database>absent(), uuid, data.get(0), (String) data.get(2));
        } catch (NoSuchElementException e) {
            return null;
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    @Override
    public GraphQuery query() {
        return new DefaultGraphQuery(this);
    }

    @Override
    public TimeAwareEdge addEdge(final Object id, final Vertex outVertex, final Vertex inVertex, final String label) {

        if (label == null) {
            throw ExceptionFactory.edgeLabelCanNotBeNull();
        }

        // Create the new edge
        try {
            UUID uuid = Peer.squuid();
            FluxVertex out = (FluxVertex) outVertex;
            FluxVertex in = (FluxVertex) inVertex;
            FluxHelper.Addition addition = getHelper().addEdge(getTxManager().getDatabase(), uuid, label, out.graphId,
                    in.graphId);
            tx.get().add(uuid, addition.statements.get(0), out.getId(), in.getId());
            final FluxEdge edge = new FluxEdge(this, null, uuid, addition.tempId, label);
            idResolver.get().put(addition.tempId, edge);
            return edge;
        } catch (ExceptionInfo e) {
            if (e.toString().contains("not a valid :string for attribute")) {
                throw new IllegalArgumentException(e.toString());
            } else {
                throw e;
            }
        }
    }

    @Override
    public TimeAwareVertex addVertex(final Object id) {
        // Create the new vertex
        UUID uuid = Peer.squuid();
        FluxHelper.Addition addition = getHelper().addVertex(getTxManager().getDatabase(), uuid);
        tx.get().add(uuid, addition.statements.get(0));

        // Add to the dirty pile...
        FluxVertex vertex = new FluxVertex(this, null, uuid, addition.tempId);
        idResolver.get().put(addition.tempId, vertex);

        return vertex;
    }

    @Override
    public TimeAwareVertex getVertex(final Object id) {
        if (null == id) {
            throw ExceptionFactory.vertexIdCanNotBeNull();
        }
        try {
            UUID uuid = FluxUtil.externalIdToUuid(id);
            List<Object> data = getHelper().getVertex(getTxManager().getDatabase(), uuid);
            return new FluxVertex(this, Optional.<Database>absent(), uuid, data.get(0));
        } catch (NoSuchElementException e) {
            return null;
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    @Override
    public CloseableIterable<Vertex> getVertices() {
        final FluxGraph graph = this;
        Iterable<List<Object>> vertices = getHelper().listVertices(getTxManager().getDatabase());

        return new WrappingCloseableIterable<Vertex>(Iterables.transform(vertices,
                new Function<List<Object>, Vertex>() {
                    @Override
                    public FluxVertex apply(List<Object> o) {
                        return new FluxVertex(graph, Optional.<Database>absent(), (UUID) o.get(1), o.get(0));
                    }
                }));
    }

    @Override
    public CloseableIterable<Edge> getEdges() {
        final FluxGraph graph = this;
        Iterable<List<Object>> edges = getHelper().listEdges(getTxManager().getDatabase());

        return new WrappingCloseableIterable<Edge>(Iterables.transform(edges,
                new Function<List<Object>,
                        Edge>() {
                    @Override
                    public Edge apply(List<Object> o) {
                        return new FluxEdge(graph, Optional.<Database>absent(), (UUID) o.get(1), o.get(0), (String) o.get(2));
                    }
                }));
    }

    @Override
    public CloseableIterable<Vertex> getVertices(String key, Object value) {

        final FluxGraph graph = this;
        //return vertexIndex.get(key, value);
        Keyword keyword = FluxUtil.createKey(key, value.getClass(), Vertex.class);
        return new WrappingCloseableIterable<Vertex>(Iterables.transform(getHelper().listVertices(getTxManager()
                .getDatabase(), keyword, value),
                new Function<List<Object>,
                        Vertex>() {
                    @Override
                    public FluxVertex apply(List<Object> o) {
                        return new FluxVertex(graph, Optional.<Database>absent(), (UUID) o.get(1), o.get(0));
                    }
                }));
    }

    @Override
    public CloseableIterable<Edge> getEdges(String key, Object value) {
        //return edgeIndex.get(key, value);
        Keyword keyword = FluxUtil.createKey(key, value.getClass(), Edge.class);
        if (key.equals("label")) {
            keyword = Keyword.intern("graph.edge/label");
        }
        final FluxGraph graph = this;
        return new WrappingCloseableIterable<Edge>(Iterables.transform(getHelper().listEdges(getTxManager()
                .getDatabase(),
                keyword,
                value),
                new Function<List<Object>, Edge>() {
                    @Override
                    public FluxEdge apply(List<Object> o) {
                        return new FluxEdge(graph, Optional.<Database>absent(), (UUID) o.get(1), o.get(0), (String) o.get(2));
                    }
                }));
    }

    @Override
    public void setCheckpointTime(Date date) {
        Long transaction = null;
        // Retrieve the transactions
        for (List<Object> txobject : (Peer.q("[:find ?tx ?when " +
                ":where [?tx :db/txInstant ?when]]", getRawGraph(date)))) {
            Long transactionid = (Long) txobject.get(0);
            if (transaction == null) {
                transaction = transactionid;
            } else {
                if (transactionid > transaction) {
                    transaction = transactionid;
                }
            }
        }
        this.checkpointTime.set(transaction);
    }

    @Override
    public void setTransactionTime(Date transactionTime) {
        this.transactionTime.set(transactionTime);
    }

    @Override
    public Date getTransactionTime() {
        return transactionTime.get();
    }

    @Override
    public Graph difference(WorkingSet workingSet, Date date1, Date date2) {
        Set<Object> factsAtDate1 = new HashSet<Object>();
        Set<Object> factsAtDate2 = new HashSet<Object>();
        // Set graph at checkpoint date1
        setCheckpointTime(date1);
        for (Object vertex : workingSet.getVertices()) {
            factsAtDate1.addAll(((FluxVertex) getVertex(vertex)).getFacts());
        }
        for (Object edge : workingSet.getEdges()) {
            factsAtDate1.addAll(((FluxEdge) getEdge(edge)).getFacts());
        }
        // Set graph at checkpoint date2
        setCheckpointTime(date2);
        for (Object vertex : workingSet.getVertices()) {
            factsAtDate2.addAll(((FluxVertex) getVertex(vertex)).getFacts());
        }
        for (Object edge : workingSet.getEdges()) {
            factsAtDate2.addAll(((FluxEdge) getEdge(edge)).getFacts());
        }
        // Calculate the difference between the facts of both time aware elements
        Set<Object> difference = FluxUtil.difference(factsAtDate1, factsAtDate2);
        return new ImmutableFluxGraph("datomic:mem://temp" + UUID.randomUUID(), this, difference);
    }

    @Override
    public Graph difference(TimeAwareElement element1, TimeAwareElement element2) {
        // Calculate the difference between the facts of both time aware elements
        Set<Object> difference = FluxUtil.difference(((FluxElement) element1).getFacts(), ((FluxElement) element2).getFacts());
        return new ImmutableFluxGraph("datomic:mem://temp" + UUID.randomUUID(), this, difference);
    }

    @Override
    public String toString() {
        return StringFactory.graphString(this, graphURI);
    }

    @Override
    public <T extends Element> void dropKeyIndex(String key, Class<T> elementClass) {
        if (elementClass == null) {
            throw ExceptionFactory.classForElementCannotBeNull();
        }
        FluxUtil.removeAttributeIndex(key, elementClass, connection);
    }

    @Override
    public <T extends Element> void createKeyIndex(String key, Class<T> elementClass, Parameter... parameter) {
        if (elementClass == null) {
            throw ExceptionFactory.classForElementCannotBeNull();
        }
        FluxUtil.createAttributeIndex(key, elementClass, connection);
    }

    //@Override
    public <T extends Element> Set<String> getIndexedKeys(Class<T> elementClass) {
        if (elementClass == null) {
            throw ExceptionFactory.classForElementCannotBeNull();
        }
        return FluxUtil.getIndexedAttributes(elementClass, this);
    }

    public void clear() {
        for (Vertex vertex : getVertices()) {
            removeVertex(vertex);
        }
    }

    public Database getRawGraph(Object transaction) {
        Database database = getTxManager().getDatabase();
        if (transaction == null) {
            return database;
        }
        return database.asOf(transaction);
    }

    @Override
    public Database getRawGraph() {
        if (checkpointTime.get() != null) {
            return getRawGraph(checkpointTime.get());
        }
        return getRawGraph(null);
    }

    public void transact() {
        TxManager txManager = tx.get();
        try {
            Map map = connection.transact(txManager.ops()).get();
            txManager.flush();
            idResolver.get().resolveIds((Database) map.get(DB_AFTER), (Map) map.get(TEMPIDS));
        } catch (InterruptedException e) {
            txManager.flush();
            throw new RuntimeException(FluxGraph.DATOMIC_ERROR_EXCEPTION_MESSAGE, e);
        } catch (ExecutionException e) {
            txManager.flush();
            throw new RuntimeException(FluxGraph.DATOMIC_ERROR_EXCEPTION_MESSAGE, e);
        }
    }

    public Connection getConnection() {
        return connection;
    }

    @Override
    public void removeEdge(final Edge edge) {
        // Retract the edge element in its totality
        FluxEdge theEdge = (FluxEdge) edge;
        TxManager txManager = tx.get();
        if (txManager.newInThisTx(theEdge.getId())) {
            txManager.remove(theEdge.getId());
            idResolver.get().removeElement(theEdge);
        } else {
            txManager.del(theEdge.getId(), Util.list(":db.fn/retractEntity", theEdge.graphId));
        }
    }

    @Override
    public void removeVertex(Vertex v) {
        FluxVertex vertex = (FluxVertex) v;
        // Retract the vertex element in its totality
        // Update the transaction info of the vertex
        TxManager txManager = tx.get();
        if (txManager.newInThisTx(vertex.getId())) {
            txManager.remove(vertex.getId());
            idResolver.get().removeElement(vertex);
        } else {
            // Retrieve all edges associated with this vertex and remove them one bye one
            for (Edge edge : vertex.getEdges(Direction.BOTH)) {
                removeEdge(edge);
            }
            txManager.del(vertex.getId(), Util.list(":db.fn/retractEntity", vertex.graphId));
        }
    }

    // Helper method to check whether the meta model of the graph still needs to be setup
    protected boolean requiresMetaModel(Database database) {

        return !Peer.q("[:find ?entity " +
                ":in $ " +
                ":where [?entity :db/ident :graph.element/type] ] ", database).iterator().hasNext();
    }

    // Setup of the various attribute types required for FluxGraph
    protected void setupMetaModel(Connection connection) throws Exception {

        getHelper().loadMetaModel(connection);

//        tx.get().global(datomic.Util.map(":db/id", datomic.Peer.tempid(":db.part/tx"), ":db/txInstant",
//                new Date(0)));
//        connection.transact(Lists.newArrayList(tx.get().ops())).get();
//        tx.get().flush();
    }


    public void dumpPendingOps(FileWriter fileWriter) throws IOException {
        for (Object pending : tx.get().ops()) {
            fileWriter.write(pending.toString() + "\n");
        }
    }

}