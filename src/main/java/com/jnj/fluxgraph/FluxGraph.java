package com.jnj.fluxgraph;

import clojure.lang.ExceptionInfo;
import clojure.lang.Keyword;
import com.google.common.base.Function;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.tinkerpop.blueprints.*;
import com.tinkerpop.blueprints.util.DefaultGraphQuery;
import com.tinkerpop.blueprints.util.ExceptionFactory;
import com.tinkerpop.blueprints.util.StringFactory;
import static datomic.Connection.TEMPIDS;
import static datomic.Connection.DB_AFTER;
import datomic.*;

import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * A Blueprints implementation of a graph on top of Datomic
 *
 * @author Davy Suvee (http://datablend.be)
 */
public class FluxGraph implements MetaGraph<Database>, TimeAwareGraph, TransactionalGraph {

    private final String graphURI;
    private final Connection connection;
    public static final String DATOMIC_ERROR_EXCEPTION_MESSAGE = "An error occured within the Datomic datastore";

    public final Object GRAPH_ELEMENT_TYPE;
    public final Object GRAPH_ELEMENT_TYPE_VERTEX;
    public final Object GRAPH_ELEMENT_TYPE_EDGE;
    public final Object GRAPH_EDGE_IN_VERTEX;
    public final Object GRAPH_EDGE_OUT_VERTEX;
    public final Object GRAPH_EDGE_LABEL;

    private final FluxIndex vertexIndex;
    private final FluxIndex edgeIndex;

    protected final ThreadLocal<TxManager> tx = new ThreadLocal<TxManager>() {
        protected TxManager initialValue() {
            return new TxManager();
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

    public FluxHelper getHelper() {
        return new FluxHelper(connection, tx.get().ops());
    }

    public TxManager getTxManager() {
        return tx.get();
    }

    private static final Features FEATURES = new Features();

    static {
        FEATURES.supportsDuplicateEdges = true;
        FEATURES.supportsSelfLoops = true;
        FEATURES.isPersistent = false;
        //FEATURES.isRDFModel = false;
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
            if (requiresMetaModel()) {
                setupMetaModel();
            }
            // Retrieve the relevant ids for the properties (for raw index access later on)
            GRAPH_ELEMENT_TYPE = FluxUtil.getIdForAttribute(this, "graph.element/type");
            GRAPH_ELEMENT_TYPE_VERTEX = FluxUtil.getIdForAttribute(this, "graph.element.type/vertex");
            GRAPH_ELEMENT_TYPE_EDGE = FluxUtil.getIdForAttribute(this, "graph.element.type/edge");
            GRAPH_EDGE_IN_VERTEX = FluxUtil.getIdForAttribute(this, "graph.edge/inVertex");
            GRAPH_EDGE_OUT_VERTEX = FluxUtil.getIdForAttribute(this, "graph.edge/outVertex");
            GRAPH_EDGE_LABEL = FluxUtil.getIdForAttribute(this, "graph.edge/label");
        } catch (Exception e) {
            throw new RuntimeException(FluxGraph.DATOMIC_ERROR_EXCEPTION_MESSAGE, e);
        }
        // Create the required indexes
        this.vertexIndex = new FluxIndex<Vertex>("vertexIndex", this, null, Vertex.class);
        this.edgeIndex = new FluxIndex<Edge>("edgeIndex", this, null, Edge.class);
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
        transact();
    }

    @Override
    public void rollback() {
        tx.get().flush();
    }

    @Override
    public TimeAwareEdge getEdge(final Object id) {
        if (null == id)
            throw ExceptionFactory.edgeIdCanNotBeNull();
        try {
            if (!(id instanceof UUID)) {
                //throw new IllegalArgumentException("FluxGraph id must be a UUID");
                return null;
            }
            List<Object> data = getHelper().getEdge((UUID)id);
            return new FluxEdge(this, this.getRawGraph(), (UUID)id, data.get(0), (String)data.get(2));
        } catch (NoSuchElementException e) {
            return null;
        }
    }

//    @Override
//    public Iterable<Edge> getEdges() {
//        Iterable<Datom> edges = this.getRawGraph().datoms(Database.AVET, GRAPH_ELEMENT_TYPE, GRAPH_ELEMENT_TYPE_EDGE);
//        return new FluxIterable<Edge>(edges, this, this.getRawGraph(), Edge.class);
//    }

    @Override
    public Iterable<Edge> getEdges(String key, Object value) {
        //return edgeIndex.get(key, value);
        Keyword keyword = FluxUtil.createKey(key, value.getClass(), Edge.class);
        if (key.equals("label")) {
            keyword = Keyword.intern("graph.edge/label");
        }
        final FluxGraph graph = this;
        return Iterables.transform(getHelper().listEdges(keyword, value), new Function<List<Object>, Edge>() {
            @Override
            public FluxEdge apply(List<Object> o) {
                return new FluxEdge(graph, connection.db(), (UUID)o.get(1), o.get(0), (String)o.get(2));
            }
        });
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
            FluxHelper.Addition addition = getHelper().addEdge(uuid, label, out.graphId, in.graphId);
            tx.get().add(uuid, addition.statements.get(0), out.getId(), in.getId());
            final FluxEdge edge = new FluxEdge(this, null, uuid, addition.tempId, label);

            // Update the transaction info of both vertices (moving up their current transaction)
//            if ((Long)inVertex.getId() >= 0 && (Long)outVertex.getId() >= 0) {
//                addTransactionInfo((TimeAwareVertex)inVertex, (TimeAwareVertex)outVertex);
//            }

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
        System.out.println("Adding vertex: " + uuid);
        FluxHelper.Addition addition = getHelper().addVertex(uuid);
        tx.get().add(uuid, addition.statements.get(0));

        // Add to the dirty pile...
        FluxVertex vertex = new FluxVertex(this, null, uuid, addition.tempId);
        idResolver.get().put(addition.tempId, vertex);

        return vertex;
    }

    @Override
    public TimeAwareVertex getVertex(final Object id) {
        if (null == id)
            throw ExceptionFactory.vertexIdCanNotBeNull();
        try {
            UUID uuid;
            if (id instanceof UUID) {
                uuid = (UUID)id;
            //} else if (id instanceof String) {
            //    uuid = UUID.fromString(id.toString());
            } else {
                return null;
            }
            List<Object> data = getHelper().getVertex(uuid);
            return new FluxVertex(this, this.getRawGraph(), uuid, data.get(0));
        } catch (NoSuchElementException e) {
            return null;
        }
    }

    @Override
    public Iterable<Vertex> getVertices() {
        final FluxGraph graph = this;
        return Iterables.transform(getHelper().listVertices(), new Function<List<Object>, Vertex>() {
            @Override
            public FluxVertex apply(List<Object> o) {
                return new FluxVertex(graph, connection.db(), (UUID)o.get(1), o.get(0));
            }
        });
    }

    @Override
    public Iterable<Edge> getEdges() {
        final FluxGraph graph = this;
        return Iterables.transform(getHelper().listEdges(), new Function<List<Object>, Edge>() {
            @Override
            public FluxEdge apply(List<Object> o) {
                return new FluxEdge(graph, connection.db(), (UUID)o.get(1), o.get(0), (String)o.get(2));
            }
        });
    }

    @Override
    public Iterable<Vertex> getVertices(String key, Object value) {
        //return vertexIndex.get(key, value);
        final FluxGraph graph = this;
        Keyword keyword = FluxUtil.createKey(key, value.getClass(), Vertex.class);
        return Iterables.transform(getHelper().listVertices(keyword, value), new Function<List<Object>, Vertex>() {
            @Override
            public FluxVertex apply(List<Object> o) {
                return new FluxVertex(graph, connection.db(), (UUID)o.get(1), o.get(0));
            }
        });
    }

    @Override
    public Database getRawGraph() {
        if (checkpointTime.get() != null) {
            return getRawGraph(checkpointTime.get());
        }
        return dbWithTx();
    }

    @Override
    public void setCheckpointTime(Date date) {
        Long transaction = null;
        // Retrieve the transactions
        Iterator<List<Object>> tx = (Peer.q("[:find ?tx ?when " +
                                           ":where [?tx :db/txInstant ?when]]", dbWithTx().asOf(date))).iterator();
        while (tx.hasNext()) {
            List<Object> txobject = tx.next();
            Long transactionid = (Long)txobject.get(0);
            if (transaction == null) {
                transaction = transactionid;
            }
            else {
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
    public Graph difference(WorkingSet workingSet, Date date1, Date date2) {
        Set<Object> factsAtDate1 = new HashSet<Object>();
        Set<Object> factsAtDate2 = new HashSet<Object>();
        // Set graph at checkpoint date1
        setCheckpointTime(date1);
        for (Object vertex : workingSet.getVertices()) {
            factsAtDate1.addAll(((FluxVertex)getVertex(vertex)).getFacts());
        }
        for (Object edge : workingSet.getEdges()) {
            factsAtDate1.addAll(((FluxEdge)getEdge(edge)).getFacts());
        }
        // Set graph at checkpoint date2
        setCheckpointTime(date2);
        for (Object vertex : workingSet.getVertices()) {
            factsAtDate2.addAll(((FluxVertex)getVertex(vertex)).getFacts());
        }
        for (Object edge : workingSet.getEdges()) {
            factsAtDate2.addAll(((FluxEdge)getEdge(edge)).getFacts());
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

    //@Override
    public <T extends Element> void dropKeyIndex(String key, Class<T> elementClass) {
        if (elementClass == null) {
            throw ExceptionFactory.classForElementCannotBeNull();
        }
        FluxUtil.removeAttributeIndex(key, elementClass, this);
    }

    //@Override
    public <T extends Element> void createKeyIndex(String key, Class<T> elementClass, Parameter... parameter) {
        if (elementClass == null) {
            throw ExceptionFactory.classForElementCannotBeNull();
        }
        FluxUtil.createAttributeIndex(key, elementClass, this);
    }

    //@Override
    public <T extends Element> Set<String> getIndexedKeys(Class<T> elementClass) {
        if (elementClass == null) {
            throw ExceptionFactory.classForElementCannotBeNull();
        }
        return FluxUtil.getIndexedAttributes(elementClass, this);
    }

    public Date getTransactionTime() {
        return transactionTime.get();
    }

    public void clear() {
        for (Vertex vertex : getVertices()) {
            removeVertex(vertex);
        }
    }

    public Database dbWithTx() {
        return (Database)connection.db().with(Lists.newArrayList(tx.get().ops())).get(DB_AFTER);
    }

    public Database getRawGraph(Object transaction) {
        if (transaction == null) {
            return dbWithTx();
        }
        return dbWithTx().asOf(transaction);
    }

    public void transact() {
        try {
            // We are adding a fact which dates back to the past. Add the required meta data on the transaction
            if (transactionTime.get() != null) {
                tx.get().global(datomic.Util.map(":db/id", datomic.Peer.tempid(":db.part/tx"), ":db/txInstant",
                        transactionTime.get()));
            }
            Map map = connection.transact(tx.get().ops()).get();
            idResolver.get().resolveIds((Database) map.get(DB_AFTER), (Map) map.get(TEMPIDS));
            tx.get().flush();
        } catch (InterruptedException e) {
            tx.get().flush();
            throw new RuntimeException(FluxGraph.DATOMIC_ERROR_EXCEPTION_MESSAGE);
        } catch (ExecutionException e) {
            tx.get().flush();
            throw new RuntimeException(FluxGraph.DATOMIC_ERROR_EXCEPTION_MESSAGE);
        }
    }

    public Connection getConnection() {
        return connection;
    }

    // Ensures that add-transaction-info database function is called during the transaction execution. This will setup the linked list of transactions
    public void addTransactionInfo(TimeAwareElement... elements) {
//        for (TimeAwareElement element : elements) {
//            tx.get().mod(element.getId(), Util.list(":add-transaction-info", element.getId(), element.getTimeId()));
//        }
    }

    @Override
    public void removeEdge(final Edge edge) {
        // Retract the edge element in its totality
        FluxEdge theEdge =  (FluxEdge)edge;
        TxManager txManager = tx.get();
        if (txManager.isAdded(theEdge.getId())) {
            txManager.remove(theEdge.getId());
        } else {
            txManager.del(theEdge.getId(), Util.list(":db.fn/retractEntity", theEdge.getId()));
        }

        // Get the in and out vertex (as their version also needs to be updated)
//        FluxVertex inVertex = (FluxVertex)theEdge.getVertex(Direction.IN);
//        FluxVertex outVertex = (FluxVertex)theEdge.getVertex(Direction.OUT);

        // Update the transaction info of the edge and both vertices (moving up their current transaction)
//        if ((Long)theEdge.getId() >= 0L) {
//            addTransactionInfo(theEdge, inVertex, outVertex);
//        }
    }

    @Override
    public void removeVertex(Vertex v) {
        FluxVertex vertex = (FluxVertex)v;
        // Retract the vertex element in its totality
        // Update the transaction info of the vertex
        TxManager txManager = tx.get();
        System.out.println("Removing: " + v.getId());
        if (txManager.isAdded(vertex.getId())) {
            System.out.println("Removing uncommitted item from queue... ");
            txManager.remove(vertex.getId());
        } else {
            System.out.println("       Removing COMMITTED item...");
            // Retrieve all edges associated with this vertex and remove them one bye one
            for (Edge edge : vertex.getEdges(Direction.BOTH)) {
                removeEdge(edge);
            }
            txManager.del(vertex.getId(), Util.list(":db.fn/retractEntity", vertex.getId()));
        }
    }

    // Helper method to check whether the meta model of the graph still needs to be setup
    protected boolean requiresMetaModel() {

        return !Peer.q("[:find ?entity " +
                       ":in $ " +
                       ":where [?entity :db/ident :graph.element/type] ] ", getRawGraph()).iterator().hasNext();
    }

    // Setup of the various attribute types required for FluxGraph
    protected void setupMetaModel() throws ExecutionException, InterruptedException, Exception {

        getHelper().loadMetaModel();

//        tx.get().global(datomic.Util.map(":db/id", datomic.Peer.tempid(":db.part/tx"), ":db/txInstant",
//                new Date(0)));
//        connection.transact(Lists.newArrayList(tx.get().ops())).get();
//        tx.get().flush();
    }

}