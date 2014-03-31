package com.jnj.fluxgraph;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.tinkerpop.blueprints.*;
import com.tinkerpop.blueprints.util.*;
import datomic.*;

import java.util.*;

/**
 * @author Davy Suvee (http://datablend.be)
 */
public class FluxVertex extends FluxElement implements TimeAwareVertex {

    protected FluxVertex(final FluxGraph fluxGraph, final Optional<Database> database) {
        super(fluxGraph, database);
    }

    public FluxVertex(final FluxGraph fluxGraph, final Optional<Database> database, final UUID uuid, final Object id) {
        super(fluxGraph, database, uuid, id);
    }

    @Override
    public TimeAwareVertex getPreviousVersion() {
        // Retrieve the previous version time id
        Object previousTimeId = FluxUtil.getPreviousTransaction(fluxGraph, this);
        if (previousTimeId != null) {
            // Create a new version of the vertex timescoped to the previous time id
            return new FluxVertex(fluxGraph, Optional.of(fluxGraph.getRawGraph(previousTimeId)), uuid, graphId);
        }
        return null;
    }

    @Override
    public TimeAwareVertex getNextVersion() {
        // Retrieve the next version time id
        Object nextTimeId = FluxUtil.getNextTransactionId(fluxGraph, this);
        if (nextTimeId != null) {
            FluxVertex nextVertexVersion = new FluxVertex(fluxGraph, Optional.of(fluxGraph.getRawGraph(nextTimeId)),
                    uuid, graphId);
            // If no next version exists, the version of the edge is the current version (timescope with a null database)
            if (FluxUtil.getNextTransactionId(fluxGraph, nextVertexVersion) == null) {
                return new FluxVertex(fluxGraph, Optional.<Database>absent(), uuid, graphId);
            }
            else {
                return nextVertexVersion;
            }
        }
        return null;
    }

    @Override
    public Iterable<TimeAwareVertex> getNextVersions() {
        return new FluxTimeIterable(this, true);
    }

    @Override
    public Iterable<TimeAwareVertex> getPreviousVersions() {
        return new FluxTimeIterable(this, false);
    }

    @Override
    public Iterable<TimeAwareVertex> getPreviousVersions(TimeAwareFilter timeAwareFilter) {
        return new FluxTimeIterable(this, false, timeAwareFilter);
    }

    @Override
    public Iterable<TimeAwareVertex> getNextVersions(TimeAwareFilter timeAwareFilter) {
        return new FluxTimeIterable(this, true, timeAwareFilter);
    }

    @Override
    public CloseableIterable<Edge> getEdges(Direction direction, String... labels) {
        if (direction.equals(Direction.OUT)) {
            return this.getOutEdges(labels);
        } else if (direction.equals(Direction.IN))
            return this.getInEdges(labels);
        else {
            return new WrappingCloseableIterable<Edge>(Iterables.concat(this.getInEdges(labels),
                    this.getOutEdges(labels)));
        }
    }

    @Override
    public Iterable<Vertex> getVertices(Direction direction, String... labels) {
        return Iterables.transform(fluxGraph.getHelper().getVerticesByUuid(getDatabase(),
                uuid, direction,
                labels),
                new Function<List<Object>,
                Vertex>() {
            @Override
            public Vertex apply(List<Object> objects) {
                return new FluxVertex(fluxGraph, database, (UUID)objects.get(1),
                        objects.get(0));
            }
        });
    }

    @Override
    public String toString() {
        return "v[" + uuid + "]";
    }

    @Override
    public VertexQuery query() {
        return new DefaultVertexQuery(this);
    }

    @Override
    public Edge addEdge(String label, Vertex vertex) {
        return fluxGraph.addEdge(null, this, vertex, label);
    }

    @Override
    public Set<Object> getFacts() {
        // Retrieve the facts of the vertex itself
        Set<Object> theFacts = super.getFacts();
        // Get the facts associated with the edges of this vertex
        Iterator<Edge> edgesIt = getEdges(Direction.BOTH).iterator();
        while (edgesIt.hasNext()) {
            Edge edge = edgesIt.next();
            // Add the fact that the edge entity is an edge
            theFacts.add(FluxUtil.map(":db/id", edge.getId(), ":graph.element/type", ":graph.element.type/edge"));
            // Add the out and in vertex
            theFacts.add(FluxUtil.map(":db/id", edge.getVertex(Direction.IN).getId(), ":graph.element/type", ":graph.element.type/vertex"));
            theFacts.add(FluxUtil.map(":db/id", edge.getId(), ":graph.edge/inVertex", edge.getVertex(Direction.IN).getId()));
            theFacts.add(FluxUtil.map(":db/id", edge.getVertex(Direction.OUT).getId(), ":graph.element/type", ":graph.element.type/vertex"));
            theFacts.add(FluxUtil.map(":db/id", edge.getId(), ":graph.edge/outVertex", edge.getVertex(Direction.OUT).getId()));
            // Add the label
            theFacts.add(FluxUtil.map(":db/id", edge.getId(), ":graph.edge/label", edge.getLabel()));
        }
        return theFacts;
    }

    private CloseableIterable<Edge> getInEdges(final String... labels) {
        return new WrappingCloseableIterable<Edge>(Iterables.transform(fluxGraph.getHelper().getEdges(getDatabase(), uuid, Direction.IN, labels),
                new Function<List<Object>, Edge>() {
            @Override
            public Edge apply(List<Object> input) {
                return new FluxEdge(fluxGraph, database, (UUID)input.get(1), input.get(0), (String)input.get(3));
            }
        }));
    }

//    private Iterable<Edge> getInEdges() {
//        Iterable<Datom> inEdges = getDatabase().datoms(Database.AVET, fluxGraph.GRAPH_EDGE_IN_VERTEX, getId());
//        return new FluxIterable<Edge>(inEdges, fluxGraph, database, Edge.class);
//    }

    private CloseableIterable<Edge> getOutEdges(final String... labels) {
        return new WrappingCloseableIterable<Edge>(Iterables.transform(fluxGraph.getHelper().getEdges(getDatabase(), uuid, Direction.OUT, labels),
                new Function<List<Object>, Edge>() {
            @Override
            public Edge apply(List<Object> input) {
                return new FluxEdge(fluxGraph, database, (UUID)input.get(1), input.get(0), (String)input.get(3));
            }
        }));
    }

//    private Iterable<Edge> getOutEdges() {
//        Iterable<Datom> outEdges = getDatabase().datoms(Database.AVET, fluxGraph.GRAPH_EDGE_OUT_VERTEX, getId());
//        return new FluxIterable<Edge>(outEdges, fluxGraph, database, Edge.class);
//    }

    @Override
    public void remove() {
        fluxGraph.removeVertex(this);
    }
}
