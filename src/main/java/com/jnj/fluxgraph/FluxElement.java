package com.jnj.fluxgraph;

import clojure.lang.Keyword;
import com.tinkerpop.blueprints.TimeAwareElement;
import com.tinkerpop.blueprints.util.ExceptionFactory;
import com.tinkerpop.blueprints.util.StringFactory;
import datomic.Database;
import datomic.Entity;
import datomic.Peer;
import datomic.Util;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.util.*;

/**
 * @author Davy Suvee (http://datablend.be)
 */
public abstract class FluxElement implements TimeAwareElement {

    protected final Database database;
    protected final FluxGraph fluxGraph;
    protected UUID uuid;
    protected Object graphId;

    protected FluxElement(final FluxGraph fluxGraph, final Database database) {
        this.database = database;
        this.fluxGraph = fluxGraph;
        // UUID used to retrieve the actual datomic id later on
        uuid = Peer.squuid();
        graphId = Peer.tempid(":graph");
    }

    @Override
    public UUID getId() {
        return uuid;
    }

    @Override
    public Object getTimeId() {
        return FluxUtil.getActualTimeId(getDatabase(), this);
    }

    @Override
    public boolean isCurrentVersion() {
        return database == null;
    }

    @Override
    public boolean isDeleted() {
        // An element is deleted if we can no longer find any reference to it in the current version of the graph
        Collection<List<Object>> found = (Peer.q("[:find ?id " +
                                                  ":in $ ?id " +
                                                  ":where [?id _ _ ] ]", getDatabase(), graphId));
        return found.isEmpty();
    }

    @Override
    public Set<String> getPropertyKeys() {
        if (isDeleted()) {
            throw new IllegalArgumentException("It is not possible to get properties on a deleted element");
        }
        Set<String> finalProperties = new HashSet<String>();
        for(String property : getDatabase().entity(graphId).keySet()) {
            if (!FluxUtil.isReservedKey(property.toString())) {
                finalProperties.add(FluxUtil.getPropertyName(property));
            }
        }
        return finalProperties;
    }

    @Override
    public <T> T getProperty(final String key) {
//        if (isDeleted()) {
//            throw new IllegalArgumentException("It is not possible to get properties on a deleted element");
//        }
        if (!FluxUtil.isReservedKey(key)) {
            // We need to iterate, as we don't know the exact type (although we ensured that only one attribute will have that name)
            for(String property : getDatabase().entity(graphId).keySet()) {
                String propertyname = FluxUtil.getPropertyName(property);
                if (key.equals(propertyname)) {
                    return (T)getDatabase().entity(graphId).get(property);
                }
            }
            // We didn't find the value
            return null;
        }
        else {
            return (T)getDatabase().entity(graphId).get(key);
        }
    }

    @Override
    public void setProperty(final String key, final Object value) {
        validate();
        if (key.equals(StringFactory.ID))
            throw ExceptionFactory.propertyKeyIdIsReserved();
        if (key.equals(StringFactory.LABEL))
            throw new IllegalArgumentException("Property key is reserved for all nodes and edges: " + StringFactory.LABEL);
        if (key.equals(StringFactory.EMPTY_STRING))
            throw ExceptionFactory.propertyKeyCanNotBeEmpty();
        // A user-defined property
        if (!FluxUtil.isReservedKey(key)) {
            // If the property does not exist yet, create the attribute if required and create the appropriate transaction
            if (getProperty(key) == null) {
                // We first need to create the new attribute on the fly
                FluxUtil.createAttributeDefinition(key, value.getClass(), this.getClass(), fluxGraph);
                fluxGraph.addToTransaction(uuid, Util.map(":db/id", graphId,
                        FluxUtil.createKey(key, value.getClass(), this.getClass()), value));
            }
            else {
                // Value types match, just perform an update
                if (getProperty(key).getClass().equals(value.getClass())) {
                    fluxGraph.addToTransaction(uuid, Util.map(":db/id", graphId,
                            FluxUtil.createKey(key, value.getClass(), this.getClass()), value));
                }
                // Value types do not match. Retract original fact and add new one
                else {
                    FluxUtil.createAttributeDefinition(key, value.getClass(), this.getClass(), fluxGraph);
                    fluxGraph.addToTransaction(uuid, Util.list(":db/retract", graphId,
                            FluxUtil.createKey(key, value.getClass(), this.getClass()), getProperty(key)));
                    fluxGraph.addToTransaction(uuid, Util.map(":db/id", graphId,
                            FluxUtil.createKey(key, value.getClass(), this.getClass()), value));
                }
            }
        }
        // A datomic graph specific property
        else {
            fluxGraph.addToTransaction(uuid, Util.map(":db/id", graphId, key, value));
        }

//        if ((Long)id >= 0L) {
//            fluxGraph.addTransactionInfo(this);
//        }
    }

    public Interval getTimeInterval() {
        DateTime startTime = new DateTime(FluxUtil.getTransactionDate(fluxGraph, getTimeId()));
        TimeAwareElement nextElement = this.getNextVersion();
        if (nextElement == null) {
            return new Interval(startTime, new DateTime(Long.MAX_VALUE));
        }
        else {
            DateTime stopTime = new DateTime(FluxUtil.getTransactionDate(fluxGraph, nextElement.getTimeId()));
            return new Interval(startTime, stopTime);
        }
    }

    @Override
    public <T> T removeProperty(final String key) {
        validate();
        Object oldvalue = getProperty(key);
        if (oldvalue != null) {
            if (!FluxUtil.isReservedKey(key)) {
                fluxGraph.addToTransaction(uuid, Util.list(":db/retract", graphId,
                                       FluxUtil.createKey(key, oldvalue.getClass(), this.getClass()), oldvalue));
            }
        }
//        if ((Long)id >= 0L) {
//            fluxGraph.addTransactionInfo(this);
//        }
        return (T)oldvalue;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FluxElement that = (FluxElement) o;
        if (uuid != null ? !uuid.equals(that.uuid) : that.uuid != null) return false;
        return true;
    }

    @Override
    public int hashCode() {
        return uuid != null ? uuid.hashCode() : 0;
    }

    protected Database getDatabase() {
        return fluxGraph.dbWithTx();
    }

    private void validate() {
        if (!isCurrentVersion()) {
            throw new IllegalArgumentException("It is not possible to set a property on a non-current version of the element");
        }
//        if (isDeleted()) {
//            throw new IllegalArgumentException("It is not possible to set a property on a deleted element");
//        }
    }

    // Creates a collection containing the set of datomic facts describing this entity
    protected Set<Object> getFacts() {
        // Create the set of facts
        Set<Object> theFacts = new HashSet<Object>();
        // Get the entity
        Entity entity = getDatabase().entity(graphId);
        // Add the base facts associated with this edge
        for (String property : entity.keySet()) {
            // Add all properties (except the ident property (is only originally used for retrieving the id of the created elements)
            if (!property.equals(":db/ident")) {
                theFacts.add(FluxUtil.map(":db/id", graphId, property, entity.get(property).toString()));
            }
        }
        return theFacts;
    }

}