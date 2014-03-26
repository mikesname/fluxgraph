package com.jnj.fluxgraph;

import clojure.lang.Keyword;
import com.google.common.base.Optional;
import com.google.common.collect.Sets;
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

    protected final Optional<Database> database;
    protected final FluxGraph fluxGraph;
    protected UUID uuid;
    protected Object graphId;

    protected FluxElement(final FluxGraph fluxGraph, final Optional<Database> database) {
        this(fluxGraph, database, Peer.squuid(), Peer.tempid(":graph"));
    }

    protected FluxElement(final FluxGraph fluxGraph, final Optional<Database> database, UUID uuid, Object graphId) {
        this.database = database;
        this.fluxGraph = fluxGraph;
        // UUID used to retrieve the actual datomic id later on
        this.uuid = uuid;
        this.graphId = graphId;
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
        return database.isPresent();
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
//        if (isDeleted()) {
//            throw new IllegalArgumentException("It is not possible to get properties on a deleted element");
//        }
//        Set<String> finalProperties = new HashSet<String>();
//        for(String property : getDatabase().entity(graphId).keySet()) {
//            if (!FluxUtil.isReservedKey(property.toString())) {
//                finalProperties.add(FluxUtil.getPropertyName(property));
//            }
//        }
//        return finalProperties;

        TxManager txManager = fluxGraph.getTxManager();
        if (txManager.isAdded(uuid)) {
            Set<String> finalProperties = Sets.newHashSet();
            for (String key : txManager.getPropertyKeys(uuid)) {
                if (!FluxUtil.isReservedKey(key)) {
                    Optional<String> propertyName = FluxUtil.getPropertyName(key);
                    if (propertyName.isPresent()) {
                        finalProperties.add(propertyName.get());
                    }
                }
            }
            return finalProperties;
        } else {
            return fluxGraph.getHelper().getPropertyKeysByUuid(uuid);
        }
    }

    @Override
    public <T> T getProperty(final String key) {

        TxManager txManager = fluxGraph.getTxManager();
        if (txManager.isAdded(uuid)) {
            Map data = txManager.getData(uuid);
            for (Object dataKey : data.keySet()) {
                Optional<String> propertyName = FluxUtil.getPropertyName(dataKey.toString());
                if (propertyName.isPresent()) {
                    if (propertyName.get().equals(key)) {
                        return (T)data.get(dataKey);
                    }
                }
            }
            return null;
        } else {
            return (T)fluxGraph.getHelper().getPropertyByUuid(uuid, key);
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

        TxManager txManager = fluxGraph.getTxManager();

        // A user-defined property
        if (!FluxUtil.isReservedKey(key)) {
            // If the property does not exist yet, create the attribute if required and create the appropriate transaction
            Keyword propKeyword = FluxUtil.createKey(key, value.getClass(), this.getClass());

            boolean isAdded = txManager.isAdded(uuid);
            if (isAdded) {
                FluxUtil.createAttributeDefinition(key, value.getClass(), this.getClass(), fluxGraph);
                txManager.setProperty(uuid, propKeyword, value);
            } else {
                // If there is not value, or value types match, just perform an update
                Object existingValue = getProperty(key);
                if (existingValue == null || existingValue.getClass().equals(value.getClass())) {
                    txManager.mod(uuid, Util.map(":db/id", graphId, propKeyword, value));
                } else {
                    FluxUtil.createAttributeDefinition(key, value.getClass(), this.getClass(), fluxGraph);
                    txManager.mod(uuid, Util.list(":db/retract", graphId, propKeyword, existingValue));
                    txManager.mod(uuid, Util.map(":db/id", graphId, propKeyword, value));
                }

            }
        }
        // A datomic graph specific property
        else {
            txManager.mod(uuid, Util.map(":db/id", graphId, key, value));
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

                Keyword propKeyword = FluxUtil.createKey(key, oldvalue.getClass(), this.getClass());

                TxManager txManager = fluxGraph.getTxManager();

                if (txManager.isAdded(uuid)) {
                    txManager.removeProperty(uuid, propKeyword);
                } else {
                    txManager.mod(uuid, Util.list(":db/retract", graphId, propKeyword, oldvalue));
                }
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
        return !(uuid != null ? !uuid.equals(that.uuid) : that.uuid != null);
    }

    @Override
    public int hashCode() {
        return uuid != null ? uuid.hashCode() : 0;
    }

    protected Database getDatabase() {
        return database.isPresent()
                ? database.get()
                : fluxGraph.dbWithTx();
    }

    private void validate() {
//        if (!isCurrentVersion()) {
//            throw new IllegalArgumentException("It is not possible to set a property on a non-current version of the element");
//        }
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