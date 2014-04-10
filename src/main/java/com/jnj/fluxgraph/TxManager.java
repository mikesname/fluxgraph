package com.jnj.fluxgraph;

import clojure.lang.Keyword;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import datomic.Connection;
import datomic.Database;
import datomic.Peer;

import java.util.*;
import java.util.concurrent.ExecutionException;

import static datomic.Connection.DB_AFTER;
import static datomic.Connection.TEMPIDS;

/**
 * Helper class for managing the mismatch between Blueprints and
 * Datomic transactional semantics. Basically this holds a list
 * of pending additions and retractions to the database in the form
 * of Datomic statements. However, statements in a pending Datomic
 * transaction can not be "contradictory", that is, you can't add and
 * retract a fact in the same operation.
 *
 * Blueprints, however, does allow adding and removing edges, vertices,
 * and properties in the same transaction, so we need to manage the set
 * of pending statements in such a way that the pending queue does not
 * contain a contradiction. Basically,
 *
 * @author Mike Bryant (http://github.com/mikesname)
 */
final class TxManager {

    private static enum  OpType {
        add,
        del,
        mod
    }

    private static class Op {
        public final OpType opType;
        public Object statement;
        public final FluxElement[] touched;
        public Op(OpType opType, Object statement, FluxElement... touched) {
            this.opType = opType;
            this.statement = statement;
            this.touched = touched;
        }
        public boolean concerns(FluxElement element) {
            for (FluxElement t : touched) {
                if (t.equals(element)) {
                    return true;
                }
            }
            return false;
        }
    }

    private Connection connection;

    // List of pending graph operations
    private LinkedHashMap<FluxElement, Op> operations;
    // List of dirty elements that need their temp IDs resolved
    // to permanent graph IDs following a commit.
    private final Map<Object,FluxElement> dirty
            = Maps.newHashMap();
    // Reverse lookup of dirty IDs
    private final Map<FluxElement,Object> revMap
            = Maps.newHashMap();


    private Database database = null;

    public TxManager(Connection connection) {
        this.connection = connection;
        operations = Maps.newLinkedHashMap();
    }

    public Database getDatabase() {
        if (database == null) {
            database = (Database)connection.db().with(ops()).get(Connection.DB_AFTER);
            return database;
        } else {
            return database;
        }
    }

    private void setDirty() {
        database = null;
    }

    private void appendOp(Object statement) {
//        this.database = (Database)database
//                .with(Util.list(statement)).get(Connection.DB_AFTER);
        setDirty();
    }

    public List<Object> ops() {
        return Lists.newArrayList(
                Sets.newLinkedHashSet(
                        Iterables.transform(operations.values(), new Function<Op, Object>() {
                            @Override
                            public Object apply(Op op) {
                                return op.statement;
                            }
                        })));
    }

    public boolean newInThisTx(final FluxElement uuid) {
        Op op = operations.get(uuid);
        return op != null && op.opType == OpType.add;
    }

    public void add(FluxElement element, Object statement, FluxElement... touched) {
        operations.put(element, new Op(OpType.add, statement, touched));
        dirty.put(element.graphId, element);
        revMap.put(element, element.graphId);
        appendOp(statement);
    }

    public void mod(FluxElement element, Object statement) {
        operations.put(element, new Op(OpType.mod, statement));
        appendOp(statement);
    }

    public void del(FluxElement element, Object statement) {
        if (newInThisTx(element)) {
            remove(element);
        } else {
            operations.put(element, new Op(OpType.del, statement));
            appendOp(statement);
        }
    }

    public void remove(FluxElement element) {
        operations.remove(element);
        Object o = revMap.get(element);
        if (o != null) {
            dirty.remove(o);
        }

        LinkedHashMap<FluxElement,Op> newMap = Maps.newLinkedHashMap();
        for (Map.Entry<FluxElement,Op> entry : operations.entrySet()) {
            if (!entry.getValue().concerns(element)) {
                newMap.put(entry.getKey(), entry.getValue());
            }
        }
        operations = newMap;
        setDirty();
    }

    public void setProperty(FluxElement uuid, Keyword key, Object value) {
        if (newInThisTx(uuid)) {
            insertIntoStatement(operations.get(uuid), key, value);
            setDirty();
        } else {
            throw new IllegalArgumentException("Item is not added in current TX: " + uuid);
        }
    }

    public void removeProperty(FluxElement uuid, Keyword key) {
        if (newInThisTx(uuid)) {
            removeFromStatement(operations.get(uuid), key);
            setDirty();
        } else {
            throw new IllegalArgumentException("Item is not added in current TX: " + uuid);
        }
    }

    public Set<String> getPropertyKeys(FluxElement uuid) {
        if (newInThisTx(uuid)) {
            Set<String> keys = Sets.newHashSet();
            for (Object item : getStatementMap(operations.get(uuid)).keySet()) {
                if (item instanceof String) {
                    keys.add((String)item);
                } else {
                    keys.add(item.toString());
                }
            }
            return keys;
        }
        throw new IllegalArgumentException("Item is not added in current TX: " + uuid);
    }

    public Map getData(FluxElement uuid) {
        if (newInThisTx(uuid)) {
            return getStatementMap(operations.get(uuid));
        }
        throw new IllegalArgumentException("Item is not added in current TX: " + uuid);
    }

    public void transact() {
        try {
            Map map = connection.transact(ops()).get();
            resolveIds((Database) map.get(DB_AFTER), (Map) map.get(TEMPIDS));
        } catch (InterruptedException e) {
            throw new RuntimeException(FluxGraph.DATOMIC_ERROR_EXCEPTION_MESSAGE, e);
        } catch (ExecutionException e) {
            throw new RuntimeException(FluxGraph.DATOMIC_ERROR_EXCEPTION_MESSAGE, e);
        } finally {
            flush();
        }
    }

    public void flush() {
        operations.clear();
        revMap.clear();
        dirty.clear();
        setDirty();
    }

    private void insertIntoStatement(Op op, Keyword key, Object value) {
        Map newMap = Maps.newHashMap(getStatementMap(op));
        newMap.put(key, value);
        op.statement = newMap;
    }

    private void removeFromStatement(Op op, Keyword key) {
        Map newMap = Maps.newHashMap(getStatementMap(op));
        newMap.remove(key);
        op.statement = newMap;
    }

    private Map getStatementMap(Op op) {
        if (op.statement instanceof Map) {
            return (Map)op.statement;
        }
        throw new IllegalArgumentException("Statement was not a map: " + op.statement);
    }

    public long size() {
        return operations.size();
    }

    private void resolveIds(Database database, Map tempIds) {
        for (Map.Entry<Object,FluxElement> entry : dirty.entrySet()) {
            entry.getValue().graphId
                    = Peer.resolveTempid(database, tempIds, entry.getKey());
        }
    }
}
