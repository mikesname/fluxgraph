package com.jnj.fluxgraph;

import clojure.lang.Keyword;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import datomic.Util;

import java.util.*;

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

    private static final UUID GLOBAL_OP = UUID.randomUUID();

    private static enum  OpType {
        add,
        del,
        mod,
        global
    }

    private static class Op {
        public final OpType opType;
        public Object statement;
        public final UUID[] touched;
        public Op(OpType opType, Object statement, UUID... touched) {
            this.opType = opType;
            this.statement = statement;
            this.touched = touched;
        }
        public boolean concerns(UUID uuid) {
            for (UUID t : touched) {
                if (t.equals(uuid)) {
                    return true;
                }
            }
            return false;
        }
    }

    private LinkedHashMap<UUID, Op> operations;

    public TxManager() {
        operations = Maps.newLinkedHashMap();
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

    public boolean isAdded(final UUID uuid) {
        Op op = operations.get(uuid);
        return op != null && op.opType == OpType.add;
    }

    public void add(UUID uuid, Object statement, UUID... touched) {
        operations.put(uuid, new Op(OpType.add, statement, touched));
    }

    public void mod(UUID uuid, Object statement) {
        operations.put(uuid, new Op(OpType.mod, statement));
    }

    public void del(UUID uuid, Object statement) {
        if (isAdded(uuid)) {
            operations.remove(uuid);
            LinkedHashMap<UUID,Op> newMap = Maps.newLinkedHashMap();
            for (Map.Entry<UUID,Op> entry : operations.entrySet()) {
                if (!entry.getValue().concerns(uuid)) {
                    newMap.put(entry.getKey(), entry.getValue());
                }
            }
            operations = newMap;
        } else {
            operations.put(uuid, new Op(OpType.del, statement));
        }
    }

    public void remove(UUID uuid) {
        if (isAdded(uuid)) {
            operations.remove(uuid);
            LinkedHashMap<UUID,Op> newMap = Maps.newLinkedHashMap();
            for (Map.Entry<UUID,Op> entry : operations.entrySet()) {
                if (!entry.getValue().concerns(uuid)) {
                    newMap.put(entry.getKey(), entry.getValue());
                }
            }
            operations = newMap;
        } else {
            throw new IllegalArgumentException("Item is not added in current TX: " + uuid);
        }
    }

    public void setProperty(UUID uuid, Keyword key, Object value) {
        if (isAdded(uuid)) {
            insertIntoStatement(operations.get(uuid), key, value);
        } else {
            throw new IllegalArgumentException("Item is not added in current TX: " + uuid);
        }
    }

    public void removeProperty(UUID uuid, Keyword key) {
        if (isAdded(uuid)) {
            removeFromStatement(operations.get(uuid), key);
        } else {
            throw new IllegalArgumentException("Item is not added in current TX: " + uuid);
        }
    }

    public Object getProperty(UUID uuid, Keyword key) {
        if (isAdded(uuid)) {
            return getStatementMap(operations.get(uuid)).get(key);
        }
        throw new IllegalArgumentException("Item is not added in current TX: " + uuid);
    }

    public Set<String> getPropertyKeys(UUID uuid) {
        if (isAdded(uuid)) {
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

    public Map getData(UUID uuid) {
        if (isAdded(uuid)) {
            return getStatementMap(operations.get(uuid));
        }
        throw new IllegalArgumentException("Item is not added in current TX: " + uuid);
    }

    public void global(Object statement) {
        operations.put(GLOBAL_OP, new Op(OpType.global, statement));
    }

    public void flush() {
        operations.clear();
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
}
