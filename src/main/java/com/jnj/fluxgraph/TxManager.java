package com.jnj.fluxgraph;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;

import java.util.List;
import java.util.UUID;

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
public final class TxManager {

    private static enum  OpType {
        add,
        mod,
        del
    }

    private static class Op {
        public final UUID uuid;
        public final OpType opType;
        public final Object statement;
        public Op(UUID uuid, OpType opType, Object statement) {
            this.uuid = uuid;
            this.opType = opType;
            this.statement = statement;
        }
    }

    private final List<Op> operations;

    public TxManager() {
        operations = Lists.newArrayList();
    }

    public List<Object> getOps() {
        return Lists.newArrayList(
                Iterables.transform(operations, new Function<Op, Object>() {
                    @Override
                    public Object apply(Op op) {
                        return op.statement;
                    }
                }));
    }

    public boolean isAdded(final UUID uuid) {
        for (Op op : operations) {
            if (op.uuid.equals(uuid) && op.opType == OpType.add) {
                return true;
            }
        }
        return false;
    }

    public void addAddOp(UUID uuid, Object statement) {
        operations.add(new Op(uuid, OpType.add, statement));
    }

    public void addModOp(UUID uuid, Object statement) {
        operations.add(new Op(uuid, OpType.mod, statement));
    }

    public void addDelOp(UUID uuid, Object statement) {
        List<Op> filteredOps = Lists.newArrayList();
        boolean expunge = false;
        for (Op op : operations) {
            if (uuid.equals(op.uuid) && op.opType == OpType.add) {
                expunge = true;
            }

            if (!(expunge && op.uuid.equals(uuid))) {
                filteredOps.add(op);
            }
        }
        if (expunge) {
            operations.clear();
            operations.addAll(filteredOps);
        } else {
            operations.add(new Op(uuid, OpType.del, statement));
        }
    }
}
