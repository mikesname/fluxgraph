package com.jnj.fluxgraph;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

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
        public final UUID[] touched;
        public Op(UUID uuid, OpType opType, Object statement, UUID... touched) {
            this.uuid = uuid;
            this.opType = opType;
            this.statement = statement;
            this.touched = touched;
        }
        public boolean concerns(UUID uuid) {
            if (this.uuid.equals(uuid)) {
                return true;
            } else {
                for (UUID t : touched) {
                    if (t.equals(uuid)) {
                        return true;
                    }
                }
            }
            return false;
        }
    }

    private final List<Op> operations;

    public TxManager() {
        operations = Lists.newArrayList();
    }

    public List<Object> ops() {
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

    public void add(UUID uuid, Object statement, UUID... touched) {
        operations.add(new Op(uuid, OpType.add, statement, touched));
    }

    public void mod(UUID uuid, Object statement) {
        operations.add(new Op(uuid, OpType.mod, statement));
    }

    public void del(UUID uuid, Object statement) {
        List<Op> filteredOps = Lists.newArrayList();
        boolean expunge = false;
        for (Op op : operations) {
            if (op.concerns(uuid) && op.opType == OpType.add) {
                expunge = true;
            }

            if (!(expunge && op.concerns(uuid))) {
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
