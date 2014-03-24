package com.jnj.fluxgraph;

import com.google.common.collect.Maps;
import datomic.Database;
import datomic.Peer;

import java.util.Map;

/**
 * Class responsible for resolving temporary IDs to permanent
 * graph IDs when a transaction is committed.
 *
 * @author Mike Bryant (http://github.com/mikesname)
 */
final class IdResolver {
    private final Map<Object,FluxElement> dirty
            = Maps.newHashMap();

    public IdResolver() {

    }

    public void put(Object tempId, FluxElement element) {
        dirty.put(tempId, element);
    }

    public void resolveIds(Database database, Map tempIds) {
        for (Map.Entry<Object,FluxElement> entry : dirty.entrySet()) {
            entry.getValue().graphId = Peer.resolveTempid(database, tempIds, entry.getKey());
        }
        dirty.clear();
    }
}
