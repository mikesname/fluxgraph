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
    private final Map<FluxElement,Object> revMap
            = Maps.newHashMap();

    public IdResolver() {

    }

    public void put(Object tempId, FluxElement element) {
        dirty.put(tempId, element);
        revMap.put(element, tempId);
    }

    public void remove(Object id) {
        FluxElement element = dirty.get(id);
        if (element != null) {
            revMap.remove(element);
        }
    }

    public void removeElement(FluxElement element) {
        Object o = revMap.get(element);
        if (o != null) {
            dirty.remove(revMap.get(o));
        }
    }

    public void resolveIds(Database database, Map tempIds) {
        for (Map.Entry<Object,FluxElement> entry : dirty.entrySet()) {
            Object id = Peer.resolveTempid(database, tempIds, entry.getKey());
            entry.getValue().graphId = id;
        }
        dirty.clear();
        revMap.clear();
    }

    public void clear() {
        dirty.clear();
        revMap.clear();
    }
}
