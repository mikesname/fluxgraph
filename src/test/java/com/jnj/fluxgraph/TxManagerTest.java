package com.jnj.fluxgraph;

import datomic.Util;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;


import java.util.UUID;

/**
 * @author Mike Bryant (http://github.com/mikesname)
 */
public class TxManagerTest {

    private UUID item1 = UUID.randomUUID();
    private UUID item2 = UUID.randomUUID();

    @Test
    public void testAdditionAndDeletion() {
        TxManager txManager = new TxManager();
        txManager.add(item1, Util.list("add"));
        txManager.del(item1, Util.list("del"));
        assertTrue(txManager.ops().isEmpty());
    }

    @Test
    public void testAdditionAndDeletionInterleaved() {
        TxManager txManager = new TxManager();
        assertFalse(txManager.ops().isEmpty());
    }

    @Test
    public void testAdditionAndDeletionWithTouched() {
        TxManager txManager = new TxManager();
        txManager.add(item1, Util.list("add"));
        txManager.add(item2, Util.list("add"), item1);
        txManager.del(item1, Util.list("del"));
        assertTrue(txManager.ops().isEmpty());
    }

    @Test
    public void testIsAdded() {
        TxManager txManager = new TxManager();
        txManager.add(item1, Util.list("add"));
        txManager.del(item1, Util.list("del"));
        assertEquals(1L, txManager.ops().size());

        txManager.add(item1, Util.list("add"));
        assertTrue(txManager.isAdded(item1));
        assertFalse(txManager.isAdded(item2));
    }
}
