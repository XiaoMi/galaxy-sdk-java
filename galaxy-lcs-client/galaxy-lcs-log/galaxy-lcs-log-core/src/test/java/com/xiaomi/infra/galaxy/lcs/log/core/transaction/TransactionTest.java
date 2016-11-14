/**
 * Copyright 2016, Xiaomi.
 * All rights reserved.
 * Author: xiajun@xiaomi.com
 */

package com.xiaomi.infra.galaxy.lcs.log.core.transaction;

import org.junit.Before;
import org.junit.Test;

public class TransactionTest {
  private class TestTransaction extends Transaction<String> {
    @Override
    protected void doInitTransaction() {

    }

    @Override
    protected void doStartTransaction() {

    }

    @Override
    protected String doTake() {
      return null;
    }

    @Override
    protected void doCommitTransaction() {

    }

    @Override
    protected void doRollbackTransaction() {

    }

    @Override
    protected void doCloseTransaction() {

    }
  }

  private static TestTransaction transaction;

  @Before
  public void setUp() throws Exception {
    transaction = new TestTransaction();
  }

  @Test
  public void testTransaction() throws Exception {
    transaction.initTransaction();
    transaction.closeTransaction();
  }

  @Test
  public void testTransaction1() throws Exception {
    // commit && commit
    transaction.initTransaction();
    transaction.startTransaction();
    transaction.take();
    transaction.commitTransaction();
    transaction.startTransaction();
    transaction.take();
    transaction.commitTransaction();
    transaction.closeTransaction();
  }

  @Test
  public void testTransaction2() throws Exception {
    // rollback && rollback
    transaction.initTransaction();
    transaction.startTransaction();
    transaction.take();
    transaction.rollbackTransaction();
    transaction.startTransaction();
    transaction.take();
    transaction.rollbackTransaction();
    transaction.closeTransaction();
  }

  @Test
  public void testTransaction3() throws Exception {
    // commit && rollback
    transaction.initTransaction();
    transaction.startTransaction();
    transaction.take();
    transaction.commitTransaction();
    transaction.startTransaction();
    transaction.take();
    transaction.rollbackTransaction();
    transaction.closeTransaction();
  }

  @Test
  public void testTransaction4() throws Exception {
    // rollback && commit
    transaction.initTransaction();
    transaction.startTransaction();
    transaction.take();
    transaction.rollbackTransaction();
    transaction.startTransaction();
    transaction.take();
    transaction.commitTransaction();
    transaction.closeTransaction();
  }

  @Test (expected = RuntimeException.class)
  public void testTransactionWithoutInitAndStart() throws Exception {
    transaction.startTransaction();
  }

  @Test (expected = RuntimeException.class)
  public void testTransactionWithoutInitAndClose() throws Exception {
    transaction.closeTransaction();
  }

  @Test (expected = RuntimeException.class)
  public void testTransactionWithoutStartAndTake() throws Exception {
    transaction.initTransaction();
    transaction.take();
  }

  @Test (expected = RuntimeException.class)
  public void testTransactionWithoutStartAndCommit() throws Exception {
    transaction.initTransaction();
    transaction.commitTransaction();
  }

  @Test (expected = RuntimeException.class)
  public void testTransactionWithoutStartAndRollback() throws Exception {
    transaction.initTransaction();
    transaction.rollbackTransaction();
  }

  @Test (expected = RuntimeException.class)
  public void testTransactionWithoutTakeAndCommit() throws Exception {
    transaction.initTransaction();
    transaction.startTransaction();
    transaction.commitTransaction();
  }

  @Test (expected = RuntimeException.class)
  public void testTransactionWithoutTakeAndRollback() throws Exception {
    transaction.initTransaction();
    transaction.startTransaction();
    transaction.rollbackTransaction();
  }

  @Test (expected = RuntimeException.class)
  public void testTransactionWithoutTakeAndClose() throws Exception {
    transaction.initTransaction();
    transaction.startTransaction();
    transaction.closeTransaction();
  }

  @Test (expected = RuntimeException.class)
  public void testTransactionWithoutCommit() throws Exception {
    transaction.initTransaction();
    transaction.startTransaction();
    transaction.take();
    transaction.startTransaction();
  }

  @Test (expected = RuntimeException.class)
  public void testTransactionWithoutCommitAndClose() throws Exception {
    transaction.initTransaction();
    transaction.startTransaction();
    transaction.take();
    transaction.closeTransaction();
  }
}
