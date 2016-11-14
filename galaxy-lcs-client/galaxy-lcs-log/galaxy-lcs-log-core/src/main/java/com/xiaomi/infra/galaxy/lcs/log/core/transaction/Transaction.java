/**
 * Copyright 2016, Xiaomi.
 * All rights reserved.
 * Author: xiajun@xiaomi.com
 */

package com.xiaomi.infra.galaxy.lcs.log.core.transaction;

import java.util.concurrent.atomic.AtomicReference;

public abstract class Transaction<T> {
  private enum TransactionState {
    /**
     * NEW state
     */
    NEW,

    /**
     * Init
     */
    INIT,

    /**
     * You are ready for transaction;
     */
    READY,

    /**
     * You are already in transaction state;
     */
    STARTED,

    /**
     * You have finished transaction, that is you can call startTransaction;
     */
    STOPED,

    /**
     * Transaction is in close, that is you can not call startTransaction anymore.
     */
    CLOSED,
  }

  private AtomicReference<TransactionState> transactionState;
  private final Object transactionLock;

  protected Transaction() {
    transactionState = new AtomicReference<TransactionState>(TransactionState.NEW);
    transactionLock = new Object();
  }

  public void initTransaction() {
    synchronized (transactionLock) {
      if (!transactionState.compareAndSet(TransactionState.NEW, TransactionState.INIT)) {
        throw new RuntimeException("You should first call init for transaction");
      }

      doInitTransaction();
    }
  }

  public void startTransaction() {
    synchronized (transactionLock) {
      if (!transactionState.compareAndSet(TransactionState.STOPED, TransactionState.READY) &&
          !transactionState.compareAndSet(TransactionState.INIT, TransactionState.READY)) {
        throw new RuntimeException("We are already in transaction state");
      }

      doStartTransaction();
    }
  }

  public T take() {
    synchronized (transactionLock) {
      if (!transactionState.compareAndSet(TransactionState.READY, TransactionState.STARTED)) {
        throw new RuntimeException("We are no in transaction state");
      }

      return doTake();
    }
  }

  public void commitTransaction() {
    synchronized (transactionLock) {
      if (!transactionState.compareAndSet(TransactionState.STARTED, TransactionState.STOPED)) {
        throw new RuntimeException("We are no in transaction state");
      }

      doCommitTransaction();
    }
  }

  public void rollbackTransaction() {
    synchronized (transactionLock) {
      if (!transactionState.compareAndSet(TransactionState.STARTED, TransactionState.STOPED)) {
        throw new RuntimeException("We are no in transaction state");
      }

      doRollbackTransaction();
    }

  }

  public void closeTransaction() {
    synchronized (transactionLock) {
      if (!transactionState.compareAndSet(TransactionState.STOPED, TransactionState.CLOSED) &&
          !transactionState.compareAndSet(TransactionState.INIT, TransactionState.CLOSED)) {
        throw new RuntimeException("We are in transaction state, can not close this transaction");
      }

      doCloseTransaction();
    }
  }

  abstract protected void doInitTransaction();

  abstract protected void doStartTransaction();

  abstract protected T doTake();

  abstract protected void doCommitTransaction();

  abstract protected void doRollbackTransaction();

  abstract protected void doCloseTransaction();
}
