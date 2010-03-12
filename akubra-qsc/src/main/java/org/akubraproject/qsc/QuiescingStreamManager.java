/* $HeadURL$
 * $Id$
 *
 * Copyright (c) 2009-2010 DuraSpace
 * http://duraspace.org
 *
 * In collaboration with Topaz Inc.
 * http://www.topazproject.org
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.akubraproject.qsc;

import java.io.IOException;
import java.io.OutputStream;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import javax.transaction.Synchronization;
import javax.transaction.Transaction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.akubraproject.BlobStoreConnection;
import org.akubraproject.impl.StreamManager;

/**
 * Utility class that tracks the open streams of a <code>BlobStore</code> in order to provide a
 * <code>setQuiescent</code> implementation with the correct blocking behavior as well as to
 * ensure that streams that belong to a <code>BlobStoreConnection</code> are closed when the
 * connection is closed.
 *
 * @author Chris Wilper
 * @author Ronald Tschalär
 */
class QuiescingStreamManager extends StreamManager {
  private static final Logger log = LoggerFactory.getLogger(QuiescingStreamManager.class);

  /** Exclusive lock on the quiescent state. */
  private final ReentrantLock stateLock = new ReentrantLock(true);

  /** Used to await and signal when quiescent state changes to false. */
  private final Condition becameUnquiescent = stateLock.newCondition();

  /** The current quiescent state. */
  private boolean quiescent;

  /** the list of open, transactional connections */
  private final Set<QuiescingBlobStoreConnection> txnCons = new HashSet<QuiescingBlobStoreConnection>();

  /** the list of open, non-transactional connections */
  private final Set<QuiescingBlobStoreConnection> rawCons = new HashSet<QuiescingBlobStoreConnection>();

  void register(final QuiescingBlobStoreConnection con, Transaction tx) throws IOException {
    Set<QuiescingBlobStoreConnection> cons;

    if (tx != null) {
      try {
        tx.registerSynchronization(new Synchronization() {
          public void beforeCompletion() {
          }

          public void afterCompletion(int status) {
            unregister(con, true);
          }
        });
      } catch (Exception e) {
        throw new IOException("Error registering txn synchronization", e);
      }

      cons = txnCons;
    } else {
      cons = rawCons;
    }

    synchronized (cons) {
      cons.add(con);
    }
  }

  void unregister(QuiescingBlobStoreConnection con, boolean transactional) {
    Set<QuiescingBlobStoreConnection> cons = transactional ? txnCons : rawCons;
    synchronized (cons) {
      if (cons.remove(con))
        cons.notifyAll();
    }
  }

  /**
   * Acquires the state lock in an unquiescent state.
   * <p>
   * This causes the calling thread to block until the unquiescent state is
   * reached.  When obtained, the caller is responsible for releasing the state
   * lock as soon as possible.
   *
   * @see #unlockState
   */
  public void lockUnquiesced() throws IOException {
    boolean ok = false;

    try {
      stateLock.lockInterruptibly();

      while (quiescent) {
        log.info("lockUnquiesced: Waiting...", new Throwable());
        becameUnquiescent.await();
        log.info("lockUnquiesced: Wait is over.");
      }

      log.debug("Aquired the unquiescent lock");
      ok = true;
    } catch (InterruptedException ie) {
      throw new IOException("lockUnquiesced: interrupted", ie);
    } finally {
      if (!ok && stateLock.isHeldByCurrentThread())
        stateLock.unlock();
    }
  }

  /**
   * Releases the lock previously obtained via {@link #lockUnquiesced}.
   */
  public void unlockState() {
    stateLock.unlock();
    log.debug("Released the unquiescent lock");
  }

  /**
   * Sets the quiescent state.
   *
   * <p>Note that setting to the current state has no effect.
   *
   * @param quiescent whether to go into the quiescent (true) or non-quiescent (false) state.
   * @return true if successful, false if the thread was interrupted while blocking.
   * @see org.akubraproject.qsc.QuiescingBlobStore#setQuiescent
   */
  public boolean setQuiescent(boolean quiescent) throws IOException {
    try {
      stateLock.lockInterruptibly();

      if (quiescent && !this.quiescent) {
        synchronized (openOutputStreams) {
          while (!openOutputStreams.isEmpty()) {
            log.info("setQuiescent: Waiting for " + openOutputStreams.size() +
                     " output streams to close...");
            openOutputStreams.wait(); // wake up when next one is closed
          }
        }

        synchronized (txnCons) {
          int cnt;
          while ((cnt = countWriteTransactions()) > 0) {
            log.info("setQuiescent: Waiting for " + cnt + " write transactions to close...");
            txnCons.wait(); // wake up when next one is completed
          }
        }

        synchronized (rawCons) {
          for (QuiescingBlobStoreConnection con : rawCons) {
            if (con.hasModifications())
              con.sync();
          }
        }

        log.info("setQuiescent: No open output streams or active write transactions. " +
                 "Entering quiescent state.");
      }

      if (!quiescent && this.quiescent) {
        log.info("setQuiescent: Exiting quiescent state.");
        becameUnquiescent.signalAll();
      }

      this.quiescent = quiescent;
      return true;
    } catch (InterruptedException ie) {
      if (quiescent)
        log.warn("Interrupted while waiting to enter quiescent state", ie);
      else
        log.warn("Interrupted while waiting to exit quiescent state", ie);
      return false;
    } finally {
      if (stateLock.isHeldByCurrentThread())
        stateLock.unlock();
    }
  }

  /* Runs with both txnCons monitor held as well as stateLock held */
  private int countWriteTransactions() {
    int cnt = 0;

    for (QuiescingBlobStoreConnection con : txnCons) {
      if (con.hasModifications())
        cnt++;
    }

    return cnt;
  }

  @Override
  public OutputStream manageOutputStream(BlobStoreConnection con, OutputStream stream)
      throws IOException {
    lockIOE();
    try {
      return super.manageOutputStream(con, stream);
    } finally {
      stateLock.unlock();
    }
  }

  private void lockIOE() throws IOException {
    try {
      stateLock.lockInterruptibly();
    } catch (InterruptedException ie) {
      throw new IOException("Wait for state-lock interrupted", ie);
    }
  }

  // are we in the quiescent state? (for testability)
  boolean isQuiescent() {
    stateLock.lock();
    try {
      return quiescent;
    } finally {
      stateLock.unlock();
    }
  }
}
