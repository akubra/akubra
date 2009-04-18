/* $HeadURL$
 * $Id$
 *
 * Copyright (c) 2008,2009 by Fedora Commons Inc.
 * http://www.fedoracommons.org
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
package org.fedoracommons.akubra.impl;

import java.io.Closeable;
import java.io.InputStream;
import java.io.OutputStream;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.commons.io.IOUtils;

import org.fedoracommons.akubra.BlobStoreConnection;

/**
 * Utility class that tracks the open streams of a <code>BlobStore</code> in order to provide a
 * <code>setQuiescent</code> implementation with the correct blocking behavior as well as to
 * ensure that streams that belong to a <code>BlobStoreConnection</code> are closed when the
 * connection is closed.
 *
 * @author Chris Wilper
 */
public class StreamManager {
  private static final Log log = LogFactory.getLog(StreamManager.class);

  /** Exclusive lock on the quiescent state. */
  private final ReentrantLock stateLock = new ReentrantLock(true);

  /** Used to await and signal when quiescent state changes to false. */
  private final Condition becameUnquiescent = stateLock.newCondition();

  /** Listens to close events. */
  private final CloseListener listener;

  /** The set of open <code>OutputStream</code>s managed by this instance. */
  private final Set<ManagedOutputStream> openOutputStreams
      = Collections.synchronizedSet(new HashSet<ManagedOutputStream>());

  /** The set of open <code>InputStream</code>s managed by this instance. */
  private final Set<ManagedInputStream> openInputStreams
      = Collections.synchronizedSet(new HashSet<ManagedInputStream>());

  /** The current quiescent state. */
  private boolean quiescent;

  /**
   * Creates an instance.
   */
  public StreamManager() {
    listener = new CloseListener() {
      public void notifyClosed(Closeable closeable) {
        if (closeable instanceof InputStream) {
          openInputStreams.remove(closeable);
        } else {
          synchronized (openOutputStreams) {
            openOutputStreams.remove(closeable);
            openOutputStreams.notify();
          }
        }
      }
    };
  }

  /**
   * Acquires the state lock in an unquiescent state.
   * <p>
   * This causes the calling thread to block until the unquiescent state is
   * reached.  When obtained, the caller is responsible for releasing the state
   * lock as soon as possible.
   *
   * @return <code>true</code> if successful, or <code>false</code> if the
   *     current thread is interrupted while waiting for the lock.
   * @see #unlockState
   */
  public boolean lockUnquiesced() {
    try {
      stateLock.lockInterruptibly();
      if (quiescent) {
        log.info("lockUnquiesced: Waiting...");
        becameUnquiescent.await();
        log.info("lockUnquiesced: Wait is over.");
      }
      log.debug("Aquired the unquiescent lock");
      return true;
    } catch (InterruptedException e) {
      if (stateLock.isHeldByCurrentThread())
        stateLock.unlock();
      return false;
    }
  }

  /**
   * Releases the lock previously obtained via <code>lockUnquiesced</code>.
   *
   * @see #lockUnquiesced
   */
  public void unlockState() {
    stateLock.unlock();
    log.debug("Released the unquiescent lock");
  }

  /**
   * Sets the quiescent state.
   *
   * Note that setting to the current state has no effect.
   *
   * @param quiescent whether to go into the quiescent (true) or non-quiescent (false) state.
   * @return true if successful, false if the thread was interrupted while blocking.
   * @see org.fedoracommons.akubra.BlobStore#setQuiescent
   */
  public boolean setQuiescent(boolean quiescent) {
    try {
      stateLock.lockInterruptibly();
      if (quiescent && !this.quiescent) {
        synchronized (openOutputStreams) {
          while (!openOutputStreams.isEmpty()) {
            log.info("setQuiescent: Waiting for " + openOutputStreams.size() + " output streams to close...");
            openOutputStreams.wait(); // wake up when next one is closed
          }
        }
        log.info("setQuiescent: No open output streams. Entering quiescent state.");
      }
      if (!quiescent && this.quiescent) {
        log.info("setQuiescent: Exiting quiescent state.");
        becameUnquiescent.signal();
      }
      this.quiescent = quiescent;
      return true;
    } catch (InterruptedException e) {
      return false;
    } finally {
      if (stateLock.isHeldByCurrentThread())
        stateLock.unlock();
    }
  }

  /**
   * Provides a tracked wrapper around a given OutputStream. The current thread must own the
   * stateLock or an exception will be thrown.
   * <p>
   * Callers should generally get a managed OutputStream in the following way:
   * <pre>
   * if (!streamManager.lockUnquiesced()) { // obtain the stateLock
   *   throw new IOException("Interrupted waiting for writable state");
   * }
   * try {
   *   stream = ...
   *   return streamManager.manageOutputStream(getConnection(), stream);
   * } finally {
   *   streamManager.unlockState();
   * }
   * </pre>
   *
   * @param con the connection from which the stream originated.
   * @param stream the stream to wrap.
   * @return the wrapped version of the stream.
   * @throws IllegalStateException if the state lock is not held by the current thread.
   */
  public OutputStream manageOutputStream(BlobStoreConnection con, OutputStream stream) {
    if (!stateLock.isHeldByCurrentThread())
      throw new IllegalStateException("State lock not held by current thread");
    ManagedOutputStream managed = new ManagedOutputStream(listener, stream, con);
    openOutputStreams.add(managed);
    return managed;
  }

  /**
   * Provides a tracked wrapper around a given InputStream.
   *
   * @param con the connection that trac.
   * @param stream the stream to wrap.
   * @return the wrapped version of the stream.
   */
  public InputStream manageInputStream(BlobStoreConnection con, InputStream stream) {
    ManagedInputStream managed = new ManagedInputStream(listener, stream, con);
    openInputStreams.add(managed);
    return managed;
  }

  /**
   * Notification that a connection is closed. All its open streams are closed.
   *
   * @param con the connection that is closed
   */
  public void connectionClosed(BlobStoreConnection con) {
    Set<Closeable> closeables = new HashSet<Closeable>();
    synchronized (openOutputStreams) {
      for (ManagedOutputStream c : openOutputStreams)
        if (c.getConnection().equals(con))
          closeables.add(c);
    }

    synchronized (openInputStreams) {
      for (ManagedInputStream c : openInputStreams)
        if (c.getConnection().equals(con))
          closeables.add(c);
    }

    if (!closeables.isEmpty()) {
      log.warn("Auto-closing " + closeables.size() + " open streams for closed connection " + con);
      for (Closeable c : closeables) {
        if (c instanceof InputStream)
          IOUtils.closeQuietly((InputStream) c);
        else
          IOUtils.closeQuietly((OutputStream) c);
      }
    }
  }

  // how many output streams are open?
  int getOpenOutputStreamCount() {
    return openOutputStreams.size();
  }

  // how many input streams are open?
  int getOpenInputStreamCount() {
    return openInputStreams.size();
  }

  // are we in the quiescent state?
  boolean isQuiescent() {
    stateLock.lock();
    try {
        return quiescent;
    } finally {
      stateLock.unlock();
    }
  }
}
