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
import java.util.concurrent.locks.ReentrantLock;

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

  /** Exclusive lock on the quiescent state. */
  private final ReentrantLock stateLock = new ReentrantLock(true);

  /** Listens to close events. */
  private final CloseListener listener;

  /** The set of open <code>OutputStream</code>s managed by this instance. */
  private Set<ManagedOutputStream> openStreams
      = Collections.synchronizedSet(new HashSet<ManagedOutputStream>());

  /** The set of open <code>InputStream</code>s managed by this instance. */
  private Set<ManagedInputStream> openInputStreams
      = Collections.synchronizedSet(new HashSet<ManagedInputStream>());

  /** The current quiescent state. */
  private boolean quiescent;

  /**
   * Creates an instance.
   */
  public StreamManager() {
    listener = new CloseListener() {
      public void notifyClosed(Closeable closeable) {
        if (closeable instanceof InputStream)
          openInputStreams.remove(closeable);
        else
          openStreams.remove(closeable);
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
      while (true) {
        stateLock.lockInterruptibly();
        if (!quiescent) {
          return true;
        }
        stateLock.unlock();
        Thread.sleep(500);
      }
    } catch (InterruptedException e) {
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
      try {
        if (quiescent && !this.quiescent) {
          while (!openStreams.isEmpty()) {
            Thread.sleep(500);
          }
        }
        this.quiescent = quiescent;
        return true;
      } finally {
        stateLock.unlock();
      }
    } catch (InterruptedException e) {
      return false;
    }
  }

  /**
   * Provides a tracked wrapper around a given OutputStream.
   *
   * @param con the connection that trac.
   * @param stream the stream to wrap.
   * @return the wrapped version of the stream.
   */
  public OutputStream manageOutputStream(BlobStoreConnection con, OutputStream stream) {
    ManagedOutputStream managed = new ManagedOutputStream(listener, stream, con);
    openStreams.add(managed);
    return managed;
  }

  /**
   * Provides a tracked wrapper around a given OutputStream.
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
    for (ManagedOutputStream out : new HashSet<ManagedOutputStream>(openStreams)) {
      if (out.getConnection().equals(con))
        IOUtils.closeQuietly(out);
    }

    for (ManagedInputStream in : new HashSet<ManagedInputStream>(openInputStreams)) {
      if (in.getConnection().equals(con))
        IOUtils.closeQuietly(in);
    }
  }

  // how many streams are open?
  int getOpenCount() {
    return openStreams.size();
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
