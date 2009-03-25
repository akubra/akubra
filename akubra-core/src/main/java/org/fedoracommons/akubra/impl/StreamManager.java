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
import java.io.OutputStream;

import java.util.WeakHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Utility class that tracks the open <code>OutputStream</code>s of a
 * <code>BlobStore</code> in order to provide a <code>setQuiescent</code>
 * implementation with the correct blocking behavior.
 *
 * @author Chris Wilper
 */
public class StreamManager {

  /** Exclusive lock on the quiescent state. */
  private final ReentrantLock stateLock = new ReentrantLock(true);

  /** Listens to close events. */
  private final CloseListener listener;

  /** The set of open <code>OutputStream</code>s managed by this instance. */
  private volatile WeakHashMap<OutputStream, Object> openStreams
      = new WeakHashMap<OutputStream, Object>();

  /** The current quiescent state. */
  private boolean quiescent;

  /**
   * Creates an instance.
   */
  public StreamManager() {
    listener = new CloseListener() {
      public void notifyClosed(Closeable closeable) {
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
   * @param stream the stream to wrap.
   * @return the wrapped version of the stream.
   */
  public OutputStream manageOutputStream(OutputStream stream) {
    OutputStream managed = new ManagedOutputStream(listener, stream);
    openStreams.put(managed, null);
    return managed;
  }

  // how many streams are open?
  int getOpenCount() {
    return openStreams.size();
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
