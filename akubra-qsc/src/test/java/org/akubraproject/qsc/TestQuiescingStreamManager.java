/* $HeadURL$
 * $Id$
 *
 * Copyright (c) 2009 DuraSpace
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

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;

import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

/**
 * Unit Tests for {@link QuiescingStreamManager}.
 *
 * @author Chris Wilper
 */
public class TestQuiescingStreamManager {

  private final QuiescingStreamManager manager = new QuiescingStreamManager();

  /**
   * Manager should start in the unquiescent state with no tracked streams.
   */
  @Test(groups={ "init" })
  public void testInitialState() {
    assertFalse(manager.isQuiescent());
  }

  /**
   * Setting quiescent state should be respected and setting to the current
   * value should have no effect.
   */
  @Test(dependsOnGroups = { "init" })
  public void testSetQuiescentWhileInactive() throws Exception {
    manager.setQuiescent(true);
    assertTrue(manager.isQuiescent());
    manager.setQuiescent(true);
    assertTrue(manager.isQuiescent());
    manager.setQuiescent(false);
    assertFalse(manager.isQuiescent());
    manager.setQuiescent(false);
    assertFalse(manager.isQuiescent());
  }

  /**
   * Going into quiescent state while an OutputStream is open should block,
   * then return true when the stream is closed.
   */
  @Test(dependsOnGroups = { "init" })
  public void testGoQuiescentOpenStreamBlocking() throws Exception {
    OutputStream managed = null;
    manager.lockUnquiesced();
    try {
      managed = manager.manageOutputStream(null, new ByteArrayOutputStream());
    } finally {
      manager.unlockState();
    }
    GoQuiescentThread thread = new GoQuiescentThread(manager);
    thread.start();
    Thread.sleep(100);
    assertTrue(thread.isAlive()); // thread should be blocking
    managed.close();
    thread.join();                // thread will now terminate; wait for it
    assertTrue(thread.getReturnValue());
    assertNull(thread.getException());
    assertTrue(manager.setQuiescent(false));
  }

  /**
   * Going into quiescent state while an OutputStream is open should block,
   * then return false if interrupted.
   */
  @Test(dependsOnGroups = { "init" })
  public void testGoQuiescentOpenStreamInterrupted() throws Exception {
    OutputStream managed = null;
    manager.lockUnquiesced();
    try {
      managed = manager.manageOutputStream(null, new ByteArrayOutputStream());
    } finally {
      manager.unlockState();
    }
    GoQuiescentThread thread = new GoQuiescentThread(manager);
    thread.start();
    Thread.sleep(100);
    assertTrue(thread.isAlive()); // thread should be blocking
    thread.interrupt();
    thread.join();                // thread will now terminate; wait for it
    assertFalse(thread.getReturnValue());
    assertNull(thread.getException());
    managed.close();
    assertTrue(manager.setQuiescent(false));
  }

  /**
   * Going into quiescent state while the state lock is held should block,
   * then return true when the state lock is released.
   */
  @Test(dependsOnGroups = { "init" })
  public void testGoQuiescentStateLockBlocking() throws Exception {
    GoQuiescentThread thread = new GoQuiescentThread(manager);
    manager.lockUnquiesced();
    try {
      thread.start();
      Thread.sleep(100);
      assertTrue(thread.isAlive()); // thread should be blocking
    } finally {
      manager.unlockState();
    }
    thread.join();                // thread will now terminate; wait for it
    assertTrue(thread.getReturnValue());
    assertNull(thread.getException());
    assertTrue(manager.setQuiescent(false));
  }

  /**
   * Going into quiescent state while the state lock is held should block,
   * then return false if interrupted.
   */
  @Test(dependsOnGroups = { "init" })
  public void testGoQuiescentStateLockInterrupted() throws Exception {
    manager.lockUnquiesced();
    try {
      GoQuiescentThread thread = new GoQuiescentThread(manager);
      thread.start();
      Thread.sleep(100);
      assertTrue(thread.isAlive()); // thread should be blocking
      thread.interrupt();
      thread.join();                // thread will now terminate; wait for it
      assertFalse(thread.getReturnValue());
      assertNull(thread.getException());
    } finally {
      manager.unlockState();
    }
    assertTrue(manager.setQuiescent(false));
  }

  /**
   * Attempting to lock unquiesced should block and return true when the
   * unquiescent state is reached.
   */
  @Test(dependsOnGroups = { "init" })
  public void testGoUnquiescentBlocking() throws Exception {
    assertTrue(manager.setQuiescent(true));
    LockUnquiescedThread thread = new LockUnquiescedThread(manager);
    thread.start();
    Thread.sleep(100);
    assertTrue(thread.isAlive()); // thread should be blocking
    assertTrue(manager.setQuiescent(false));
    thread.join();                // thread will now terminate; wait for it
    assertNull(thread.getException());
  }

  /**
   * Attempting to lock unquiesced should block and return false if interrupted.
   */
  @Test(dependsOnGroups = { "init" })
  public void testGoUnquiescentInterrupted() throws Exception {
    assertTrue(manager.setQuiescent(true));
    LockUnquiescedThread thread = new LockUnquiescedThread(manager);
    thread.start();
    Thread.sleep(100);
    assertTrue(thread.isAlive()); // thread should be blocking
    thread.interrupt();
    thread.join();                // thread will now terminate; wait for it
    assertNotNull(thread.getException());
    assertTrue(manager.setQuiescent(false));
  }

  private static class GoQuiescentThread extends Thread {
    private final QuiescingStreamManager manager;
    private boolean returnValue;
    private Throwable t;

    public GoQuiescentThread(QuiescingStreamManager manager) {
      this.manager = manager;
    }

    @Override
    public void run() {
      try {
        returnValue = manager.setQuiescent(true);
      } catch (Throwable t) {
        this.t = t;
      }
    }

    public boolean getReturnValue() {
      return returnValue;
    }

    public Throwable getException() {
      return t;
    }
  }

  private static class LockUnquiescedThread extends Thread {
    private final QuiescingStreamManager manager;
    private Throwable t;

    public LockUnquiescedThread(QuiescingStreamManager manager) {
      this.manager = manager;
    }

    @Override
    public void run() {
      try {
        manager.lockUnquiesced();
        manager.unlockState();
      } catch (Throwable t) {
        this.t = t;
      }
    }

    public Throwable getException() {
      return t;
    }
  }
}
