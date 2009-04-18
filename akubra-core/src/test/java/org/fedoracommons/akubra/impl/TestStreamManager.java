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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.Iterator;
import java.util.Map;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.fedoracommons.akubra.Blob;
import org.fedoracommons.akubra.BlobStoreConnection;

/**
 * Unit Tests for {@link StreamManager}.
 *
 * @author Chris Wilper
 */
public class TestStreamManager {

  private final StreamManager manager = new StreamManager();

  /**
   * Manager should start in the unquiescent state with no tracked streams.
   */
  @Test
  public void testInitialState() {
    assertEquals(manager.getOpenOutputStreamCount(), 0);
    assertEquals(manager.getOpenInputStreamCount(), 0);
    assertFalse(manager.isQuiescent());
  }

  /**
   * Managed OutputStreams should be tracked when open and forgotten when closed.
   */
  @Test
  public void testManageOutputStream() throws Exception {
    OutputStream managed = null;
    assertTrue(manager.lockUnquiesced());
    try {
      managed = manager.manageOutputStream(null, new ByteArrayOutputStream());
    } finally {
      manager.unlockState();
    }
    assertEquals(manager.getOpenOutputStreamCount(), 1);
    managed.close();
    assertEquals(manager.getOpenOutputStreamCount(), 0);
  }

  /**
   * Managed InputStreams should be tracked when open and forgotten when closed.
   */
  @Test
  public void testManageInputStream() throws Exception {
    InputStream managed = null;
    assertTrue(manager.lockUnquiesced());
    try {
      managed = manager.manageInputStream(null, new ByteArrayInputStream(new byte[0]));
    } finally {
      manager.unlockState();
    }
    assertEquals(manager.getOpenInputStreamCount(), 1);
    managed.close();
    assertEquals(manager.getOpenInputStreamCount(), 0);
  }

  /**
   * Managed Streams should be tracked when open and closed when connection is closed.
   */
  @Test
  public void testTrackedConnectionCloses() throws Exception {
    BlobStoreConnection con1 = new MockConnection(manager);
    BlobStoreConnection con2 = new MockConnection(manager);
    assertTrue(manager.lockUnquiesced());
    try {
      manager.manageInputStream(con1, new ByteArrayInputStream(new byte[0]));
      manager.manageOutputStream(con1, new ByteArrayOutputStream());
      assertEquals(manager.getOpenInputStreamCount(), 1);
      assertEquals(manager.getOpenOutputStreamCount(), 1);

      manager.manageInputStream(con2, new ByteArrayInputStream(new byte[0]));
      manager.manageOutputStream(con2, new ByteArrayOutputStream());
      assertEquals(manager.getOpenInputStreamCount(), 2);
      assertEquals(manager.getOpenOutputStreamCount(), 2);

      con1.close();
      assertEquals(manager.getOpenInputStreamCount(), 1);
      assertEquals(manager.getOpenOutputStreamCount(), 1);

      con2.close();
      assertEquals(manager.getOpenInputStreamCount(), 0);
      assertEquals(manager.getOpenOutputStreamCount(), 0);
    } finally {
      manager.unlockState();
    }
  }

  /**
   * Setting quiescent state should be respected and setting to the current
   * value should have no effect.
   */
  @Test
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
  @Test
  public void testGoQuiescentOpenStreamBlocking() throws Exception {
    OutputStream managed = null;
    assertTrue(manager.lockUnquiesced());
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
    assertTrue(manager.setQuiescent(false));
  }

  /**
   * Going into quiescent state while an OutputStream is open should block,
   * then return false if interrupted.
   */
  @Test
  public void testGoQuiescentOpenStreamInterrupted() throws Exception {
    OutputStream managed = null;
    assertTrue(manager.lockUnquiesced());
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
    managed.close();
    assertTrue(manager.setQuiescent(false));
  }

  /**
   * Going into quiescent state while the state lock is held should block,
   * then return true when the state lock is released.
   */
  @Test
  public void testGoQuiescentStateLockBlocking() throws Exception {
    GoQuiescentThread thread = new GoQuiescentThread(manager);
    assertTrue(manager.lockUnquiesced());
    try {
      thread.start();
      Thread.sleep(100);
      assertTrue(thread.isAlive()); // thread should be blocking
    } finally {
      manager.unlockState();
    }
    thread.join();                // thread will now terminate; wait for it
    assertTrue(thread.getReturnValue());
    assertTrue(manager.setQuiescent(false));
  }

  /**
   * Going into quiescent state while the state lock is held should block,
   * then return false if interrupted.
   */
  @Test
  public void testGoQuiescentStateLockInterrupted() throws Exception {
    assertTrue(manager.lockUnquiesced());
    try {
      GoQuiescentThread thread = new GoQuiescentThread(manager);
      thread.start();
      Thread.sleep(100);
      assertTrue(thread.isAlive()); // thread should be blocking
      thread.interrupt();
      thread.join();                // thread will now terminate; wait for it
      assertFalse(thread.getReturnValue());
    } finally {
      manager.unlockState();
    }
    assertTrue(manager.setQuiescent(false));
  }

  /**
   * Attempting to lock unquiesced should block and return true when the
   * unquiescent state is reached.
   */
  @Test
  public void testGoUnquiescentBlocking() throws Exception {
    assertTrue(manager.setQuiescent(true));
    LockUnquiescedThread thread = new LockUnquiescedThread(manager);
    thread.start();
    Thread.sleep(100);
    assertTrue(thread.isAlive()); // thread should be blocking
    assertTrue(manager.setQuiescent(false));
    thread.join();                // thread will now terminate; wait for it
    assertTrue(thread.getReturnValue());
  }

  /**
   * Attempting to lock unquiesced should block and return false if interrupted.
   */
  @Test
  public void testGoUnquiescentInterrupted() throws Exception {
    assertTrue(manager.setQuiescent(true));
    LockUnquiescedThread thread = new LockUnquiescedThread(manager);
    thread.start();
    Thread.sleep(100);
    assertTrue(thread.isAlive()); // thread should be blocking
    thread.interrupt();
    thread.join();                // thread will now terminate; wait for it
    assertFalse(thread.getReturnValue());
    assertTrue(manager.setQuiescent(false));
  }

  private static class GoQuiescentThread extends Thread {
    private final StreamManager manager;
    private boolean returnValue;
    public GoQuiescentThread(StreamManager manager) {
      this.manager = manager;
    }
    @Override
    public void run() {
      returnValue = manager.setQuiescent(true);
    }
    public boolean getReturnValue() {
      return returnValue;
    }
  }
  private static class LockUnquiescedThread extends Thread {
    private final StreamManager manager;
    private boolean returnValue;
    public LockUnquiescedThread(StreamManager manager) {
      this.manager = manager;
    }
    @Override
    public void run() {
      returnValue = manager.lockUnquiesced();
      if (returnValue) {
        manager.unlockState();
      }
    }
    public boolean getReturnValue() {
      return returnValue;
    }
  }

  private static class MockConnection extends AbstractBlobStoreConnection {
    public MockConnection(StreamManager manager) {
      super(null, manager);
    }

    //@Override
    public Blob getBlob(URI blobId, Map<String, String> hints) throws IOException {
      return null;
    }

    //@Override
    public Iterator<URI> listBlobIds(String filterPrefix) throws IOException {
      return null;
    }
  }
}
