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
package org.fedoracommons.akubra.fs;

import java.io.File;

import java.net.URI;

import java.util.ArrayList;

import javax.transaction.Synchronization;
import javax.transaction.Transaction;
import javax.transaction.xa.XAResource;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.fedoracommons.akubra.BlobStore;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Unit tests for {@link FSBlobStore}.
 *
 * @author Chris Wilper
 */
public class TestFSBlobStore {
  private static URI id;

  private static File tmpDir;
  private static FSBlobStore store;

  @BeforeClass
  public static void init() throws Exception {
    id = new URI("urn:example:store");
    tmpDir = FSTestUtil.createTempDir();
    store = new FSBlobStore(id, tmpDir);
  }

  @AfterClass
  public static void destroy() {
    FSTestUtil.rmdir(tmpDir);
  }

  /**
   * Store id should be what it was initialized with.
   */
  @Test
  public void testGetId() {
    assertEquals(id, store.getId());
  }

  /**
   * Request to open a connection without a transaction should succeed.
   */
  @Test
  public void testOpenConnectionNoTransaction() {
    try {
      store.openConnection(null);
    } catch (UnsupportedOperationException e) {
      fail("failed to open non-transactional connection");
    }
  }

  /**
   * Request to open a connection with a transaction is unsupported.
   */
  @Test (expected=UnsupportedOperationException.class)
  public void testOpenConnectionWithTransaction() {
    store.openConnection(new MockTransaction());
  }

  /**
   * Request to go quiescent and non-quiescent (even when already in those
   * states) should be supported.
   */
  @Test
  public void testSetQuiescent() {
    assertTrue(store.setQuiescent(true));
    assertTrue(store.setQuiescent(true));
    assertTrue(store.setQuiescent(false));
    assertTrue(store.setQuiescent(false));
  }

  /**
   * Should return an empty list.
   */
  @Test
  public void testGetBackingStores() {
    assertEquals(0, store.getBackingStores().size());
  }

  /**
   * Not supported; should fail.
   */
  @Test (expected=UnsupportedOperationException.class)
  public void testSetBackingStores() {
    store.setBackingStores(new ArrayList<BlobStore>());
  }

  /**
   * Should be empty; test when implemented.
   */
  @Test
  public void testGetDeclaredCapabilities() {
    assertEquals(1, store.getDeclaredCapabilities().length);
    assertEquals(BlobStore.GENERATE_ID_CAPABILITY, store.getDeclaredCapabilities()[0].getId());
  }

  /**
   * Should be empty; test when implemented.
   */
  @Test
  public void testGetCapabilities() {
    assertEquals(1, store.getCapabilities().length);
    assertEquals(BlobStore.GENERATE_ID_CAPABILITY, store.getDeclaredCapabilities()[0].getId());
  }

  private static class MockTransaction implements Transaction {
    public void commit() { }
    public boolean delistResource(XAResource xaRes, int flag) { return false; }
    public boolean enlistResource(XAResource xaRes) { return false; }
    public int getStatus() { return 0; }
    public void registerSynchronization(Synchronization sync) { }
    public void rollback() { }
    public void setRollbackOnly() { }
  }
}
