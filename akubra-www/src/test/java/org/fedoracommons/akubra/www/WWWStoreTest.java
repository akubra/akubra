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
package org.fedoracommons.akubra.www;

import java.io.IOException;

import java.net.URI;

import java.util.ArrayList;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.RollbackException;
import javax.transaction.Synchronization;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.xa.XAResource;

import org.fedoracommons.akubra.BlobStore;
import org.fedoracommons.akubra.Capability;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

/**
 * Unit Test for WWWStore.
 *
 * @author Pradeep Krishnan
 */
public class WWWStoreTest {
  private URI       id;
  private BlobStore store;

  /**
   * Set up a WWWStore for testing.
   *
   * @throws Exception on an error
   */
  @BeforeSuite
  public void setUp() throws Exception {
    store = new WWWStore(id = URI.create("urn:www:test"));
  }

  /**
   * Tear down the store after testing.
   *
   * @throws Exception on an error
   */
  @AfterSuite
  public void tearDown() throws Exception {
  }

  /**
   * Test the getId operation.
   */
  @Test
  public void testGetId() {
    assertEquals(id, store.getId());
  }

  /**
   * Test the getBackingStores operation.
   */
  @Test
  public void testGetBackingStores() {
    assertTrue(store.getBackingStores().isEmpty());
  }

  /**
   * Test the setBackingStores operation.
   *
   * @throws UnsupportedOperationException DOCUMENT ME!
   */
  @Test(expectedExceptions =  {
    UnsupportedOperationException.class}
  )
  public void testSetBackingStores() throws UnsupportedOperationException {
    store.setBackingStores(new ArrayList<BlobStore>());
    fail("setBackingStores() failed");
  }

  /**
   * Test the normal case of openConnection.
   */
  @Test
  public void testOpenConnectionWithNullTxn() {
    try {
      store.openConnection(null).close();
    } catch (UnsupportedOperationException e) {
      fail("openConnection() failed", e);
    } catch (IOException e) {
      fail("openConnection() failed", e);
    }
  }

  /**
   * Test the error case of openConnection.
   *
   * @throws UnsupportedOperationException the expected exception
   */
  @Test(expectedExceptions =  {
    UnsupportedOperationException.class}
  )
  public void testOpenConnectionWithTxn() throws UnsupportedOperationException {
    try {
      store.openConnection(new TestTransaction());
    } catch (IOException e) {
      fail("openConnection() failed", e);
    }
  }

  /**
   * Test the setQuiescent operation.
   */
  @Test
  public void testSetQuiescent() {
    try {
      assertFalse(store.setQuiescent(true));
      assertFalse(store.setQuiescent(false));
    } catch (IOException e) {
      fail("setQuiescent() failed", e);
    }
  }

  /**
   * Test getDeclaredCapabilities
   */
  @Test
  public void testDeclaredCapabilities() {
    assertEquals(new Capability[0], store.getDeclaredCapabilities());
  }

  /**
   * Test getCapabilities
   */
  @Test
  public void testCapabilities() {
    assertEquals(new Capability[0], store.getCapabilities());
  }

  private static class TestTransaction implements Transaction {
    public void commit()
                throws RollbackException, HeuristicMixedException, HeuristicRollbackException,
                       SecurityException, IllegalStateException, SystemException {
    }

    public boolean delistResource(XAResource arg0, int arg1)
                           throws IllegalStateException, SystemException {
      return false;
    }

    public boolean enlistResource(XAResource arg0)
                           throws RollbackException, IllegalStateException, SystemException {
      return false;
    }

    public int getStatus() throws SystemException {
      return 0;
    }

    public void registerSynchronization(Synchronization arg0)
                                 throws RollbackException, IllegalStateException, SystemException {
    }

    public void rollback() throws IllegalStateException, SystemException {
    }

    public void setRollbackOnly() throws IllegalStateException, SystemException {
    }
  }
}
