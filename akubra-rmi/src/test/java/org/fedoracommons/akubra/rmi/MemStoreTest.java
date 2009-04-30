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
package org.fedoracommons.akubra.rmi;

import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.io.IOException;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.RollbackException;
import javax.transaction.Synchronization;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.xa.XAResource;

import org.fedoracommons.akubra.BlobStore;
import org.fedoracommons.akubra.mem.MemBlobStore;

import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

/**
 * Tests for accessing a MemStore across RMI.
 *
 * @author Pradeep Krishnan
  */
public class MemStoreTest {
  private BlobStore       mem;
  private BlobStore       store;
  private AkubraRMIServer server;

  /**
   * Starts up an rmi-server and create a connection to it.
   *
   */
  @BeforeSuite
  public void setUp() throws Exception {
    mem = new MemBlobStore();
    int reg = ServiceTest.freePort();
    server = new AkubraRMIServer(mem, reg);
    store = AkubraRMIClient.create(reg);
  }

  /**
   * Shuts down the server store.
   *
   */
  @AfterSuite
  public void tearDown() throws Exception {
    server.shutDown(true);
  }

  /**
   * Test the normal case of openConnection (for MemStore).
   */
  @Test
  public void testOpenConnectionWithNullTxn() {
    try {
      store.openConnection(null, null).close();
    } catch (UnsupportedOperationException e) {
      fail("openConnection() failed", e);
    } catch (IOException e) {
      fail("openConnection() failed", e);
    }
  }

  /**
   * Test the error case of openConnection (for MemStore).
   */
  @Test
  public void testOpenConnectionWithTxn() {
    try {
      store.openConnection(new MockTransaction(), null);
      fail("Failed to rcv an expected exceptio");
    } catch (UnsupportedOperationException e) {
    } catch (IOException e) {
      fail("openConnection() failed", e);
    }
  }

  private static class MockTransaction implements Transaction {
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
