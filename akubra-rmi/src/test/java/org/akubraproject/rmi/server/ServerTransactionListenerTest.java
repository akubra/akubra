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
package org.akubraproject.rmi.server;

import static org.easymock.EasyMock.createMock;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.io.IOException;
import java.net.URI;
import java.rmi.RemoteException;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.RollbackException;
import javax.transaction.Synchronization;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.xa.XAResource;

import org.akubraproject.BlobStore;
import org.akubraproject.BlobStoreConnection;
import org.akubraproject.impl.AbstractBlobStore;
import org.akubraproject.rmi.remote.RemoteCallListener.Operation;
import org.akubraproject.rmi.remote.RemoteCallListener.Result;
import org.akubraproject.rmi.remote.RemoteConnection;
import org.akubraproject.rmi.remote.RemoteSynchronization;
import org.akubraproject.rmi.remote.RemoteTransactionListener.DelistXAResource;
import org.akubraproject.rmi.remote.RemoteTransactionListener.EnlistXAResource;
import org.akubraproject.rmi.remote.RemoteTransactionListener.GetStatus;
import org.akubraproject.rmi.remote.RemoteTransactionListener.RegisterSynchronization;
import org.akubraproject.rmi.remote.RemoteTransactionListener.Rollback;
import org.akubraproject.rmi.remote.RemoteXAResource;
import org.akubraproject.rmi.server.Exporter;
import org.akubraproject.rmi.server.ServerTransactionListener;

import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

/**
 * Unit tests ServerTransactionListener
 *
 * @author Pradeep Krishnan
 */
@Test(sequential=true)
public class ServerTransactionListenerTest {
  private Exporter                  exporter;
  private ServerTransactionListener st;
  private ExecutorService           executor;
  private BlobStoreConnection       con;

  /**
   * Setup for all tests.
   *
   */
  @BeforeSuite
  public void setUp() throws Exception {
    exporter = new Exporter(0);
    con   = createMock(BlobStoreConnection.class);

    BlobStore store = new AbstractBlobStore(URI.create("urn:test")) {

      @Override
      public BlobStoreConnection openConnection(Transaction tx, Map<String, String> hints)
          throws UnsupportedOperationException, IOException {
        /*
         * The st object is usable for tests till openConnection succeeds.
         * So block open connection till we complete our tests.
         */
        synchronized(con) {
          try {
            con.wait();
          } catch (InterruptedException e) {
            throw new RuntimeException("interrupted", e);
          }
        }
        return con;
      }
    };

    executor = Executors.newSingleThreadExecutor();
    st = new ServerTransactionListener(store, null, exporter);
  }

  /**
   * Tear down after all tests.
   *
   */
  @SuppressWarnings("unchecked")
  @AfterSuite
  public void tearDown() throws Exception {
    synchronized(con) {
      con.notify();
    }
    Operation<?> op = st.getNextOperation();
    assertTrue(op instanceof Result);

    RemoteConnection rc = ((Result<RemoteConnection>) op).get();
    assertTrue(rc instanceof ServerConnection);
    assertEquals(con, ((ServerConnection) rc).getConnection());

    executor.shutdownNow();

    for (int i = 1; (i <= 10) && (st.getExported() != null); i++)
      Thread.sleep(Exporter.RETRY_DELAY * i);

    assertNull(st.getExported());
  }

  /**
   * Tests that commit() is always denied.
   *
   */
  @Test(expectedExceptions =  {SecurityException.class})
  public void testCommit()
                  throws SecurityException, IllegalStateException, RollbackException,
                         HeuristicMixedException, HeuristicRollbackException, SystemException,
                         RemoteException {
    st.commit();
  }

  /**
   * Tests that enlist/delist of XAResource is relayed correctly.
   *
   */
  @Test
  public void testXAResource() throws Exception {
    assertFalse(st.delistResource(null, 0));
    assertFalse(st.enlistResource(null));

    final XAResource res    = createMock(XAResource.class);
    Future<Boolean>  future;

    future =
      executor.submit(new Callable<Boolean>() {
          public Boolean call() throws Exception {
            return st.enlistResource(res);
          }
        });

    Operation<?> op         = st.getNextOperation();
    assertTrue(op instanceof EnlistXAResource);

    RemoteXAResource rxa = ((EnlistXAResource) op).getXAResource();
    assertTrue(rxa instanceof ServerXAResource);
    assertEquals(((ServerXAResource) rxa).getXAResource(), res);
    st.postResult(new Result<Boolean>(true));
    assertTrue(future.get());

    future =
      executor.submit(new Callable<Boolean>() {
          public Boolean call() throws Exception {
            return st.enlistResource(res);
          }
        });

    op = st.getNextOperation();
    assertTrue(op instanceof EnlistXAResource);
    rxa = ((EnlistXAResource) op).getXAResource();
    assertTrue(rxa instanceof ServerXAResource);
    assertEquals(((ServerXAResource) rxa).getXAResource(), res);
    st.postResult(new Result<Boolean>(false));
    assertFalse(future.get());

    future =
      executor.submit(new Callable<Boolean>() {
          public Boolean call() throws Exception {
            return st.delistResource(res, 42);
          }
        });

    op = st.getNextOperation();
    assertTrue(op instanceof DelistXAResource);
    assertEquals(42, ((DelistXAResource) op).getFlags());
    rxa = ((DelistXAResource) op).getXAResource();
    assertTrue(rxa instanceof ServerXAResource);
    assertEquals(((ServerXAResource) rxa).getXAResource(), res);
    st.postResult(new Result<Boolean>(true));
    assertTrue(future.get());

    assertFalse(st.delistResource(res, 0));
  }

  /**
   * Tests that getStatus() is relayed correctly.
   *
   */
  @Test
  public void testGetStatus() throws Exception {
    Future<Integer> future =
      executor.submit(new Callable<Integer>() {
          public Integer call() throws Exception {
            return st.getStatus();
          }
        });

    Operation<?> op        = st.getNextOperation();
    assertTrue(op instanceof GetStatus);
    st.postResult(new Result<Integer>(42));

    assertEquals(new Integer(42), future.get());
  }

  /**
   * Tests that registerSynchronization() is relayed correctly.
   *
   */
  @Test
  public void testRegisterSynchronization() throws Exception {
    final Synchronization sync   = createMock(Synchronization.class);

    Future<Void>          future =
      executor.submit(new Callable<Void>() {
          public Void call() throws Exception {
            st.registerSynchronization(sync);

            return null;
          }
        });

    Operation<?> op              = st.getNextOperation();
    assertTrue(op instanceof RegisterSynchronization);

    RemoteSynchronization rsync = ((RegisterSynchronization) op).getSynchronization();
    assertTrue(rsync instanceof ServerSynchronization);
    assertEquals(sync, ((ServerSynchronization) rsync).getSynchronization());
    st.postResult(new Result<Void>());

    assertNull(future.get());
  }

  /**
   * Tests that rollback() is relayed correctly.
   */
  @Test
  public void testRollback() throws Exception {
    Future<Void> future =
      executor.submit(new Callable<Void>() {
          public Void call() throws Exception {
            st.rollback();

            return null;
          }
        });

    Operation<?> op     = st.getNextOperation();
    assertTrue(op instanceof Rollback);
    st.postResult(new Result<Void>());

    assertNull(future.get());
  }

  /**
   * Tests that the setRollbackOnly is disallowed.
   *
   */
  @Test(expectedExceptions =  {IllegalStateException.class})
  public void testSetRollbackOnly() throws RemoteException, SystemException {
    st.setRollbackOnly();
  }
}
