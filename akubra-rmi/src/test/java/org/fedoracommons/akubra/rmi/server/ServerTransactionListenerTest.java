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
package org.fedoracommons.akubra.rmi.server;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.isA;
import static org.easymock.classextension.EasyMock.createMock;
import static org.easymock.classextension.EasyMock.makeThreadSafe;
import static org.easymock.classextension.EasyMock.replay;
import static org.easymock.classextension.EasyMock.verify;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.rmi.RemoteException;

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

import org.fedoracommons.akubra.BlobStore;
import org.fedoracommons.akubra.BlobStoreConnection;
import org.fedoracommons.akubra.rmi.remote.RemoteCallListener.Operation;
import org.fedoracommons.akubra.rmi.remote.RemoteCallListener.Result;
import org.fedoracommons.akubra.rmi.remote.RemoteConnection;
import org.fedoracommons.akubra.rmi.remote.RemoteSynchronization;
import org.fedoracommons.akubra.rmi.remote.RemoteTransactionListener.DelistXAResource;
import org.fedoracommons.akubra.rmi.remote.RemoteTransactionListener.EnlistXAResource;
import org.fedoracommons.akubra.rmi.remote.RemoteTransactionListener.GetStatus;
import org.fedoracommons.akubra.rmi.remote.RemoteTransactionListener.RegisterSynchronization;
import org.fedoracommons.akubra.rmi.remote.RemoteTransactionListener.Rollback;
import org.fedoracommons.akubra.rmi.remote.RemoteXAResource;
import org.fedoracommons.akubra.rmi.server.Exporter;
import org.fedoracommons.akubra.rmi.server.ServerTransactionListener;

import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

/**
 * Unit tests ServerTransactionListener
 *
 * @author Pradeep Krishnan
 */
public class ServerTransactionListenerTest {
  private Exporter                  exporter;
  private ServerTransactionListener st;
  private ExecutorService           executor;

  /**
   * Setup for all tests.
   *
   */
  @SuppressWarnings("unchecked")
  @BeforeSuite
  public void setUp() throws Exception {
    exporter = new Exporter(0);

    BlobStore           store = createMock(BlobStore.class);
    BlobStoreConnection con   = createMock(BlobStoreConnection.class);

    expect(store.openConnection(isA(Transaction.class))).andReturn(con);
    makeThreadSafe(store, true);
    replay(store);

    st = new ServerTransactionListener(store, false, exporter);

    Operation<?> op = st.getNextOperation();
    assertTrue(op instanceof Result);

    RemoteConnection rc = ((Result<RemoteConnection>) op).get();
    assertTrue(rc instanceof ServerConnection);
    assertEquals(con, ((ServerConnection) rc).getConnection());

    verify(store);

    executor = Executors.newSingleThreadExecutor();
  }

  /**
   * Tear down after all tests.
   *
   */
  @AfterSuite
  public void tearDown() throws Exception {
    st.unExport(false);
    executor.shutdownNow();
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
