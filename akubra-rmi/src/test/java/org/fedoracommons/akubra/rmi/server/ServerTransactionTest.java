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

import static org.easymock.EasyMock.and;
import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.isA;
import static org.easymock.classextension.EasyMock.createMock;
import static org.easymock.classextension.EasyMock.replay;
import static org.easymock.classextension.EasyMock.reset;
import static org.easymock.classextension.EasyMock.verify;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.rmi.RemoteException;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.RollbackException;
import javax.transaction.Synchronization;
import javax.transaction.SystemException;
import javax.transaction.Transaction;

import org.easymock.Capture;

import org.fedoracommons.akubra.rmi.client.ClientSynchronization;
import org.fedoracommons.akubra.rmi.client.ClientXAResource;
import org.fedoracommons.akubra.rmi.remote.RemoteTransaction;
import org.fedoracommons.akubra.rmi.remote.RemoteXAResource;

import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

/**
 * Unit tests for ServerTransaction.
 *
 * @author Pradeep Krishnan
 */
public class ServerTransactionTest {
  private Exporter          exporter;
  private ServerTransaction st;
  private Transaction       txn;

  @BeforeSuite
  public void setUp() throws Exception {
    exporter   = new Exporter(0);
    txn        = createMock(Transaction.class);
    // akubra-client side stub that receives calls from akubra-server
    st         = new ServerTransaction(txn, exporter);
  }

  @AfterSuite
  public void tearDown() throws Exception {
    st.unExport(false);
    st         = null;
    exporter   = null;
  }

  @Test
  public void testServerTransaction() {
    assertTrue(st.getExported() instanceof RemoteTransaction);
  }

  @Test
  public void testGetRemoteXAResource()
                               throws IllegalStateException, RollbackException, SystemException,
                                      RemoteException {
    RemoteXAResource          rxa = createMock(RemoteXAResource.class);
    Capture<ClientXAResource> c   = new Capture<ClientXAResource>();

    reset(txn);
    expect(txn.enlistResource(and(isA(ClientXAResource.class), capture(c)))).andReturn(true);
    replay(txn);

    assertTrue(st.getEnlistedResources().isEmpty());
    assertTrue(st.enlistResource(rxa));
    assertEquals(1, st.getEnlistedResources().size());
    assertEquals(st.getEnlistedResources().iterator().next(), c.getValue());
    assertEquals(rxa, st.getRemoteXAResource(c.getValue()));
    verify(txn);

    reset(txn);
    expect(txn.delistResource(c.getValue(), 42)).andReturn(true);
    replay(txn);

    assertTrue(st.delistResource(rxa, 42));
    assertTrue(st.getEnlistedResources().isEmpty());
    assertNull(st.getRemoteXAResource(c.getValue()));
    verify(txn);
  }

  @Test
  public void testCommit()
                  throws SecurityException, IllegalStateException, RollbackException,
                         HeuristicMixedException, HeuristicRollbackException, SystemException,
                         RemoteException {
    reset(txn);
    txn.commit();
    replay(txn);

    st.commit();
    verify(txn);
  }

  @Test
  public void testDelistResource()
                          throws IllegalStateException, SystemException, RemoteException,
                                 RollbackException {
    RemoteXAResource          rxa = createMock(RemoteXAResource.class);
    Capture<ClientXAResource> c   = new Capture<ClientXAResource>();

    reset(txn);
    expect(txn.enlistResource(and(isA(ClientXAResource.class), capture(c)))).andReturn(true);
    replay(txn);

    assertTrue(st.enlistResource(rxa));
    verify(txn);

    reset(txn);
    expect(txn.delistResource(c.getValue(), 42)).andReturn(true);
    replay(txn);

    assertTrue(st.delistResource(rxa, 42));
    assertFalse(st.delistResource(rxa, 42));
    assertFalse(st.delistResource(null, 0));
    verify(txn);
  }

  @Test
  public void testEnlistResource()
                          throws IllegalStateException, SystemException, RemoteException,
                                 RollbackException {
    RemoteXAResource          rxa = createMock(RemoteXAResource.class);
    Capture<ClientXAResource> c   = new Capture<ClientXAResource>();

    reset(txn);
    expect(txn.enlistResource(and(isA(ClientXAResource.class), capture(c)))).andReturn(true);
    expect(txn.enlistResource(null)).andReturn(false);
    replay(txn);

    assertTrue(st.enlistResource(rxa));
    assertFalse(st.enlistResource(rxa));
    assertFalse(st.enlistResource(null));
    assertEquals(1, st.getEnlistedResources().size());
    verify(txn);

    // De-list so that st can be used in other tests.
    reset(txn);
    expect(txn.delistResource(c.getValue(), 0)).andReturn(true);
    replay(txn);

    assertTrue(st.delistResource(rxa, 0));
    verify(txn);
  }

  @Test
  public void testGetStatus() throws RemoteException, SystemException {
    reset(txn);
    expect(txn.getStatus()).andReturn(42);
    replay(txn);

    assertEquals(42, st.getStatus());
    verify(txn);
  }

  @Test
  public void testRegisterSynchronization()
                                   throws RemoteException, RollbackException, SystemException {
    reset(txn);

    Synchronization sync = createMock(Synchronization.class);
    txn.registerSynchronization(isA(ClientSynchronization.class));
    replay(txn);

    st.registerSynchronization(new ServerSynchronization(sync, exporter));
    verify(txn);
  }

  @Test
  public void testRollback() throws RemoteException, SystemException {
    reset(txn);
    txn.rollback();
    replay(txn);

    st.rollback();
    verify(txn);
  }

  @Test
  public void testSetRollbackOnly() throws RemoteException, SystemException {
    reset(txn);
    txn.setRollbackOnly();
    replay(txn);

    st.setRollbackOnly();
    verify(txn);
  }
}
