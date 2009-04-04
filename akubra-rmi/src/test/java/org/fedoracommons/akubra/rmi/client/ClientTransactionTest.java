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
package org.fedoracommons.akubra.rmi.client;

import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.isA;
import static org.easymock.classextension.EasyMock.createMock;
import static org.easymock.classextension.EasyMock.replay;
import static org.easymock.classextension.EasyMock.reset;
import static org.easymock.classextension.EasyMock.verify;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.rmi.RemoteException;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.RollbackException;
import javax.transaction.Synchronization;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.xa.XAResource;

import org.fedoracommons.akubra.rmi.remote.RemoteTransaction;
import org.fedoracommons.akubra.rmi.server.Exporter;
import org.fedoracommons.akubra.rmi.server.ServerTransaction;

import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

/**
 * Unit tests ClientTransaction.
 *
 * @author Pradeep Krishnan
  */
public class ClientTransactionTest {
  private Exporter          exporter;
  private Transaction       txn;
  private ServerTransaction st;
  private ClientTransaction ct;

  @BeforeSuite
  public void setUp() throws Exception {
    exporter   = new Exporter(0);
    txn        = createMock(Transaction.class);
    st         = new ServerTransaction(txn, exporter);
    ct         = new ClientTransaction((RemoteTransaction) st.getExported(), exporter);
  }

  @AfterSuite
  public void tearDown() throws Exception {
    st.unExport(false);
  }

  @Test
  public void testCommit()
                  throws SecurityException, IllegalStateException, RollbackException,
                         HeuristicMixedException, HeuristicRollbackException, SystemException,
                         RemoteException {
    reset(txn);
    txn.commit();
    replay(txn);

    ct.commit();
    verify(txn);
  }

  @Test
  public void testDelistResource()
                          throws IllegalStateException, SystemException, RemoteException,
                                 RollbackException {
    reset(txn);

    XAResource res = createMock(XAResource.class);
    expect(txn.enlistResource(isA(ClientXAResource.class))).andReturn(true);
    expect(txn.delistResource(isA(ClientXAResource.class), eq(0))).andReturn(true);
    replay(txn);

    assertNull(ct.getRemoteXAResource(res));
    assertFalse(ct.delistResource(res, 0));
    assertNull(ct.getRemoteXAResource(res));
    assertTrue(ct.enlistResource(res));
    assertNotNull(ct.getRemoteXAResource(res));
    assertTrue(ct.delistResource(res, 0));
    assertNull(ct.getRemoteXAResource(res));
    verify(txn);
  }

  @Test
  public void testEnlistResource()
                          throws IllegalStateException, SystemException, RemoteException,
                                 RollbackException {
    reset(txn);

    XAResource res = createMock(XAResource.class);
    expect(txn.enlistResource(isA(ClientXAResource.class))).andReturn(true);
    replay(txn);

    assertNull(ct.getRemoteXAResource(res));
    assertTrue(ct.enlistResource(res));
    assertFalse(ct.enlistResource(res));
    verify(txn);
  }

  @Test
  public void testGetStatus() throws RemoteException, SystemException {
    reset(txn);
    expect(txn.getStatus()).andReturn(42);
    replay(txn);

    assertEquals(42, ct.getStatus());
    verify(txn);
  }

  @Test
  public void testRegisterSynchronization()
                                   throws RemoteException, RollbackException, SystemException {
    reset(txn);

    Synchronization sync = createMock(Synchronization.class);
    txn.registerSynchronization(isA(ClientSynchronization.class));
    replay(txn);

    ct.registerSynchronization(sync);
    verify(txn);
  }

  @Test
  public void testRollback() throws RemoteException, SystemException {
    reset(txn);
    txn.rollback();
    replay(txn);

    ct.rollback();
    verify(txn);
  }

  @Test
  public void testSetRollbackOnly() throws RemoteException, SystemException {
    reset(txn);
    txn.setRollbackOnly();
    replay(txn);

    ct.setRollbackOnly();
    verify(txn);
  }
}
