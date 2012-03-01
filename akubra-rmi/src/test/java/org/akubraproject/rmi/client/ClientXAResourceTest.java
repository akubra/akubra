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
package org.akubraproject.rmi.client;

import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.isA;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.reset;
import static org.easymock.EasyMock.verify;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.rmi.RemoteException;

import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.akubraproject.rmi.remote.SerializedXid;
import org.akubraproject.rmi.server.Exporter;
import org.akubraproject.rmi.server.ServerTransactionListener;
import org.akubraproject.rmi.server.ServerXAResource;

import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

/**
 * Unit tests ClientXAResource.
 *
 * @author Pradeep Krishnan
  */
public class ClientXAResourceTest {
  private Exporter          exporter;
  private ServerXAResource  sx;
  private ClientXAResource  cx;
  private XAResource        res;
  private Xid               xid;
  private ClientTransactionListener txn;
  private ServerTransactionListener stxn;

  @BeforeSuite
  public void setUp() throws Exception {
    exporter   = new Exporter(0);
    res        = createMock(XAResource.class);
    xid =
      new Xid() {
          public byte[] getBranchQualifier() {
            return null;
          }

          public int getFormatId() {
            return 0;
          }

          public byte[] getGlobalTransactionId() {
            return "test-xid".getBytes();
          }
        };

    stxn = createMock(ServerTransactionListener.class);
    sx  = new ServerXAResource(res, stxn, exporter);
    txn = createMock(ClientTransactionListener.class);
    cx  = new ClientXAResource(sx, txn);
  }

  @AfterSuite
  public void tearDown() throws Exception {
    sx.unExport(false);
  }


  @Test
  public void testCommit() throws XAException {
    reset(res);
    res.commit(isA(SerializedXid.class), eq(false));
    replay(res);

    cx.commit(xid, false);
    verify(res);
  }

  @Test
  public void testEnd() throws XAException {
    reset(res);
    res.end(isA(SerializedXid.class), eq(42));
    replay(res);

    cx.end(xid, 42);
    verify(res);
  }

  @Test
  public void testForget() throws XAException {
    reset(res);
    res.forget(isA(SerializedXid.class));
    replay(res);

    cx.forget(xid);
    verify(res);
  }

  @Test
  public void testGetTransactionTimeout() throws XAException {
    reset(res);
    expect(res.getTransactionTimeout()).andReturn(42);
    replay(res);

    assertEquals(42, cx.getTransactionTimeout());
    verify(res);
  }

  @Test
  public void testIsSameRM()
                    throws RemoteException, XAException, RollbackException, SystemException {
    reset(res);
    reset(txn);
    reset(stxn);
    expect(txn.getRemoteXAResource(res)).andReturn(sx);
    expect(stxn.getXAResource(sx)).andReturn(res);
    expect(res.isSameRM(res)).andReturn(true);
    replay(res);
    replay(txn);
    replay(stxn);

    assertTrue(cx.isSameRM(cx));
    assertFalse(cx.isSameRM(null));
    assertTrue(cx.isSameRM(res));

    verify(res);
    verify(txn);
    verify(stxn);
  }

  @Test
  public void testPrepare() throws XAException {
    reset(res);
    expect(res.prepare(isA(SerializedXid.class))).andReturn(XAResource.XA_OK);
    replay(res);

    assertEquals(XAResource.XA_OK, cx.prepare(xid));
    verify(res);
  }

  @Test
  public void testRecover() throws XAException {
    reset(res);
    expect(res.recover(42)).andReturn(new Xid[] { xid });
    replay(res);

    assertEquals(new Xid[] { new SerializedXid(xid) }, cx.recover(42));
    verify(res);
  }

  @Test
  public void testRollback() throws XAException {
    reset(res);
    res.rollback(isA(SerializedXid.class));
    replay(res);

    cx.rollback(xid);
    verify(res);
  }

  @Test
  public void testSetTransactionTimeout() throws XAException {
    reset(res);
    expect(res.setTransactionTimeout(42)).andReturn(true);
    replay(res);

    assertTrue(cx.setTransactionTimeout(42));
    verify(res);
  }

  @Test
  public void testStart() throws XAException {
    reset(res);
    res.start(isA(SerializedXid.class), eq(42));
    replay(res);

    cx.start(xid, 42);
    verify(res);
  }
}
