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
import static org.testng.Assert.assertTrue;

import java.rmi.RemoteException;

import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.fedoracommons.akubra.rmi.remote.RemoteTransaction;
import org.fedoracommons.akubra.rmi.remote.RemoteXAResource;
import org.fedoracommons.akubra.rmi.remote.SerializedXid;
import org.fedoracommons.akubra.rmi.server.Exporter;
import org.fedoracommons.akubra.rmi.server.ServerTransaction;

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
  private RemoteXAResource  sx;
  private ClientXAResource  cx;
  private ServerTransaction st;
  private XAResource        res;
  private Transaction       txn;
  private Xid               xid;

  @BeforeSuite
  public void setUp() throws Exception {
    exporter   = new Exporter(0);
    res        = createMock(XAResource.class);
    txn        = createMock(Transaction.class);
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

    expect(txn.enlistResource(isA(XAResource.class))).andReturn(true);
    replay(txn);

    // txn gets exported to the server by the akubra-client
    st         = new ServerTransaction(txn, exporter);

    // exported txn gets converted to txn by the akubra-server
    ClientTransaction ct = new ClientTransaction((RemoteTransaction) st.getExported(), exporter);

    /*
     * A resource enlists with this txn on the akubra-server side.
     * this should trigger an enlist all the way to the akubra-client side
     */
    assertTrue(ct.enlistResource(res));

    verify(txn); // verifies that this did get registered on akubra-client

    // gets the exported server resource at the akubra-server side
    sx = ct.getRemoteXAResource(res);
    assertNotNull(sx);

    /*
     * gets the akubra-client side resource that got enlisted with the akubra-client side
     * transaction (the real txn). This is the ClientXAResource under test. (ie.
     * integration-testing)
     */
    cx = (ClientXAResource) st.getEnlistedResources().iterator().next();
  }

  @AfterSuite
  public void tearDown() throws Exception {
    st.unExport(false);
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
    expect(res.isSameRM(isA(XAResource.class))).andReturn(true);
    replay(res);

    assertTrue(cx.isSameRM(cx));
    assertFalse(cx.isSameRM(null));
    assertFalse(cx.isSameRM(res));
    verify(res);
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
