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

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.isA;
import static org.easymock.classextension.EasyMock.createMock;
import static org.easymock.classextension.EasyMock.replay;
import static org.easymock.classextension.EasyMock.reset;
import static org.easymock.classextension.EasyMock.verify;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.rmi.RemoteException;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.akubraproject.rmi.remote.RemoteXAResource;

import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

/**
 * Unit tests for ServerXAResource.
 *
 * @author Pradeep Krishnan
  */
public class ServerXAResourceTest {
  private Exporter                  exporter;
  private ServerTransactionListener txn;
  private ServerXAResource          sx;
  private XAResource                res;
  private Xid                       xid;

  @BeforeSuite
  public void setUp() throws Exception {
    exporter   = new Exporter(0);
    res        = createMock(XAResource.class);
    txn        = createMock(ServerTransactionListener.class);
    sx         = new ServerXAResource(res, txn, exporter);
    xid        = createMock(Xid.class);
  }

  @AfterSuite
  public void tearDown() throws Exception {
    sx.unExport(false);
    exporter = null;
  }

  @Test
  public void testServerXAResource() {
    assertTrue(sx.getExported() instanceof RemoteXAResource);
  }

  @Test
  public void testCommit() throws RemoteException, XAException {
    reset(res);
    res.commit(xid, false);
    replay(res);

    sx.commit(xid, false);
    verify(res);
  }

  @Test
  public void testEnd() throws RemoteException, XAException {
    reset(res);
    res.end(xid, 42);
    replay(res);

    sx.end(xid, 42);
    verify(res);
  }

  @Test
  public void testForget() throws RemoteException, XAException {
    reset(res);
    res.forget(xid);
    replay(res);

    sx.forget(xid);
    verify(res);
  }

  @Test
  public void testGetTransactionTimeout() throws RemoteException, XAException {
    reset(res);
    expect(res.getTransactionTimeout()).andReturn(42);
    replay(res);

    assertEquals(42, sx.getTransactionTimeout());
    verify(res);
  }

  @Test
  public void testIsSameRM() throws RemoteException, XAException {
    reset(res);
    expect(res.isSameRM(res)).andReturn(true);
    expect(txn.getXAResource(sx)).andReturn(res);
    expect(txn.getXAResource(isA(ServerXAResource.class))).andReturn(null);
    replay(res);
    replay(txn);

    assertTrue(sx.isSameRM(sx));
    assertFalse(sx.isSameRM(null));
    assertFalse(sx.isSameRM(new ServerXAResource(res, txn, exporter)));
    verify(res);
    verify(txn);
  }

  @Test
  public void testPrepare() throws RemoteException, XAException {
    reset(res);
    expect(res.prepare(xid)).andReturn(XAResource.XA_RDONLY);
    replay(res);

    assertEquals(XAResource.XA_RDONLY, sx.prepare(xid));
    verify(res);
  }

  @Test
  public void testRecover() throws RemoteException, XAException {
    reset(res);
    expect(res.recover(42)).andReturn(new Xid[] { xid });
    replay(res);

    assertEquals(new Xid[] { xid }, sx.recover(42));
    verify(res);
  }

  @Test
  public void testRollback() throws RemoteException, XAException {
    reset(res);
    res.rollback(xid);
    replay(res);

    sx.rollback(xid);
    verify(res);
  }

  @Test
  public void testSetTransactionTimeout() throws RemoteException, XAException {
    reset(res);
    expect(res.setTransactionTimeout(42)).andReturn(true);
    replay(res);

    assertTrue(sx.setTransactionTimeout(42));
    verify(res);
  }

  @Test
  public void testStart() throws RemoteException, XAException {
    reset(res);
    res.start(xid, 42);
    replay(res);

    sx.start(xid, 42);
    verify(res);
  }
}
