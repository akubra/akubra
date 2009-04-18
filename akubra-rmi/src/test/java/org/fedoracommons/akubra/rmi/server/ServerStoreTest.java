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
import static org.easymock.classextension.EasyMock.replay;
import static org.easymock.classextension.EasyMock.reset;
import static org.easymock.classextension.EasyMock.verify;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.io.IOException;

import java.net.URI;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import javax.transaction.Transaction;

import org.fedoracommons.akubra.BlobStore;
import org.fedoracommons.akubra.BlobStoreConnection;
import org.fedoracommons.akubra.rmi.remote.RemoteConnection;
import org.fedoracommons.akubra.rmi.remote.RemoteStore;
import org.fedoracommons.akubra.rmi.remote.RemoteTransactionListener;
import org.fedoracommons.akubra.rmi.remote.RemoteCallListener.Operation;
import org.fedoracommons.akubra.rmi.remote.RemoteCallListener.Result;

import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

/**
 * Unit tests for ServerStore.
 *
 * @author Pradeep Krishnan
  */
public class ServerStoreTest {
  private Exporter    exporter;
  private ServerStore ss;
  private BlobStore   store;

  @BeforeSuite
  public void setUp() throws Exception {
    exporter   = new Exporter(0);
    store      = createMock(BlobStore.class);
    ss         = new ServerStore(store, exporter);
  }

  @AfterSuite
  public void tearDown() throws Exception {
    ss.unExport(false);
  }

  @Test
  public void testServerStore() {
    assertTrue(ss.getExported() instanceof RemoteStore);
  }

  @Test
  public void testOpenConnection() throws Exception {
    BlobStoreConnection con = createMock(BlobStoreConnection.class);
    reset(store);
    expect(store.openConnection(null)).andReturn(con);
    expect(store.openConnection(null)).andThrow(new UnsupportedOperationException());
    replay(store);

    RemoteConnection rc = ss.openConnection();
    assertTrue(rc instanceof ServerConnection);
    assertEquals(con, ((ServerConnection)rc).getConnection());

    try {
      ss.openConnection();
      fail("Failed to rcv expected exception");
    } catch (UnsupportedOperationException e) {
    }

    verify(store);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testStartTransaction() throws Exception {
    BlobStoreConnection con = createMock(BlobStoreConnection.class);

    reset(store);
    expect(store.openConnection(isA(Transaction.class))).andReturn(con);
    replay(store);

    RemoteTransactionListener rtl = ss.startTransactionListener();
    Operation<?> op = rtl.getNextOperation();
    assertTrue(op instanceof Result);

    RemoteConnection rc = ((Result<RemoteConnection>) op).get();
    assertTrue(rc instanceof ServerConnection);
    assertEquals(con, ((ServerConnection)rc).getConnection());

    verify(store);
  }


  @Test
  public void testGetCapabilities() {
    Set<URI> caps =
      new HashSet<URI>(Arrays.asList(BlobStore.TXN_CAPABILITY, BlobStore.GENERATE_ID_CAPABILITY));

    reset(store);
    expect(store.getCapabilities()).andReturn(caps);
    replay(store);

    assertEquals(caps, ss.getCapabilities());
    verify(store);
  }

  @Test
  public void testSetQuiescent() throws IOException {
    reset(store);
    expect(store.setQuiescent(true)).andReturn(true);
    replay(store);

    assertTrue(ss.setQuiescent(true));
    verify(store);
  }
}
