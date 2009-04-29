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

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.isA;
import static org.easymock.EasyMock.isNull;
import static org.easymock.classextension.EasyMock.createMock;
import static org.easymock.classextension.EasyMock.makeThreadSafe;
import static org.easymock.classextension.EasyMock.replay;
import static org.easymock.classextension.EasyMock.reset;
import static org.easymock.classextension.EasyMock.verify;

import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.io.IOException;

import java.net.URI;

import java.util.Map;
import javax.transaction.Transaction;

import org.fedoracommons.akubra.BlobStore;
import org.fedoracommons.akubra.BlobStoreConnection;
import org.fedoracommons.akubra.rmi.remote.RemoteStore;
import org.fedoracommons.akubra.rmi.server.Exporter;
import org.fedoracommons.akubra.rmi.server.ServerStore;
import org.fedoracommons.akubra.rmi.server.ServerTransactionListener;

import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

/**
 * Unit tests for ClientStore.
 *
 * @author Pradeep Krishnan
  */
public class ClientStoreTest {
  private Exporter    exporter;
  private BlobStore   store;
  private ServerStore ss;
  private ClientStore cs;

  @BeforeSuite
  public void setUp() throws Exception {
    exporter   = new Exporter(0);
    store      = createMock(BlobStore.class);

    ss   = new ServerStore(store, exporter);
    cs   = new ClientStore(URI.create("urn:rmi-client"), (RemoteStore) ss.getExported());
  }

  @AfterSuite
  public void tearDown() throws Exception {
    ss.unExport(false);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testOpenConnection() throws IOException {
    BlobStoreConnection con = createMock(BlobStoreConnection.class);
    Transaction         tx  = createMock(Transaction.class);

    reset(store);
    makeThreadSafe(store, true);
    expect(store.openConnection(null, null)).andThrow(new UnsupportedOperationException());
    expect(store.openConnection(isA(ServerTransactionListener.class), (Map) isNull())).
        andReturn(con);
    replay(store);

    try {
      cs.openConnection(null, null);
      fail("Failed to rcv expected exception");
    } catch (UnsupportedOperationException e) {
    }

    BlobStoreConnection rc = cs.openConnection(tx, null);
    assertTrue(rc instanceof ClientConnection);

    verify(store);
  }

  @Test
  public void testSetQuiescent() throws IOException {
    reset(store);
    expect(store.setQuiescent(true)).andReturn(true);
    replay(store);

    assertTrue(cs.setQuiescent(true));
    verify(store);
  }
}
