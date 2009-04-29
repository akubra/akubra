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

import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.isA;
import static org.easymock.classextension.EasyMock.createMock;
import static org.easymock.classextension.EasyMock.makeThreadSafe;
import static org.easymock.classextension.EasyMock.replay;
import static org.easymock.classextension.EasyMock.reset;
import static org.easymock.classextension.EasyMock.verify;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import java.io.IOException;
import java.io.PipedInputStream;

import java.net.URI;

import java.rmi.RemoteException;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.fedoracommons.akubra.Blob;
import org.fedoracommons.akubra.BlobStoreConnection;
import org.fedoracommons.akubra.UnsupportedIdException;
import org.fedoracommons.akubra.rmi.remote.RemoteBlob;
import org.fedoracommons.akubra.rmi.remote.RemoteBlobCreator;
import org.fedoracommons.akubra.rmi.remote.RemoteConnection;
import org.fedoracommons.akubra.rmi.remote.RemoteIterator;

import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

/**
 * Unit tests for ServerConnection.
 *
 * @author Pradeep Krishnan
  */
public class ServerConnectionTest {
  private Exporter            exporter;
  private ServerConnection    sc;
  private BlobStoreConnection con;

  @BeforeSuite
  public void setUp() throws Exception {
    exporter   = new Exporter(0);
    con        = createMock(BlobStoreConnection.class);
    sc         = new ServerConnection(con, exporter);
  }

  @AfterSuite
  public void tearDown() throws Exception {
    sc.unExport(false);
  }

  @Test
  public void testServerConnection() {
    assertTrue(sc.getExported() instanceof RemoteConnection);
  }

  @Test
  public void testGetBlobURIMapOfStringString() throws IOException {
    URI                 id    = URI.create("foo:bar");
    Map<String, String> hints = new HashMap<String, String>();
    hints.put("try harder?", "yes, of course!");

    Blob blob = createMock(Blob.class);

    reset(con);
    expect(con.getBlob(id, hints)).andReturn(blob);
    expect(con.getBlob(id, hints)).andThrow(new UnsupportedIdException(id));
    replay(con);

    RemoteBlob rb = sc.getBlob(id, hints);
    assertTrue(rb instanceof ServerBlob);
    assertEquals(blob, ((ServerBlob) rb).getBlob());

    try {
      sc.getBlob(id, hints);
      fail("Failed to rcv expected exception");
    } catch (UnsupportedIdException e) {
      assertEquals(id, e.getBlobId());
    }

    verify(con);
  }

  @Test
  public void testGetBlobCreator() throws IOException {
    Map<String, String> hints = new HashMap<String, String>();
    hints.put("try harder?", "yes, of course!");

    Blob blob = createMock(Blob.class);

    reset(con);
    makeThreadSafe(con, true);

    expect(con.getBlob(isA(PipedInputStream.class), eq(42L), eq(hints))).andReturn(blob);
    expect(con.getBlob(isA(PipedInputStream.class), eq(-1L), eq(hints)))
       .andThrow(new UnsupportedOperationException());

    replay(con);
    RemoteBlobCreator rbc = sc.getBlobCreator(42L, hints);
    assertTrue(rbc instanceof ServerBlobCreator);
    RemoteBlob rb = rbc.shutDown(false);
    assertTrue(rb instanceof ServerBlob);
    assertEquals(blob, ((ServerBlob)rb).getBlob());

    try {
      sc.getBlobCreator(-1L, hints).shutDown(false);
      fail("Failed to rcv expected exception");
    } catch (UnsupportedOperationException e) {
    }

    verify(con);
  }

  @Test
  public void testListBlobIds() throws IOException {
    URI           id = URI.create("foo:bar");
    Iterator<URI> it = Arrays.asList(id).iterator();

    reset(con);
    expect(con.listBlobIds(null)).andReturn(it);
    replay(con);

    RemoteIterator<URI> ri = sc.listBlobIds(null);
    assertTrue(ri instanceof ServerIterator);
    assertEquals(it, ((ServerIterator<URI>) ri).getIterator());
    verify(con);
  }

  @Test
  public void testClose() throws RemoteException {
    ServerConnection sc = new ServerConnection(con, exporter);

    reset(con);
    con.close();
    replay(con);

    assertNotNull(sc.getExported());
    sc.close();
    assertNull(sc.getExported());
    verify(con);
  }

  @Test
  public void testUnreferenced() throws RemoteException {
    ServerConnection sc = new ServerConnection(con, exporter);

    reset(con);
    con.close();
    replay(con);

    assertNotNull(sc.getExported());
    sc.unreferenced();
    assertNull(sc.getExported());
    verify(con);
  }
}
