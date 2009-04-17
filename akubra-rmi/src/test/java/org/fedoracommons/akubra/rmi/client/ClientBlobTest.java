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
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.classextension.EasyMock.createMock;
import static org.easymock.classextension.EasyMock.makeThreadSafe;
import static org.easymock.classextension.EasyMock.replay;
import static org.easymock.classextension.EasyMock.reset;
import static org.easymock.classextension.EasyMock.verify;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import java.net.URI;

import java.util.Iterator;
import java.util.Map;

import org.fedoracommons.akubra.Blob;
import org.fedoracommons.akubra.BlobStoreConnection;
import org.fedoracommons.akubra.MissingBlobException;
import org.fedoracommons.akubra.UnsupportedIdException;
import org.fedoracommons.akubra.impl.AbstractBlobStoreConnection;
import org.fedoracommons.akubra.impl.StreamManager;
import org.fedoracommons.akubra.rmi.remote.RemoteBlob;
import org.fedoracommons.akubra.rmi.server.Exporter;
import org.fedoracommons.akubra.rmi.server.ServerBlob;

import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

/**
 * Unit tests for ClientBlob.
 *
 * @author Pradeep Krishnan
  */
public class ClientBlobTest {
  private Exporter   exporter;
  private Blob       blob;
  private ServerBlob sb;
  private ClientBlob cb;

  @BeforeSuite
  public void setUp() throws Exception {
    exporter   = new Exporter(0);
    blob       = createMock(Blob.class);

    sb         = new ServerBlob(blob, exporter);

    BlobStoreConnection con =
      new AbstractBlobStoreConnection(null) {
        public Blob getBlob(URI blobId, Map<String, String> hints)
                     throws IOException, UnsupportedIdException {
          return null;
        }

        public Iterator<URI> listBlobIds(String filterPrefix)
                                  throws IOException {
          return null;
        }
      };

    cb = new ClientBlob(con, new StreamManager(), (RemoteBlob) sb.getExported(), null);
  }

  @AfterSuite
  public void tearDown() throws Exception {
    sb.unExport(false);
  }

  @Test
  public void testOpenInputStream() throws IOException {
    URI         id = URI.create("foo:bar");
    InputStream in = createMock(InputStream.class);
    makeThreadSafe(in, true);

    reset(blob);
    makeThreadSafe(blob, true);
    expect(blob.openInputStream()).andReturn(in);
    expect(blob.openInputStream()).andThrow(new MissingBlobException(id));

    expect(in.read()).andReturn(42);
    in.close();
    expectLastCall().atLeastOnce();
    replay(blob);
    replay(in);

    InputStream ci = cb.openInputStream();
    assertNotNull(ci);
    assertEquals(42, ci.read());
    ci.close();

    try {
      cb.openInputStream();
      fail("Failed to rcv expected exception");
    } catch (MissingBlobException e) {
      assertEquals(id, e.getBlobId());
    }

    verify(blob);
    verify(in);
  }

  @Test
  public void testOpenOutputStream() throws IOException {
    URI          id  = URI.create("foo:bar");
    OutputStream out = createMock(OutputStream.class);
    makeThreadSafe(out, true);

    reset(blob);
    makeThreadSafe(blob, true);
    expect(blob.openOutputStream(42L)).andReturn(out);
    expect(blob.openOutputStream(-1L)).andThrow(new MissingBlobException(id));

    out.write(42);
    out.close();
    expectLastCall().atLeastOnce();
    replay(blob);
    replay(out);

    OutputStream co = cb.openOutputStream(42L);
    assertNotNull(co);
    co.write(42);
    co.close();

    try {
      cb.openOutputStream(-1L);
      fail("Failed to rcv expected exception");
    } catch (MissingBlobException e) {
      assertEquals(id, e.getBlobId());
    }

    verify(blob);
    verify(out);
  }

  @Test
  public void testGetSize() throws IOException {
    reset(blob);
    expect(blob.getSize()).andReturn(42L);
    replay(blob);

    assertEquals(42L, cb.getSize());
    verify(blob);
  }

  @Test
  public void testExists() throws IOException {
    reset(blob);
    expect(blob.exists()).andReturn(true);
    replay(blob);

    assertTrue(cb.exists());
    verify(blob);
  }

  @Test
  public void testCreate() throws IOException {
    reset(blob);
    blob.create();
    replay(blob);

    cb.create();
    verify(blob);
  }

  @Test
  public void testDelete() throws IOException {
    reset(blob);
    blob.delete();
    replay(blob);

    sb.delete();
    verify(blob);
  }

  @Test
  public void testMoveTo() throws IOException {
    URI                 id1   = URI.create("foo:1");
    URI                 id2   = URI.create("foo:2");
    BlobStoreConnection con   = createMock(BlobStoreConnection.class);
    Blob                blob2 = createMock(Blob.class);

    reset(blob);
    expect(blob.getConnection()).andStubReturn(con);
    expect(blob2.getConnection()).andStubReturn(con);

    expect(blob.getId()).andStubReturn(id1);
    expect(blob2.getId()).andStubReturn(id2);

    expect(con.getBlob(id1, null)).andStubReturn(blob);
    expect(con.getBlob(id2, null)).andStubReturn(blob2);

    blob.moveTo(blob);
    blob.moveTo(blob2);
    replay(blob);
    replay(blob2);
    replay(con);

    ServerBlob sb  = new ServerBlob(blob, exporter);
    ClientBlob cb  =
      new ClientBlob(this.cb.getConnection(), new StreamManager(), (RemoteBlob) sb.getExported(),
                     null);

    ServerBlob sb2 = new ServerBlob(blob2, exporter);
    ClientBlob cb2 =
      new ClientBlob(cb.getConnection(), new StreamManager(), (RemoteBlob) sb2.getExported(), null);

    cb.moveTo(cb);
    cb.moveTo(cb2);

    try {
      cb.moveTo(null);
      fail("Failed to rcv expected exception");
    } catch (Exception e) {
    }

    verify(blob);

    sb.unExport(false);
    sb2.unExport(false);
  }
}
