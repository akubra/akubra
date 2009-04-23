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
package org.fedoracommons.akubra.rmi;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import java.net.URI;

import java.util.Iterator;

import org.fedoracommons.akubra.Blob;
import org.fedoracommons.akubra.BlobStore;
import org.fedoracommons.akubra.BlobStoreConnection;
import org.fedoracommons.akubra.DuplicateBlobException;
import org.fedoracommons.akubra.MissingBlobException;
import org.fedoracommons.akubra.mem.MemBlobStore;

import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

/**
 * Tests on BlobStoreConnection on a MemStore exported via RMI.
 *
 * @author Pradeep Krishnan
  */
public class MemStoreConnectionTest {
  private BlobStore           mem;
  private BlobStoreConnection con;
  private AkubraRMIServer     server;

  /**
   * Starts up the server and create a client connection.
   *
   * @throws Exception on an error
   */
  @BeforeSuite
  public void setUp() throws Exception {
    mem = new MemBlobStore();
    int reg = ServiceTest.freePort();
    server = new AkubraRMIServer(mem, reg);

    BlobStore store = AkubraRMIClient.create(reg);
    con = store.openConnection(null);
    assertEquals(store, con.getBlobStore());
  }

  /**
   * Close the connection and unexport server.
   *
   * @throws Exception on an error
   */
  @AfterSuite
  public void tearDown() throws Exception {
    con.close();
    server.shutDown(true);
  }

  /**
   * Tests the generation of a new id.
   *
   * @throws IOException on an error
   */
  @Test
  public void testGenerateBlob() throws IOException {
    Blob blob = con.getBlob(null, null);
    assertNotNull(blob);
    assertNotNull(blob.getId());
    assertEquals(con, blob.getConnection());
  }

  /**
   * Tests getting a blob with the given id.
   *
   * @throws IOException on an error
   */
  @Test
  public void testUseAppBlob() throws IOException {
    Blob blob = con.getBlob(URI.create("foo:bar"), null);
    assertNotNull(blob);
    assertEquals(URI.create("foo:bar"), blob.getId());
    assertEquals(con, blob.getConnection());
  }

  /**
   * Tests getting a new blob with the given content as a content-addressible-storage would do.
   *
   * @throws IOException on an error
   */
  @Test
  public void testCASBlob() throws IOException {
    byte[] buf  = new byte[] { 1, 3, 5, 7, 11, 13, 17, 23 };
    Blob   blob = con.getBlob(new ByteArrayInputStream(buf), buf.length, null);
    assertNotNull(blob);
    assertNotNull(blob.getId());
    assertEquals(con, blob.getConnection());
    assertEquals(buf.length, blob.getSize());

    byte[]      read = new byte[buf.length];
    InputStream in   = blob.openInputStream();
    assertEquals(buf.length, in.read(read));
    assertEquals(-1, in.read());
    in.close();
  }

  /**
   * Tests the listing of blob-ids.
   *
   * @throws IOException on an error
   */
  @Test
  public void testListBlobIds() throws IOException {
    for (Iterator<URI> it = con.listBlobIds(null); it.hasNext();) {
      URI id = it.next();
      con.getBlob(id, null).delete();
    }

    assertFalse(con.listBlobIds(null).hasNext());

    Blob b1 = con.getBlob(null, null);

    b1.openOutputStream(0, true).close();

    Iterator<URI> it = con.listBlobIds(null);
    assertTrue(it.hasNext());
    assertEquals(b1.getId(), it.next());
    assertFalse(it.hasNext());
  }

  /**
   * Tests renames.
   *
   * @throws IOException on an error
   */
  @Test
  public void testMove() throws IOException {
    Blob b1 = con.getBlob(null, null);
    Blob b2 = con.getBlob(null, null);

    b1.openOutputStream(0, true).close();

    if (b2.exists())
      b2.delete();

    assertTrue(b1.exists());
    assertFalse(b2.exists());

    assertEquals(b2, b1.moveTo(b2.getId(), null));

    assertTrue(b2.exists());
    assertFalse(b1.exists());

    assertEquals(b1, b2.moveTo(b1.getId(), null));

    assertTrue(b1.exists());
    assertFalse(b2.exists());

    b2.openOutputStream(0, true).close();
    assertTrue(b2.exists());

    try {
      b2.moveTo(b1.getId(), null);
      fail("Failed to rcv exepected exception");
    } catch (DuplicateBlobException e) {
    }

    b1.delete();
    assertFalse(b1.exists());

    b2.delete();
    assertFalse(b2.exists());

    try {
      b2.moveTo(b1.getId(), null);
      fail("Failed to rcv exepected exception");
    } catch (MissingBlobException e) {
    }
  }
}
