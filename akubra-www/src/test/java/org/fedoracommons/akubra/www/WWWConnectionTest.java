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
package org.fedoracommons.akubra.www;

import java.io.IOException;

import java.net.URI;

import org.fedoracommons.akubra.Blob;
import org.fedoracommons.akubra.BlobStore;
import org.fedoracommons.akubra.BlobStoreConnection;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.fail;

import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

/**
 * Unit Test for WWWConnection.
 *
 * @author Pradeep Krishnan
 */
public class WWWConnectionTest {
  private BlobStore           store;
  private BlobStoreConnection con;
  private BlobStoreConnection closeableCon;

  /**
   * Setup a connection for testing.
   *
   * @throws Exception on an error
   */
  @BeforeSuite
  public void setUp() throws Exception {
    store          = new WWWStore(URI.create("urn:www:test"));
    con            = store.openConnection(null, null);
    closeableCon   = store.openConnection(null, null);
  }

  /**
   * Tear down the connection used by tests.
   *
   * @throws Exception on an error
   */
  @AfterSuite
  public void tearDown() throws Exception {
    con.close();
  }

  /**
   * Test the getBlob operation.
   */
  @Test
  public void testGetBlob() {
    try {
      Blob blob1 = con.getBlob(URI.create("http://www.google.com"), null);
      assertNotNull(blob1);

      Blob blob2 = con.getBlob(URI.create("http://www.google.com"), null);
      assertNotNull(blob2);
      assertEquals(blob1, blob2);
    } catch (IOException e) {
      fail("createBlob() failed", e);
    }
  }

  /**
   * Test the getBlobStore operation.
   */
  @Test
  public void testGetBlobStore() {
    assertEquals(store, con.getBlobStore());
  }

  /**
   * Test the listBlobIds operation.
   */
  @Test(expectedExceptions =  { UnsupportedOperationException.class })
  public void testListBlobIds() throws IOException {
    con.listBlobIds(null);
  }

  /**
   * Test the close operation.
   *
   * @throws IllegalStateException the expected exception on a getBlob() from a closed Connection
   */
  @Test(expectedExceptions =  { IllegalStateException.class })
  public void testClose() throws IOException {
    closeableCon.close();
    closeableCon.getBlob(URI.create("http://www.google.com"), null);
  }
}
