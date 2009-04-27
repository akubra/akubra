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
import org.fedoracommons.akubra.BlobStoreConnection;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.fail;

import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

/**
 * Unit tests for WWWBlob.
 *
 * @author Pradeep Krishnan
 */
public class WWWBlobTest {
  private BlobStoreConnection con;
  private URI                 blobId;
  private Blob                blob;

  /**
   * Set up the blob for testing.
   *
   * @throws Exception on an error
   */
  @BeforeSuite
  public void setUp() throws Exception {
    WWWStore store = new WWWStore(URI.create("urn:www:test"));
    con      = store.openConnection(null, null);
    blobId   = URI.create("http://www.google.com");
    blob     = con.getBlob(blobId, null);
  }

  /**
   * Tear down the blob after testing.
   *
   * @throws Exception on an error
   */
  @AfterSuite
  public void tearDown() throws Exception {
    con.close();
  }

  /**
   * Test blob creation and caching.
   */
  @Test
  public void testWWWBlob() {
    try {
      assertEquals(blob, con.getBlob(blobId, null));
    } catch (IOException e) {
      fail("getBlob() failed", e);
    }
  }

  /**
   * Test the connection back pointer.
   */
  @Test
  public void testGetConnection() {
    assertEquals(con, blob.getConnection());
  }

  /**
   * Test getId method.
   */
  @Test
  public void testGetId() {
    assertEquals(blobId, blob.getId());
  }

  /**
   * Test getSize method.
   */
  @Test
  public void testGetSize() {
    try {
      assertEquals(-1, blob.getSize());
    } catch (IOException e) {
      fail("getSize() failed", e);
    }
  }

  /**
   * Test openInputStream() method.
   */
  @Test
  public void testOpenInputStream() {
    try {
      assertNotNull(blob.openInputStream());
    } catch (IOException e) {
      fail("openInputStream() failed", e);
    }
  }

  /**
   * Test openOutputStream method.
   */
  @Test
  public void testOpenOutputStream() {
    try {
      assertNotNull(blob.openOutputStream(-1, true));
    } catch (IOException e) {
      fail("openOutputStream() failed", e);
    }
  }
}
