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
package org.fedoracommons.akubra.fs;

import java.io.File;

import java.net.URI;
import java.net.URISyntaxException;

import org.fedoracommons.akubra.UnsupportedIdException;
import org.fedoracommons.akubra.impl.StreamManager;
import org.fedoracommons.akubra.util.DefaultPathAllocator;
import org.fedoracommons.akubra.util.PathAllocator;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * Unit tests for {@link FSBlob}.
 *
 * @author Chris Wilper
 */
public class TestFSBlob {
  private static URI id;

  private static File baseDir;
  private static FSBlobStoreConnection conn;
  private static StreamManager mgr;

  @BeforeClass
  public static void init() throws Exception {
    id = new URI("urn:example:store");
    baseDir = FSTestUtil.createTempDir();
    FSBlobStore store = new FSBlobStore(id, baseDir);
    PathAllocator pAlloc = new DefaultPathAllocator();
    mgr = new StreamManager();
    conn = new FSBlobStoreConnection(store, baseDir, pAlloc, mgr);
  }

  @AfterClass
  public static void destroy() {
    conn.close();
    FSTestUtil.rmdir(baseDir);
  }

  /**
   * Blobs that don't exist should report that they don't exist.
   */
  @Test
  public void testExistsFalse() throws Exception {
    assertFalse(getFSBlob("nonExistingPath").exists());
    assertFalse(getFSBlob("nonExistingPath/nonExistingPath").exists());
  }

  /**
   * Test that various valid blob ids validate correctly.
   */
  @Test
  public void testValidateGoodIds() {
    assertTrue(isValidId("file:foo"));
    assertTrue(isValidId("file:foo/bar"));
    assertTrue(isValidId("file:..."));
    assertTrue(isValidId("file:.../foo"));
    assertTrue(isValidId("file:foo/..."));
    assertTrue(isValidId("file:foo/.../bar"));
  }

  /**
   * Test that various invalid blob ids fail validation.
   */
  @Test
  public void testValidateBadIds() {
    assertFalse(isValidId(null));
    assertFalse(isValidId("urn:foo"));
    assertFalse(isValidId("file:/foo"));
    assertFalse(isValidId("file://foo"));
    assertFalse(isValidId("file:///foo"));
    assertFalse(isValidId("file:foo//bar"));
    assertFalse(isValidId("file:foo/bar/"));
    assertFalse(isValidId("file:foo/bar//"));
    assertFalse(isValidId("file:.."));
    assertFalse(isValidId("file:../foo"));
    assertFalse(isValidId("file:foo/.."));
    assertFalse(isValidId("file:foo/../bar"));
  }

  private static FSBlob getFSBlob(String path) {
    URI blobId;
    try {
      blobId = new URI("file:" + path);
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
    return new FSBlob(conn,
                      blobId,
                      new File(baseDir, path),
                      mgr);
  }

  private static boolean isValidId(String id) {
    try {
      URI uri = null;
      if (id != null) {
        uri = new URI(id);
      }
      FSBlob.validateId(uri);
      return true;
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    } catch (NullPointerException e) {
      return false;
    } catch (UnsupportedIdException e) {
      return false;
    }
  }

}
