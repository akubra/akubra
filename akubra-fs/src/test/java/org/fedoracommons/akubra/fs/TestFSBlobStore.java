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

import java.util.ArrayList;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import org.fedoracommons.akubra.BlobStore;

import static org.testng.Assert.assertEquals;

/**
 * Unit tests for {@link FSBlobStore}.
 *
 * @author Chris Wilper
 */
public class TestFSBlobStore {
  private static URI id;

  private static File tmpDir;
  private static FSBlobStore store;

  @BeforeClass
  public static void init() throws Exception {
    id = new URI("urn:example:store");
    tmpDir = FSTestUtil.createTempDir();
    store = new FSBlobStore(id, tmpDir);
  }

  @AfterClass
  public static void destroy() {
    FSTestUtil.rmdir(tmpDir);
  }

  /**
   * Should return an empty list.
   */
  @Test
  public void testGetBackingStores() {
    assertEquals(store.getBackingStores().size(), 0);
  }

  /**
   * Not supported; should fail.
   */
  @Test (expectedExceptions={ UnsupportedOperationException.class })
  public void testSetBackingStores() {
    store.setBackingStores(new ArrayList<BlobStore>());
  }

  /**
   * Should be empty; test when implemented.
   */
  @Test
  public void testGetDeclaredCapabilities() {
    assertEquals(store.getDeclaredCapabilities().size(), 1);
    assertEquals(store.getDeclaredCapabilities().iterator().next(), BlobStore.GENERATE_ID_CAPABILITY);
  }

  /**
   * Should be empty; test when implemented.
   */
  @Test
  public void testGetCapabilities() {
    assertEquals(store.getCapabilities().size(), 1);
    assertEquals(store.getCapabilities().iterator().next(), BlobStore.GENERATE_ID_CAPABILITY);
  }
}
