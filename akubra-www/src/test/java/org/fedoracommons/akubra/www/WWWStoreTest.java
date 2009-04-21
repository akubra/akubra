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

import java.util.ArrayList;

import org.fedoracommons.akubra.BlobStore;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

/**
 * Unit Test for WWWStore.
 *
 * @author Pradeep Krishnan
 */
public class WWWStoreTest {
  private URI       id;
  private BlobStore store;

  /**
   * Set up a WWWStore for testing.
   *
   * @throws Exception on an error
   */
  @BeforeSuite
  public void setUp() throws Exception {
    store = new WWWStore(id = URI.create("urn:www:test"));
  }

  /**
   * Tear down the store after testing.
   *
   * @throws Exception on an error
   */
  @AfterSuite
  public void tearDown() throws Exception {
  }

  /**
   * Test the getId operation.
   */
  @Test
  public void testGetId() {
    assertEquals(id, store.getId());
  }

  /**
   * Test the getBackingStores operation.
   */
  @Test
  public void testGetBackingStores() {
    assertTrue(store.getBackingStores().isEmpty());
  }

  /**
   * Test the setBackingStores operation.
   *
   * @throws UnsupportedOperationException DOCUMENT ME!
   */
  @Test(expectedExceptions =  {
    UnsupportedOperationException.class}
  )
  public void testSetBackingStores() throws UnsupportedOperationException {
    store.setBackingStores(new ArrayList<BlobStore>());
    fail("setBackingStores() failed");
  }

  /**
   * Test the setQuiescent operation.
   */
  @Test
  public void testSetQuiescent() {
    try {
      assertFalse(store.setQuiescent(true));
      assertFalse(store.setQuiescent(false));
    } catch (IOException e) {
      fail("setQuiescent() failed", e);
    }
  }

  /**
   * Test getDeclaredCapabilities
   */
  @Test
  public void testDeclaredCapabilities() {
    assertEquals(0, store.getDeclaredCapabilities().size());
  }

  /**
   * Test getCapabilities
   */
  @Test
  public void testCapabilities() {
    assertEquals(0, store.getCapabilities().size());
  }
}
