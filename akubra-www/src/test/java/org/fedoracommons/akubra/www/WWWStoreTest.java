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

import org.fedoracommons.akubra.BlobStore;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
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
}
