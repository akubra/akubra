/* $HeadURL::                                                                            $
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
package org.fedoracommons.akubra.map;

import java.net.URI;

import org.fedoracommons.akubra.BlobStore;
import org.fedoracommons.akubra.mem.MemBlobStore;
import org.fedoracommons.akubra.tck.TCKTestSuite;

/**
 * TCK test suite for {@link IdMappingBlobStore}.
 *
 * @author Chris Wilper;
 */
public class IdMappingBlobStoreTCKTest extends TCKTestSuite {

  private static final URI testStoreId = URI.create("urn:test-store");

  public IdMappingBlobStoreTCKTest() {
    super(getTestStore(), testStoreId, false, true);
  }

  private static BlobStore getTestStore() {
    return new IdMappingBlobStore(testStoreId, new MemBlobStore(), new MockIdMapper());
  }

  @Override
  protected URI[] getAliases(URI uri) {
    // for underlying mem store, all uris are distinct
    return new URI[] { uri };
  }

  @Override
  protected URI getInvalidId() {
    // for underlying mem store, all uris are valid
    return null;
  }

}
