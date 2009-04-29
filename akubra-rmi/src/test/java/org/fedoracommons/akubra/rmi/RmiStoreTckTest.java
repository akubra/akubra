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

import java.io.File;
import java.io.IOException;

import java.net.URI;

import org.apache.commons.io.FileUtils;

import org.fedoracommons.akubra.BlobStore;
import org.fedoracommons.akubra.mem.MemBlobStore;
import org.fedoracommons.akubra.tck.TCKTestSuite;
import org.fedoracommons.akubra.txn.derby.TransactionalStore;

import org.testng.annotations.AfterSuite;
import org.testng.annotations.Factory;

/**
 * TCK test suite for RMI client stores.
 *
 * @author Pradeep Krishnan
 */
public class RmiStoreTckTest {
  private AkubraRMIServer[] servers;

  /**
   * Dynamically creates the test suits.
   *
   * @return the tests to run
   *
   * @throws Exception on any error
   */
  @Factory
  public Object[] createTests() throws Exception {
    int       reg     = ServiceTest.freePort();
    String    server1 = "rmi-tck-server-1";
    String    server2 = "rmi-tck-server-2";
    BlobStore store1  = new MemBlobStore(URI.create("urn:store:1"));
    BlobStore store2  = new MemBlobStore(URI.create("urn:store:2"));
    BlobStore store3  = createTxnStore("rmi-txn-test-1", store2);

    servers = new AkubraRMIServer[] {
                new AkubraRMIServer(store1, server1, reg),
                new AkubraRMIServer(store3, server2, reg),
              };

    BlobStore client1 = AkubraRMIClient.create(server1, reg);
    BlobStore client2 = AkubraRMIClient.create(server2, reg);

    return new Object[] {
             new RmiStoreTestSuite(client1, client1.getId(), false, true),
             new RmiStoreTestSuite(client2, client2.getId(), true, false),
           };
  }

  /**
   * Shuts down the RMI servers that we started.
   *
   * @throws Exception on an error
   */
  @AfterSuite
  public void tearDown() throws Exception {
    for (AkubraRMIServer server : servers)
      server.shutDown(true);
  }

  private BlobStore createTxnStore(String name, BlobStore backingStore)
                            throws IOException {
    File base  = new File(System.getProperty("basedir"), "target");
    File dbDir = new File(base, name);
    FileUtils.deleteDirectory(dbDir);
    dbDir.getParentFile().mkdirs();

    System.setProperty("derby.stream.error.file", new File(base, "derby.log").toString());

    BlobStore store =
      new TransactionalStore(URI.create("urn:" + name), backingStore, dbDir.getPath());

    return store;
  }

  /**
   * Our TCK test suite
   */
  private static class RmiStoreTestSuite extends TCKTestSuite {
    public RmiStoreTestSuite(BlobStore store, URI storeId, boolean isTransactional,
                             boolean supportsIdGen) {
      super(store, storeId, isTransactional, supportsIdGen);
    }

    @Override
    protected URI getInvalidId() {
      return null; // all ids are valid
    }

    /**
     * all URI's are distinct
     *
     * @param uri the uri
     *
     * @return aliases
     */
    @Override
    protected URI[] getAliases(URI uri) {
      return new URI[] { uri };
    }

    @Override
    public void testSetQuiescent() throws Exception {
      // rmi threads are not interruptible. So skip this test
    }
  }
}
