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
package org.fedoracommons.akubra.qsc;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;

import org.apache.commons.io.FileUtils;

import org.testng.annotations.Factory;
import org.testng.annotations.Test;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import org.fedoracommons.akubra.AkubraException;
import org.fedoracommons.akubra.Blob;
import org.fedoracommons.akubra.BlobStore;
import org.fedoracommons.akubra.BlobStoreConnection;
import org.fedoracommons.akubra.mem.MemBlobStore;
import org.fedoracommons.akubra.tck.TCKTestSuite;
import org.fedoracommons.akubra.txn.derby.TransactionalStore;

/**
 * TCK test suite for {@link QuiescingBlobStore}.
 *
 * @author Chris Wilper;
 */
public class QuiescingBlobStoreTCKTest {
  @Factory
  public Object[] createTests() throws Exception {
    URI storeId1 = URI.create("urn:qsc-tck-test:42");
    URI storeId2 = URI.create("urn:qsc-tck-test:43");

    BlobStore nonTxnStore = new MemBlobStore(URI.create("urn:store:1"));
    BlobStore txnStore    =
                  createTxnStore("qsc-txn-text-1", new MemBlobStore(URI.create("urn:store:2")));

    return new Object[] {
      new QuiescingBlobStoreTestSuite(new QuiescingBlobStore(storeId1, nonTxnStore), storeId1,
                                      false, true),
      new QuiescingBlobStoreTestSuite(new QuiescingBlobStore(storeId2, txnStore), storeId2,
                                      true, false),
    };
  }

  private BlobStore createTxnStore(String name, BlobStore backingStore) throws IOException {
    File base  = new File(System.getProperty("basedir"), "target");
    File dbDir = new File(base, name);
    FileUtils.deleteDirectory(dbDir);
    dbDir.getParentFile().mkdirs();

    System.setProperty("derby.stream.error.file", new File(base, "derby.log").toString());

    BlobStore store = new TransactionalStore(URI.create("urn:" + name), backingStore, dbDir.getPath());
    return store;
  }


  public static class QuiescingBlobStoreTestSuite extends TCKTestSuite {
    public QuiescingBlobStoreTestSuite(BlobStore store, URI storeId, boolean isTransactional,
                                       boolean supportsIdGen) {
      super(store, storeId, isTransactional, supportsIdGen);
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

    /**
     * Request to go quiescent and non-quiescent (even when already in those
     * states) should be supported.
     */
    @Test(groups={ "store" }, dependsOnGroups={ "init" })
    public void testSetQuiescent() throws Exception {
      // basic set (non-)quiescent
      assertTrue(((QuiescingBlobStore) store).setQuiescent(true));
      assertTrue(((QuiescingBlobStore) store).setQuiescent(true));
      assertTrue(((QuiescingBlobStore) store).setQuiescent(false));
      assertTrue(((QuiescingBlobStore) store).setQuiescent(false));

      // in quiescent state: read-only should proceed, write should block
      final URI id1 = createId("blobSetQuiescent1");
      final URI id2 = createId("blobSetQuiescent2");

      createBlob(id1, "bar", true);

      runTests(new ConAction() {
          public void run(BlobStoreConnection con) throws Exception {
            final Blob b1 = getBlob(con, id1, "bar");
            final Blob b2 = getBlob(con, id2, false);

            assertTrue(((QuiescingBlobStore) store).setQuiescent(true));

            try {
              doWithTimeout(new ERunnable() {
                @Override
                public void erun() throws Exception {
                  assertTrue(b1.exists());
                }
              }, 100, true);

              doWithTimeout(new ERunnable() {
                @Override
                public void erun() throws Exception {
                  assertFalse(b2.exists());
                }
              }, 100, true);

              doWithTimeout(new ERunnable() {
                @Override
                public void erun() throws Exception {
                  assertEquals(getBody(b1), "bar");
                }
              }, 100, true);

              if (isOutputSupp) {
                doWithTimeout(new ERunnable() {
                  @Override
                  public void erun() throws Exception {
                    b1.openOutputStream(-1, true);
                  }
                }, 100, false);
              }

              if (isDeleteSupp) {
                doWithTimeout(new ERunnable() {
                  @Override
                  public void erun() throws Exception {
                    b1.delete();
                  }
                }, 100, false);
              }

              if (isMoveToSupp) {
                doWithTimeout(new ERunnable() {
                  @Override
                  public void erun() throws Exception {
                    b1.moveTo(b2.getId(), null);
                  }
                }, 100, false);
              }
            } finally {
              assertTrue(((QuiescingBlobStore) store).setQuiescent(false));
            }
          }
      }, true);

      // going quiescent should wait for in-progress write operations to complete
      if (isOutputSupp) {
        final boolean[] cv     = new boolean[] { false };
        final boolean[] failed = new boolean[] { false };

        final Thread t = doInThread(new ERunnable() {
          @Override
          public void erun() throws Exception {
            waitFor(cv, true, 0);
            try {
              assertTrue(((QuiescingBlobStore) store).setQuiescent(true));
            } finally {
              assertTrue(((QuiescingBlobStore) store).setQuiescent(false));
            }
          }
        }, failed);

        runTests(new ConAction() {
          public void run(BlobStoreConnection con) throws Exception {
            final Blob b1 = getBlob(con, id1, "bar");

            OutputStream os = b1.openOutputStream(-1, true);

            TCKTestSuite.notify(cv, true);

            t.join(100);
            assertTrue(t.isAlive());

            os.close();

            t.join(100);
            if (isTransactional) {
              assertTrue(t.isAlive());
            } else {
              assertFalse(t.isAlive());
              assertFalse(failed[0]);
            }
          }
        }, true);

        if (isTransactional) {
          t.join(100);
          assertFalse(t.isAlive());
          assertFalse(failed[0]);
        }

        assertTrue(((QuiescingBlobStore) store).setQuiescent(false));
      }

      // clean up
      assertTrue(((QuiescingBlobStore) store).setQuiescent(false));
      deleteBlob(id1, "", true);
      assertNoBlobs(getPrefixFor("blobSetQuiescent"));
    }

    private void doWithTimeout(final ERunnable test, final long timeout, final boolean expSucc)
        throws Exception {
      boolean[] failed = new boolean[] { false };

      Thread t = doInThread(new Runnable() {
        public void run() {
          try {
            test.erun();
            if (!expSucc)
              fail("Did not get expected exception");
          } catch (IOException ioe) {
            if (expSucc || ioe instanceof AkubraException)
              throw new RuntimeException(ioe);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }
      }, failed);

      t.join(timeout);
      if (expSucc) {
        assertFalse(t.isAlive());
      } else {
        assertTrue(t.isAlive());
        t.interrupt();
        t.join(timeout);
        assertFalse(t.isAlive(), "operation failed to return after interrupt");
      }

      assertFalse(failed[0]);
    }
  }
}
