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

package org.fedoracommons.akubra.txn.derby;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;

import javax.transaction.TransactionManager;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;

import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import org.fedoracommons.akubra.Blob;
import org.fedoracommons.akubra.BlobStore;
import org.fedoracommons.akubra.BlobStoreConnection;
import org.fedoracommons.akubra.UnsupportedIdException;
import org.fedoracommons.akubra.mem.MemBlobStore;
import org.fedoracommons.akubra.txn.BtmUtils;
import org.fedoracommons.akubra.txn.ConcurrentBlobUpdateException;

/**
 * Unit tests for {@link TransactionalStore}.
 *
 * @author Ronald Tschalär
 */
public class TestTransactionalStore {
  private URI                storeId;
  private File               dbDir;
  private TransactionalStore store;
  private BlobStore          blobStore;
  private TransactionManager tm;

  @BeforeSuite(alwaysRun=true)
  public void init() throws Exception {
    /*
    java.util.logging.LogManager.getLogManager().readConfiguration(
        new java.io.FileInputStream("/tmp/jdklog.properties"));
    */

    // set up store
    storeId = new URI("urn:example:txnstore");

    File base = new File(System.getProperty("basedir"), "target");

    dbDir = new File(base, "txn-db");
    FileUtils.deleteDirectory(dbDir);
    dbDir.getParentFile().mkdirs();

    System.setProperty("derby.stream.error.file", new File(base, "derby.log").toString());
    store = new TransactionalStore(storeId, dbDir.getAbsolutePath());

    // set up transaction manager
    tm = BtmUtils.getTM();
  }

  @AfterSuite
  public void destroy() {
  }

  /**
   * Store id should be what it was initialized with.
   */
  @Test
  public void testGetId() {
    assertEquals(store.getId(), storeId);
  }

  /**
   * A single backing store must be set.
   */
  @Test(groups={ "init" })
  public void testSetBackingStore() throws Exception {
    try {
      store.openConnection(null);
      fail("Did not get expected IllegalStateException on unitialized store");
    } catch (IllegalStateException ise) {
    }

    try {
      store.setBackingStores(null);
      fail("Did not get expected NullPointerException setting null stores");
    } catch (NullPointerException npe) {
    }

    try {
      store.setBackingStores(new ArrayList<BlobStore>());
      fail("Did not get expected IllegalArgumentException setting 0 stores");
    } catch (IllegalArgumentException iae) {
    }

    try {
      store.setBackingStores(Arrays.<BlobStore>asList(new MemBlobStore(), new MemBlobStore()));
      fail("Did not get expected IllegalArgumentException setting 2 stores");
    } catch (IllegalArgumentException iae) {
    }

    blobStore = new MemBlobStore();
    store.setBackingStores(Arrays.<BlobStore>asList(blobStore));

    assertNoBlobs(null);
  }

  /**
   * Request to open a connection without a transaction should fail.
   */
  @Test(dependsOnGroups={ "init" }, expectedExceptions={ IOException.class })
  public void testOpenConnectionNoTransaction() throws IOException {
    store.openConnection(null);
  }

  /**
   * Request to open a connection with a transaction.
   */
  @Test(dependsOnGroups={ "init" })
  public void testOpenConnectionWithTransaction() throws Exception {
    tm.begin();
    try {
      store.openConnection(tm.getTransaction());
    } finally {
      tm.rollback();
    }
  }

  /**
   * Test closing a connection.
   */
  @Test(groups={ "blobs" }, dependsOnGroups={ "init" })
  public void testCloseConnection() throws Exception {
    URI id = URI.create("urn:blobCloseConn1");

    // no operations, commit, close
    doInTxn(new Action() {
        public void run(BlobStoreConnection con) throws Exception {
        }
    }, true);

    // no operations, roll back, close
    doInTxn(new Action() {
        public void run(BlobStoreConnection con) throws Exception {
        }
    }, false);

    // no operations, close before commit
    tm.begin();
    store.openConnection(tm.getTransaction()).close();
    tm.commit();

    // no operations, close before roll back
    tm.begin();
    store.openConnection(tm.getTransaction()).close();
    tm.rollback();

    // one operation, close before commit
    tm.begin();
    BlobStoreConnection con = store.openConnection(tm.getTransaction());
    Blob b = getBlob(con, id, null);
    createBlob(con, b, null);
    con.close();
    tm.commit();

    // one operation, close before roll back
    tm.begin();
    con = store.openConnection(tm.getTransaction());
    b = getBlob(con, id, "");
    deleteBlob(con, b);
    con.close();
    tm.rollback();

    // one operation, commit then close
    deleteBlob(id, "", true);
  }

  /**
   * Request to go quiescent and non-quiescent (even when already in those
   * states) should be supported.
   */
  @Test(dependsOnGroups={ "init" })
  public void testSetQuiescent() throws IOException {
    assertTrue(store.setQuiescent(true));
    assertTrue(store.setQuiescent(true));
    assertTrue(store.setQuiescent(false));
    assertTrue(store.setQuiescent(false));
  }

  /**
   * Should return 1 entry.
   */
  @Test(dependsOnGroups={ "init" })
  public void testGetBackingStores() {
    assertEquals(store.getBackingStores().size(), 1);
  }

  /**
   * Should return transactional and accept-app-id capability.
   */
  @Test
  public void testGetDeclaredCapabilities() {
    Set<URI> caps = store.getDeclaredCapabilities();
    assertEquals(caps.size(), 2);
    assertTrue(caps.contains(BlobStore.TXN_CAPABILITY));
    assertTrue(caps.contains(BlobStore.ACCEPT_APP_ID_CAPABILITY));
  }

  /**
   * Should return the union of capabilities.
   */
  @Test(dependsOnGroups={ "init" })
  public void testGetCapabilities() {
    Set<URI> caps = new HashSet<URI>(store.getCapabilities());

    for (URI cap : store.getDeclaredCapabilities())
      assertTrue(caps.contains(cap));
    for (URI cap : store.getBackingStores().get(0).getCapabilities())
      assertTrue(caps.contains(cap));

    caps.removeAll(store.getDeclaredCapabilities());
    caps.removeAll(store.getBackingStores().get(0).getCapabilities());
    assertEquals(caps.size(), 0);
  }

  /**
   * Basic create, get, rename, delete.
   */
  @Test(groups={ "blobs" }, dependsOnGroups={ "init" })
  public void testBasicCommit() throws Exception {
    URI id = URI.create("urn:blobBasicCommit1");

    createBlob(id, "hello", true);
    getBlob(id, "hello", true);

    URI id2 = URI.create("urn:blobBasicCommit2");
    renameBlob(id, id2, "hello", true);
    getBlob(id, null, true);
    getBlob(id2, "hello", true);

    setBlob(id2, "bye bye", true);
    getBlob(id2, "bye bye", true);

    deleteBlob(id2, "bye bye", true);
    getBlob(id2, null, true);

    assertNoBlobs("urn:blobBasicCommit");
  }

  /**
   * Basic create, get, rename, delete with rollbacks.
   */
  @Test(groups={ "blobs" }, dependsOnGroups={ "init" })
  public void testBasicRollback() throws Exception {
    URI id = URI.create("urn:blobBasicRollback1");

    // roll back a create
    createBlob(id, "hello", false);
    getBlob(id, null, false);

    // create, roll back a rename
    createBlob(id, "hello", true);
    getBlob(id, "hello", true);

    URI id2 = URI.create("urn:blobBasicRollback2");
    renameBlob(id, id2, "hello", false);

    getBlob(id, "hello", true);
    getBlob(id2, null, true);

    // update and roll back
    setBlob(id, "bye bye", false);
    getBlob(id, "hello", true);

    // roll back a delete
    deleteBlob(id, "hello", false);
    getBlob(id, "hello", true);

    // delete
    deleteBlob(id, "hello", true);
    getBlob(id, null, true);

    assertNoBlobs("urn:blobBasicRollback");
  }

  /**
   * Test id validation..
   */
  @Test(groups={ "blobs" }, dependsOnGroups={ "init" })
  public void testIdValidation() throws Exception {
    // just long enough (1000 chars)
    StringBuilder uri = new StringBuilder("urn:blobIdValidation");
    for (int idx = 0; idx < 98; idx++)
      uri.append("oooooooooo");
    URI id = URI.create(uri.toString());

    createBlob(id, null, true);
    deleteBlob(id, "", true);

    // one too long
    id = URI.create(uri.toString() + "x");
    try {
      createBlob(id, null, true);
      fail("Did not get expected UnsupportedIdException");
    } catch (UnsupportedIdException uie) {
      assertEquals(uie.getBlobId(), id);
    }
  }

  /**
   * Test changing a blob's value.
   */
  @Test(groups={ "blobs" }, dependsOnGroups={ "init" })
  public void testBlobUpdate() throws Exception {
    final URI id = URI.create("urn:blobBlobUpdate1");

    doInTxn(new Action() {
        public void run(BlobStoreConnection con) throws Exception {
          Blob b = getBlob(con, id, null);
          createBlob(con, b, null);
          setBlob(con, b, "value1");
          setBlob(con, b, "value2");
          setBlob(con, b, "value3");
        }
    }, true);

    doInTxn(new Action() {
        public void run(BlobStoreConnection con) throws Exception {
          Blob b = getBlob(con, id, "value3");
          setBlob(con, b, "value4");
          setBlob(con, b, "value5");
        }
    }, true);

    deleteBlob(id, "value5", true);
    getBlob(id, null, true);

    assertNoBlobs("urn:blobBlobUpdate");
  }

  /**
   * Test deletions are done and cleaned up properly under various combinations of
   * creating/moving/deleting blobs.
   */
  @Test(groups={ "blobs" }, dependsOnGroups={ "init" })
  public void testDeleteCleanup() throws Exception {
    final URI    id   = URI.create("urn:blobBlobDelete1");
    final URI    id2  = URI.create("urn:blobBlobDelete2");
    final String body = "value";

    // create-delete in one txn
    doInTxn(new Action() {
        public void run(BlobStoreConnection con) throws Exception {
          Blob b = getBlob(con, id, null);
          createBlob(con, b, null);
          deleteBlob(con, b);
        }
    }, true);

    // create-delete-create in one txn
    doInTxn(new Action() {
        public void run(BlobStoreConnection con) throws Exception {
          Blob b = getBlob(con, id, null);
          createBlob(con, b, null);
          deleteBlob(con, b);
          createBlob(con, b, null);
        }
    }, true);

    // delete-create-delete in one txn
    doInTxn(new Action() {
        public void run(BlobStoreConnection con) throws Exception {
          Blob b = getBlob(con, id, "");
          deleteBlob(con, b);
          createBlob(con, b, null);
          deleteBlob(con, b);
        }
    }, true);

    // create-delete-create-delete in one txn
    doInTxn(new Action() {
        public void run(BlobStoreConnection con) throws Exception {
          Blob b = getBlob(con, id, null);
          createBlob(con, b, null);
          deleteBlob(con, b);
          createBlob(con, b, null);
          deleteBlob(con, b);
        }
    }, true);

    // create-update-delete-create-update-delete in one txn
    doInTxn(new Action() {
        public void run(BlobStoreConnection con) throws Exception {
          Blob b = getBlob(con, id, null);
          createBlob(con, b, body);
          deleteBlob(con, b);
          createBlob(con, b, body);
          deleteBlob(con, b);
        }
    }, true);

    // create in one, delete in another
    createBlob(id, body, true);
    deleteBlob(id, body, true);

    // create in one, delete + create in another
    createBlob(id, body, true);

    doInTxn(new Action() {
        public void run(BlobStoreConnection con) throws Exception {
          Blob b = getBlob(con, id, body);
          deleteBlob(con, b);
          createBlob(con, b, null);
        }
    }, true);

    // delete + create + update in one
    doInTxn(new Action() {
        public void run(BlobStoreConnection con) throws Exception {
          Blob b = getBlob(con, id, "");
          deleteBlob(con, b);
          createBlob(con, b, body);
        }
    }, true);

    // update + delete + create + update in one
    doInTxn(new Action() {
        public void run(BlobStoreConnection con) throws Exception {
          Blob b = getBlob(con, id, body);
          setBlob(con, b, "foo");
          deleteBlob(con, b);
          createBlob(con, b, body);
        }
    }, true);

    // delete + create + update + delete in one
    doInTxn(new Action() {
        public void run(BlobStoreConnection con) throws Exception {
          Blob b = getBlob(con, id, body);
          deleteBlob(con, b);
          createBlob(con, b, body);
          deleteBlob(con, b);
        }
    }, true);

    // create-move-delete in one txn
    doInTxn(new Action() {
        public void run(BlobStoreConnection con) throws Exception {
          Blob b  = getBlob(con, id, null);
          Blob b2 = getBlob(con, id2, null);

          createBlob(con, b, null);
          moveBlob(con, b, b2, "");
          deleteBlob(con, b2);
        }
    }, true);

    // create in one, move-delete in another txn
    createBlob(id, body, true);

    doInTxn(new Action() {
        public void run(BlobStoreConnection con) throws Exception {
          Blob b  = getBlob(con, id, body);
          Blob b2 = getBlob(con, id2, null);

          moveBlob(con, b, b2, body);
          deleteBlob(con, b2);
        }
    }, true);

    // create-move-delete-create-move in one txn
    doInTxn(new Action() {
        public void run(BlobStoreConnection con) throws Exception {
          Blob b  = getBlob(con, id, null);
          Blob b2 = getBlob(con, id2, null);

          createBlob(con, b, null);
          moveBlob(con, b, b2, "");
          deleteBlob(con, b2);
          createBlob(con, b2, null);
          moveBlob(con, b2, b, "");
          setBlob(con, b, body);
        }
    }, true);

    // move-delete-create-move-again-yada-yada...
    doInTxn(new Action() {
        public void run(BlobStoreConnection con) throws Exception {
          Blob b  = getBlob(con, id, body);
          Blob b2 = getBlob(con, id2, null);

          moveBlob(con, b, b2, body);
          deleteBlob(con, b2);
          createBlob(con, b, null);
          moveBlob(con, b, b2, "");
          moveBlob(con, b2, b, "");
          deleteBlob(con, b);
          createBlob(con, b2, null);
          moveBlob(con, b2, b, "");
          setBlob(con, b, body);
          moveBlob(con, b, b2, body);
          deleteBlob(con, b2);
          createBlob(con, b, body);
        }
    }, true);

    // clean up
    deleteBlob(id, body, true);
    getBlob(id, null, true);

    assertNoBlobs("urn:blobBlobDelete");
  }

  /**
   * Test conflicts between two transactions (creating, updating, or deleting a blob that
   * the other has touched).
   */
  @Test(groups={ "blobs" }, dependsOnGroups={ "init" })
  public void testConflicts() throws Exception {
    final URI id1 = URI.create("urn:blobConflict1");
    final URI id2 = URI.create("urn:blobConflict2");
    final URI id3 = URI.create("urn:blobConflict3");

    final String body1  = "original blob";
    final String body11 = "modified blob";
    final String body2  = "create me";
    final String body3  = "rename me";

    // create blob1
    createBlob(id1, body1, true);

    // create a set of actions
    Action createNoBody = new Action() {
            public void run(BlobStoreConnection con) throws Exception {
              Blob b = getBlob(con, id2, null);
              createBlob(con, b, null);
            }
        };

    Action createWithBody = new Action() {
            public void run(BlobStoreConnection con) throws Exception {
              Blob b = getBlob(con, id2, null);
              createBlob(con, b, body2);
            }
        };

    Action delete1 = new Action() {
            public void run(BlobStoreConnection con) throws Exception {
              Blob b = getBlob(con, id1, body1);
              deleteBlob(con, b);
            }
        };

    Action delete2 = new Action() {
            public void run(BlobStoreConnection con) throws Exception {
              Blob b = getBlob(con, id2, body2);
              deleteBlob(con, b);
            }
        };

    Action modify1 = new Action() {
            public void run(BlobStoreConnection con) throws Exception {
              Blob b = getBlob(con, id1, body1);
              setBlob(con, b, body11);
            }
        };

    Action rename12 = new Action() {
            public void run(BlobStoreConnection con) throws Exception {
              Blob b1 = getBlob(con, id1, body1);
              Blob b2 = getBlob(con, id2, null);
              moveBlob(con, b1, b2, body1);
            }
        };

    // test create-create conflict
    testConflict(createNoBody, createNoBody, id2);
    getBlob(id2, "", true);
    deleteBlob(id2, "", true);

    testConflict(createWithBody, createNoBody, id2);
    getBlob(id2, body2, true);
    deleteBlob(id2, body2, true);

    testConflict(createWithBody, createWithBody, id2);
    getBlob(id2, body2, true);
    deleteBlob(id2, body2, true);

    testConflict(createNoBody, createWithBody, id2);
    getBlob(id2, "", true);
    deleteBlob(id2, "", true);

    // test delete-delete conflict
    testConflict(delete1, delete1, id1);
    getBlob(id1, null, true);
    createBlob(id1, body1, true);

    // test delete-modify conflict
    testConflict(delete1, modify1, id1);
    getBlob(id1, null, true);
    createBlob(id1, body1, true);

    testConflict(modify1, delete1, id1);
    getBlob(id1, body11, true);
    setBlob(id1, body1, true);

    // test modify-modify conflict
    testConflict(modify1, modify1, id1);
    getBlob(id1, body11, true);
    setBlob(id1, body1, true);

    // test rename-rename conflict
    testConflict(rename12, rename12, id1);
    getBlob(id2, body1, true);
    renameBlob(id2, id1, body1, true);

    // test rename-modify conflict
    testConflict(rename12, modify1, id1);
    getBlob(id2, body1, true);
    renameBlob(id2, id1, body1, true);

    testConflict(modify1, rename12, id1);
    getBlob(id1, body11, true);
    setBlob(id1, body1, true);

    // test rename-create conflict
    testConflict(rename12, createNoBody, id2);
    getBlob(id2, body1, true);
    renameBlob(id2, id1, body1, true);

    testConflict(createNoBody, rename12, id2);
    getBlob(id1, body1, true);
    getBlob(id2, "", true);
    deleteBlob(id2, "", true);

    testConflict(createWithBody, rename12, id2);
    getBlob(id1, body1, true);
    getBlob(id2, body2, true);
    deleteBlob(id2, body2, true);

    // test rename-delete conflict
    testConflict(rename12, delete1, id1);
    getBlob(id2, body1, true);
    renameBlob(id2, id1, body1, true);

    testConflict(delete1, rename12, id1);
    getBlob(id1, null, true);
    createBlob(id1, body1, true);

    // clean up

    deleteBlob(id1, body1, true);
    getBlob(id1, null, true);

    assertNoBlobs("urn:blobConflict");
  }

  private void testConflict(final Action first, final Action second, final URI id) throws Exception {
    final boolean[] cv     = new boolean[] { false };
    final boolean[] failed = new boolean[] { false };
    Thread[] threads = new Thread[2];

    threads[0] = doInThread(new ERunnable() {
      public void erun() throws Exception {
        doInTxn(new Action() {
            public void run(BlobStoreConnection con) throws Exception {
              notifyAndWait(cv, true);

              first.run(con);

              notifyAndWait(cv, true);
            }
        }, true);

        TestTransactionalStore.notify(cv, true);
      }
    }, failed);

    threads[1] = doInThread(new ERunnable() {
      public void erun() throws Exception {
        doInTxn(new Action() {
            public void run(BlobStoreConnection con) throws Exception {
              waitFor(cv, true, 0);
              notifyAndWait(cv, false);

              try {
                second.run(con);
                fail("Did not get expected ConcurrentBlobUpdateException");
              } catch (ConcurrentBlobUpdateException cbue) {
                assertEquals(cbue.getBlobId(), id);
              }

              notifyAndWait(cv, false);
            }
        }, false);
      }
    }, failed);

    for (int t = 0; t < threads.length; t++)
      threads[t].join();

    assertFalse(failed[0]);
  }

  /**
   * Test listing blobs.
   */
  @Test(groups={ "blobs" }, dependsOnGroups={ "init" })
  public void testListBlobs() throws Exception {
    URI id1 = URI.create("urn:blobBasicList1");
    URI id2 = URI.create("urn:blobBasicList2");

    listBlobs("urn:blobBasicList", new URI[] { });

    createBlob(id1, "hello", true);
    listBlobs("urn:blobBasicList", new URI[] { id1 });

    createBlob(id2, "bye", true);
    listBlobs("urn:blobBasicList", new URI[] { id1, id2 });

    deleteBlob(id1, "hello", true);
    listBlobs("urn:blobBasicList", new URI[] { id2 });

    deleteBlob(id2, "bye", true);
    listBlobs("urn:blobBasicList", new URI[] { });
  }

  /**
   * Create, get, rename, update, delete in other transactions should not affect current
   * transaction.
   */
  @Test(groups={ "blobs" }, dependsOnGroups={ "init" })
  public void testBasicTransactionIsolation() throws Exception {
    final URI id1 = URI.create("urn:blobBasicTxnIsol1");
    final URI id2 = URI.create("urn:blobBasicTxnIsol2");
    final URI id3 = URI.create("urn:blobBasicTxnIsol3");
    final URI id4 = URI.create("urn:blobBasicTxnIsol4");

    final String body1  = "long lived blob";
    final String body2  = "long lived, modified blob";
    final String body3  = "create me";
    final String body4  = "delete me";
    final String body22 = "body1, v2";
    final String body42 = "delete me, v2";
    final String body43 = "delete me, v3";

    // create blob1
    createBlob(id1, body1, true);

    // first start txn1, then run a bunch of other transactions while txn1 is active
    doInTxn(new Action() {
        public void run(final BlobStoreConnection con) throws Exception {
          // check our snapshot
          getBlob(con, id1, body1);
          getBlob(con, id2, null);
          getBlob(con, id3, null);
          getBlob(con, id4, null);

          // create another blob and modify
          Blob b = getBlob(con, id2, null);
          createBlob(con, b, body1);
          setBlob(con, b, body2);

          // create a new blob and verify we don't see it but others see it
          boolean[] failed = new boolean[] { false };

          doInThread(new ERunnable() {
            public void erun() throws Exception {
              createBlob(id3, body3, true);
            }
          }, failed).join();
          assertFalse(failed[0]);

          doInThread(new ERunnable() {
            public void erun() throws Exception {
              getBlob(id1, body1, true);
              getBlob(id2, null, true);
              getBlob(id3, body3, true);
              getBlob(id4, null, true);
            }
          }, failed).join();
          assertFalse(failed[0]);

          getBlob(con, id1, body1);
          getBlob(con, id2, body2);
          getBlob(con, id3, null);
          getBlob(con, id4, null);

          // delete the new blob
          doInThread(new ERunnable() {
            public void erun() throws Exception {
              deleteBlob(id3, body3, true);
            }
          }, failed).join();
          assertFalse(failed[0]);

          doInThread(new ERunnable() {
            public void erun() throws Exception {
              getBlob(id1, body1, true);
              getBlob(id2, null, true);
              getBlob(id3, null, true);
              getBlob(id4, null, true);
            }
          }, failed).join();
          assertFalse(failed[0]);

          getBlob(con, id1, body1);
          getBlob(con, id2, body2);
          getBlob(con, id3, null);
          getBlob(con, id4, null);

          // delete the first blob
          doInThread(new ERunnable() {
            public void erun() throws Exception {
              deleteBlob(id1, body1, true);
            }
          }, failed).join();
          assertFalse(failed[0]);

          doInThread(new ERunnable() {
            public void erun() throws Exception {
              getBlob(id1, null, true);
              getBlob(id2, null, true);
              getBlob(id3, null, true);
              getBlob(id4, null, true);
            }
          }, failed).join();
          assertFalse(failed[0]);

          getBlob(con, id1, body1);
          getBlob(con, id2, body2);
          getBlob(con, id3, null);
          getBlob(con, id4, null);

          // re-create the first blob, but with different content
          doInThread(new ERunnable() {
            public void erun() throws Exception {
              createBlob(id1, body22, true);
            }
          }, failed).join();
          assertFalse(failed[0]);

          doInThread(new ERunnable() {
            public void erun() throws Exception {
              getBlob(id1, body22, true);
              getBlob(id2, null, true);
              getBlob(id3, null, true);
              getBlob(id4, null, true);
            }
          }, failed).join();
          assertFalse(failed[0]);

          getBlob(con, id1, body1);
          getBlob(con, id2, body2);
          getBlob(con, id3, null);
          getBlob(con, id4, null);

          // step through, making sure we don't see anything from active transactions
          final boolean[] cv = new boolean[1];
          Thread t = doInThread(new ERunnable() {
            public void erun() throws Exception {
              doInTxn(new Action() {
                public void run(BlobStoreConnection c2) throws Exception {
                  Blob b = getBlob(c2, id4, null);
                  createBlob(c2, b, body4);

                  notifyAndWait(cv, true);

                  deleteBlob(c2, b);

                  notifyAndWait(cv, true);

                  createBlob(c2, b, body42);

                  notifyAndWait(cv, true);

                  setBlob(c2, b, body43);

                  notifyAndWait(cv, true);
                }
              }, true);
            }
          }, failed);

          waitFor(cv, true, 0);

          assertFalse(con.getBlob(id4, null).exists());

          notifyAndWait(cv, false);

          assertFalse(con.getBlob(id4, null).exists());

          notifyAndWait(cv, false);

          assertFalse(con.getBlob(id4, null).exists());

          notifyAndWait(cv, false);

          assertFalse(con.getBlob(id4, null).exists());

          TestTransactionalStore.notify(cv, false);
          t.join();
          assertFalse(failed[0]);

          assertFalse(con.getBlob(id4, null).exists());
        }
    }, true);

    deleteBlob(id1, body22, true);
    deleteBlob(id2, body2, true);
    deleteBlob(id3, null, true);
    deleteBlob(id4, body43, true);

    assertNoBlobs("urn:blobBasicTxnIsol");
  }

  /**
   * Test single stepping two transactions.
   */
  @Test(groups={ "blobs" }, dependsOnGroups={ "init" })
  public void testTransactionIsolation2() throws Exception {
    final URI id1 = URI.create("urn:blobTxnIsol2_1");
    final URI id2 = URI.create("urn:blobTxnIsol2_2");
    final URI id3 = URI.create("urn:blobTxnIsol2_3");
    final URI id4 = URI.create("urn:blobTxnIsol2_4");
    final URI id5 = URI.create("urn:blobTxnIsol2_5");
    final URI id6 = URI.create("urn:blobTxnIsol2_6");

    final String body1  = "blob1";
    final String body2  = "blob2";
    final String body3  = "blob3";
    final String body4  = "blob4";

    final boolean[] failed  = new boolean[] { false };
    final boolean[] cv      = new boolean[] { false };
    final Thread[]  threads = new Thread[2];

    // 2 threads, which will single-step, each doing a step and then waiting for the other
    for (int t = 0; t < threads.length; t++) {
      final URI[]    ids     = new URI[]    { t == 0 ? id1 : id3, t == 0 ? id2 : id4 };
      final URI[]    oids    = new URI[]    { t != 0 ? id1 : id3, t != 0 ? id2 : id4 };
      final URI      tid     = t == 0 ? id5 : id6;
      final String[] bodies  = new String[] { t == 0 ? body1 : body3, t == 0 ? body2 : body4 };
      final String[] obodies = new String[] { t != 0 ? body1 : body3, t != 0 ? body2 : body4 };
      final boolean  cvVal   = (t == 0);

      threads[t] = doInThread(new ERunnable() {
        public void erun() throws Exception {
          // create two blobs
          doInTxn(new Action() {
              public void run(final BlobStoreConnection con) throws Exception {
                getBlob(con, id1, false);
                getBlob(con, id2, false);
                getBlob(con, id3, false);
                getBlob(con, id4, false);

                waitFor(cv, cvVal, 0);

                notifyAndWait(cv, !cvVal);

                for (int idx = 0; idx < 2; idx++ ) {
                  Blob b = getBlob(con, ids[idx], null);
                  createBlob(con, b, null);

                  for (int idx2 = 0; idx2 < 2; idx2++ )
                    getBlob(con, oids[idx2], false);

                  notifyAndWait(cv, !cvVal);

                  setBlob(con, b, bodies[idx]);

                  notifyAndWait(cv, !cvVal);
                }
              }
          }, true);

          notifyAndWait(cv, !cvVal);

          // exchange the two blobs
          doInTxn(new Action() {
              public void run(final BlobStoreConnection con) throws Exception {
                getBlob(con, id1, body1);
                getBlob(con, id2, body2);
                getBlob(con, id3, body3);
                getBlob(con, id4, body4);

                notifyAndWait(cv, !cvVal);

                URI[][] seq = new URI[][] {
                  { ids[0], tid },
                  { ids[1], ids[0] },
                  { tid, ids[1] },
                };

                for (int idx = 0; idx < seq.length; idx++ ) {
                  Blob bs = getBlob(con, seq[idx][0], true);
                  Blob bd = getBlob(con, seq[idx][1], false);
                  moveBlob(con, bs, bd, null);

                  notifyAndWait(cv, !cvVal);
                }
              }
          }, true);

          notifyAndWait(cv, !cvVal);

          // delete the two blobs
          doInTxn(new Action() {
              public void run(final BlobStoreConnection con) throws Exception {
                getBlob(con, id1, body2);
                getBlob(con, id2, body1);
                getBlob(con, id3, body4);
                getBlob(con, id4, body3);

                notifyAndWait(cv, !cvVal);

                for (int idx = 0; idx < 2; idx++ ) {
                  Blob b = getBlob(con, ids[idx], bodies[1 - idx]);
                  deleteBlob(con, b);

                  for (int idx2 = 0; idx2 < 2; idx2++ )
                    getBlob(con, oids[idx2], obodies[1 - idx2]);

                  notifyAndWait(cv, !cvVal);
                }
              }
          }, true);

          notifyAndWait(cv, !cvVal);
          TestTransactionalStore.notify(cv, !cvVal);
        }
      }, failed);
    }

    for (int t = 0; t < threads.length; t++)
      threads[t].join();

    assertFalse(failed[0]);

    assertNoBlobs("urn:blobTxnIsol2_");
  }

  /**
   * Stress test the stuff a bit.
   */
  @Test(groups={ "blobs" }, dependsOnGroups={ "init" })
  public void stressTest() throws Exception {
    // get our config
    final int numFillers = Integer.getInteger("akubra.txn.test.numFillers", 0);
    final int numReaders = Integer.getInteger("akubra.txn.test.numReaders", 10);
    final int numWriters = Integer.getInteger("akubra.txn.test.numWriters", 10);
    final int numObjects = Integer.getInteger("akubra.txn.test.numObjects", 10);
    final int numRounds  = Integer.getInteger("akubra.txn.test.numRounds",  10);

    long t0 = System.currentTimeMillis();

    // "fill" the db a bit
    for (int b = 0; b < numFillers / 1000; b++) {
      final int start = b * 1000;

      doInTxn(new Action() {
        public void run(BlobStoreConnection con) throws Exception {
          for (int idx = start; idx < start + 1000; idx++) {
            Blob b = con.getBlob(URI.create("urn:blobStressTestFiller" + idx), null);
            b.create();
            setBody(b, "v" + idx);
          }
        }
      }, true);
    }

    long t1 = System.currentTimeMillis();

    // set up
    Thread[] writers = new Thread[numWriters];
    Thread[] readers = new Thread[numReaders];
    boolean[] failed = new boolean[] { false };

    final boolean[] testDone = new boolean[] { false };
    final int[]     lowIds   = new int[numWriters];
    final int[]     highId   = new int[] { 0 };

    // start/run the writers
    for (int t = 0; t < writers.length; t++) {
      final int tid   = t;
      final int start = t * numRounds * numObjects;

      writers[t] = doInThread(new ERunnable() {
        public void erun() throws Exception {
          for (int r = 0; r < numRounds; r++) {
            final int off = start + r * numObjects;

            doInTxn(new Action() {
              public void run(BlobStoreConnection con) throws Exception {
                for (int o = 0; o < numObjects; o++) {
                  int    idx = off + o;
                  URI    id  = URI.create("urn:blobStressTest" + idx);
                  String val = "v" + idx;

                  Blob b = getBlob(con, id, null);
                  createBlob(con, b, val);
                }
              }
            }, true);

            synchronized (testDone) {
              highId[0] = Math.max(highId[0], off + numObjects);
            }

            doInTxn(new Action() {
              public void run(BlobStoreConnection con) throws Exception {
                for (int o = 0; o < numObjects; o++) {
                  int    idx = off + o;
                  URI    id  = URI.create("urn:blobStressTest" + idx);
                  String val = "v" + idx;

                  Blob b = getBlob(con, id, val);
                  deleteBlob(con, b);
                }
              }
            }, true);

            synchronized (testDone) {
              lowIds[tid] = off + numObjects;
            }
          }
        }
      }, failed);
    }

    // start/run the readers
    for (int t = 0; t < readers.length; t++) {
      readers[t] = doInThread(new ERunnable() {
        public void erun() throws Exception {
          final Random rng   = new Random();
          final int[]  found = new int[] { 0 };

          while (true) {
            final int low, high;
            synchronized (testDone) {
              if (testDone[0])
                break;

              high = highId[0];

              int tmp = Integer.MAX_VALUE;
              for (int id : lowIds)
                tmp = Math.min(tmp, id);
              low = tmp;
            }

            if (low == high) {
              Thread.yield();
              continue;
            }

            doInTxn(new Action() {
              public void run(BlobStoreConnection con) throws Exception {
                for (int o = 0; o < numObjects; o++) {
                  int    idx = rng.nextInt(high - low) + low;
                  URI    id  = URI.create("urn:blobStressTest" + idx);
                  String val = "v" + idx;

                  Blob b = con.getBlob(id, null);
                  if (b.exists()) {
                    assertEquals(getBody(b), val);
                    found[0]++;
                  }
                }
              }
            }, true);
          }

          if (found[0] == 0)
            System.out.println("Warning: this reader found no blobs");
        }
      }, failed);
    }

    // wait for things to end
    for (int t = 0; t < writers.length; t++)
      writers[t].join();

    synchronized (testDone) {
      testDone[0] = true;
    }

    for (int t = 0; t < readers.length; t++)
      readers[t].join();

    long t2 = System.currentTimeMillis();

    // remove the fillers again
    for (int b = 0; b < numFillers / 1000; b++) {
      final int start = b * 1000;

      doInTxn(new Action() {
        public void run(BlobStoreConnection con) throws Exception {
          for (int idx = start; idx < start + 1000; idx++)
            con.getBlob(URI.create("urn:blobStressTestFiller" + idx), null).delete();
        }
      }, true);
    }

    long t3 = System.currentTimeMillis();

    System.out.println("Time to create " + numFillers + " fillers: " + ((t1 - t0) / 1000.) + " s");
    System.out.println("Time to remove " + numFillers + " fillers: " + ((t3 - t2) / 1000.) + " s");
    System.out.println("Time to run test (" + numWriters + "/" + numRounds + "/" + numObjects +
                       "): " + ((t2 - t1) / 1000.) + " s");

    assertFalse(failed[0]);
  }

  /**
   * Test that things get cleaned up. This runs after all other tests that create or otherwise
   * manipulate blobs.
   */
  @Test(groups={ "post" }, dependsOnGroups={ "blobs" })
  public void testCleanup() throws Exception {
    assertNoBlobs(null);

    // verify that the tables are truly empty
    Connection connection = DriverManager.getConnection("jdbc:derby:" + dbDir);
    ResultSet rs =
        connection.createStatement().executeQuery("SELECT * FROM " + TransactionalStore.NAME_TABLE);
    assertFalse(rs.next(), "unexpected entries in name-map table;");

    rs = connection.createStatement().executeQuery("SELECT * FROM " + TransactionalStore.DEL_TABLE);
    assertFalse(rs.next(), "unexpected entries in deleted-list table;");
  }

  /*
   * simple helpers that do an operation and assert things went fine
   */

  private Blob getBlob(BlobStoreConnection con, URI id, boolean exists) throws Exception {
    Blob b = con.getBlob(id, null);
    assertEquals(b.getId(), id);
    assertEquals(b.exists(), exists);
    return b;
  }

  private Blob getBlob(BlobStoreConnection con, URI id, String body) throws Exception {
    Blob b = getBlob(con, id, body != null);

    if (body != null)
      assertEquals(getBody(b), body);

    return b;
  }

  private void createBlob(BlobStoreConnection con, Blob b, String body) throws Exception {
    b.create();
    assertTrue(b.exists());
    assertTrue(con.getBlob(b.getId(), null).exists());

    assertEquals(getBody(b), "");
    assertEquals(getBody(con.getBlob(b.getId(), null)), "");

    if (body != null)
      setBlob(con, b, body);
  }

  private void setBlob(BlobStoreConnection con, Blob b, String body) throws Exception {
    setBody(b, body);
    assertEquals(getBody(b), body);
    assertEquals(getBody(con.getBlob(b.getId(), null)), body);
  }

  private void deleteBlob(BlobStoreConnection con, Blob b) throws Exception {
    b.delete();
    assertFalse(b.exists());
    assertFalse(con.getBlob(b.getId(), null).exists());
  }

  private void moveBlob(BlobStoreConnection con, Blob ob, Blob nb, String body) throws Exception {
    ob.moveTo(nb);
    assertFalse(ob.exists());
    assertFalse(con.getBlob(ob.getId(), null).exists());
    assertTrue(nb.exists());
    assertTrue(con.getBlob(nb.getId(), null).exists());

    if (body != null) {
      assertEquals(getBody(nb), body);
      assertEquals(getBody(con.getBlob(nb.getId(), null)), body);
    }
  }

  private void listBlobs(BlobStoreConnection con, String prefix, URI[] expected) throws Exception {
    Set<URI> exp = new HashSet(Arrays.asList(expected));
    URI id;

    for (Iterator<URI> iter = con.listBlobIds(prefix); iter.hasNext(); )
      assertTrue(exp.remove(id = iter.next()), "unexpected blob '" + id + "' found;");

    assertTrue(exp.isEmpty(), "expected blobs not found for prefix '" + prefix + "': " + exp + ";");
  }

  /*
   * versions of the simple helpers that run in a transaction
   */

  private void createBlob(final URI id, final String val, boolean commit) throws Exception {
    doInTxn(new Action() {
        public void run(BlobStoreConnection con) throws Exception {
          Blob b = getBlob(con, id, null);
          createBlob(con, b, val);
        }
    }, commit);
  }

  private void deleteBlob(final URI id, final String body, final boolean commit) throws Exception {
    doInTxn(new Action() {
        public void run(BlobStoreConnection con) throws Exception {
          Blob b = getBlob(con, id, body);
          deleteBlob(con, b);
        }
    }, commit);
  }

  private void getBlob(final URI id, final String val, boolean commit) throws Exception {
    doInTxn(new Action() {
        public void run(BlobStoreConnection con) throws Exception {
          getBlob(con, id, val);
        }
    }, commit);
  }

  private void setBlob(final URI id, final String val, boolean commit) throws Exception {
    doInTxn(new Action() {
        public void run(BlobStoreConnection con) throws Exception {
          Blob b = getBlob(con, id, true);
          setBlob(con, b, val);
        }
    }, commit);
  }

  private void renameBlob(final URI oldId, final URI newId, final String val, boolean commit)
      throws Exception {
    doInTxn(new Action() {
        public void run(BlobStoreConnection con) throws Exception {
          Blob ob = getBlob(con, oldId, val);
          Blob nb = getBlob(con, newId, null);
          moveBlob(con, ob, nb, val);
        }
    }, commit);
  }

  private void listBlobs(final String prefix, final URI[] expected) throws Exception {
    doInTxn(new Action() {
        public void run(BlobStoreConnection con) throws Exception {
          listBlobs(con, prefix, expected);
        }
    }, true);
  }

  private void assertNoBlobs(final String prefix) throws Exception {
    doInTxn(new Action() {
        public void run(BlobStoreConnection con) throws Exception {
          assertFalse(con.listBlobIds(prefix).hasNext(),
                      "unexpected blobs found for prefix '" + prefix + "';");
        }
    }, true);

    BlobStoreConnection con = blobStore.openConnection(null);
    try {
      assertFalse(con.listBlobIds(prefix).hasNext(),
                  "unexpected blobs found for prefix '" + prefix + "';");
    } finally {
      con.close();
    }
  }

  /*
   * Other helpers
   */

  private static String getBody(Blob b) throws IOException {
    InputStream is = b.openInputStream();
    try {
      return IOUtils.toString(is);
    } finally {
      is.close();
    }
  }

  private static void setBody(Blob b, String data) throws IOException {
    OutputStream os = b.openOutputStream(-1);
    try {
      os.write(data.getBytes());
    } finally {
      os.close();
    }
  }

  private void doInTxn(Action a, boolean commit) throws Exception {
    tm.begin();
    BlobStoreConnection con = store.openConnection(tm.getTransaction());

    try {
      a.run(con);

      if (commit)
        tm.commit();
      else
        tm.rollback();
    } finally {
      if (tm.getTransaction() != null) {
        try {
          tm.rollback();
        } catch (Exception e) {
          e.printStackTrace();
        }
      }

      con.close();
    }
  }

  private static Thread doInThread(Runnable r) throws Exception {
    return doInThread(r, null);
  }

  private static Thread doInThread(Runnable r, final boolean[] failed) throws Exception {
    Thread t = new Thread(r);

    t.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
      public void uncaughtException(Thread t, Throwable e) {
        if (failed != null) {
          synchronized (failed) {
            failed[0] = true;
          }
        }

        e.printStackTrace();
      }
    });

    t.start();
    return t;
  }

  private static void waitFor(boolean[] cv, boolean val, long to) throws InterruptedException {
    long t0 = System.currentTimeMillis();
    synchronized (cv) {
      while (cv[0] != val && (to == 0 || (System.currentTimeMillis() - t0) < to))
        cv.wait(to);
    }
  }

  private static void notify(boolean[] cv, boolean val) {
    synchronized (cv) {
      cv[0] = val;
      cv.notify();
    }
  }

  private static void notifyAndWait(boolean[] cv, boolean val) throws InterruptedException {
    synchronized (cv) {
      notify(cv, val);
      waitFor(cv, !val, 0);
    }
  }

  private static interface Action {
    public void run(BlobStoreConnection con) throws Exception;
  }

  private static abstract class ERunnable implements Runnable {
    public void run() {
      try {
        erun();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    public abstract void erun() throws Exception;
  }
}
