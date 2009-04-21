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

package org.fedoracommons.akubra.tck;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import javax.transaction.Transaction;

import org.testng.annotations.Test;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import org.fedoracommons.akubra.AkubraException;
import org.fedoracommons.akubra.Blob;
import org.fedoracommons.akubra.BlobStore;
import org.fedoracommons.akubra.BlobStoreConnection;
import org.fedoracommons.akubra.DuplicateBlobException;
import org.fedoracommons.akubra.UnsupportedIdException;
import org.fedoracommons.akubra.MissingBlobException;
import org.fedoracommons.akubra.impl.AbstractBlob;

/**
 * A suite of unit tests for Akubra stores.
 *
 * <p>The tests are assigned, and depend on, various groups. All tests here depend on (directly or
 * indirectly) the group "init", and there is one dummy test in that group; all tests that
 * manipulate blobs (create, delete, modify, move) are in the "manipulatesBlobs" group; there is
 * one test in the "post" group, and that test depends on the "manipulatesBlobs" to ensure it is
 * run after all blob manipulations are completed. Additional there are three groups that represent
 * the entity they're testing: "store", "connection", and "blob".
 *
 * <p>Subclasses of this must implement at least {@link #getInvalidId getInvalidId}. Additionally,
 * many stores will probably want to override {@link #createId createId} and {@link #getPrefixFor
 * getPrefixFor} in order to create URI's which are understood by the store.
 *
 * <p>For stores that don't support all modify operations, various flags can be passed to the
 * constructor indicating what is supported. This works fine for the transactional, id-gen, and
 * move-to flags; however, create, delete, and list-ids are needed throughout the tests. For
 * stores that don't support one or more of these latter flags you must also override one or more
 * of {@link #createBlob(BlobStoreConnection, Blob, String) createBlob} and {@link
 * #deleteBlob(BlobStoreConnection, Blob) deleteBlob} to create and delete the underlying
 * resources, and {@link #assertNoBlobs(String) assertNoBlobs} and {@link
 * #listBlobs(BlobStoreConnection, String, URI[]) listBlobs} for verifying things got cleaned up
 * properly.
 *
 * @author Ronald Tschalär
 */
@SuppressWarnings("PMD.TooManyStaticImports")
public abstract class TCKTestSuite extends AbstractTests {
  protected final URI     storeId;
  protected final boolean isIdGenSupp;
  protected final boolean isListIdsSupp;
  protected final boolean isCreateSupp;
  protected final boolean isDeleteSupp;
  protected final boolean isMoveToSupp;

  /**
   * Create a new test suite instance. This assumes a fully functional store, i.e. one that
   * supports creation, deletion, moving, and listing of blobs.
   *
   * @param store           the store to test
   * @param storeId         the store's expected id
   * @param isTransactional if the store is transactional
   * @param isIdGenSupp     true if id generation (<code>getBlob(null, ...)</code>) is supported
   */
  protected TCKTestSuite(BlobStore store, URI storeId, boolean isTransactional,
                         boolean isIdGenSupp) {
    this(store, storeId, isTransactional, isIdGenSupp, true, true, true, true);
  }

  /**
   * Create a new test suite instance.
   *
   * @param store           the store to test
   * @param storeId         the store's expected id
   * @param isTransactional if the store is transactional
   * @param isIdGenSupp     true if id generation (<code>getBlob(null, ...)</code>) is supported
   * @param isListIdsSupp   true if <code>con.listBlobIds()</code> is supported
   * @param isCreateSupp    true if <var>Blob.create()</var> is supported
   * @param isDeleteSupp    true if <var>Blob.delete()</var> is supported
   * @param isMoveToSupp    true if <var>Blob.moveTo()</var> is supported
   */
  protected TCKTestSuite(BlobStore store, URI storeId, boolean isTransactional, boolean isIdGenSupp,
                         boolean isListIdsSupp, boolean isCreateSupp, boolean isDeleteSupp,
                         boolean isMoveToSupp) {
    super(store, isTransactional);
    this.storeId       = storeId;
    this.isIdGenSupp   = isIdGenSupp;
    this.isListIdsSupp = isListIdsSupp;
    this.isCreateSupp  = isCreateSupp;
    this.isDeleteSupp  = isDeleteSupp;
    this.isMoveToSupp  = isMoveToSupp;
  }

  /**
   * Create an id for the given name. Usually this will incorporate the given name as part of the
   * URI itself (e.g. by appending it to a fixed prefix), but for stores that can't do this (e.g.
   * a read-only store) they can return an arbitrary URI. This must return different URI's for
   * different names and the same URI for the same name, at least for the duration of each test.
   *
   * <p>The default implementation returns <code>URI.create("urn:akubra-tck:" + name)</code>.
   *
   * @param name  the URI's "name"
   * @return the URI
   * @see #getPrefixFor
   */
  protected URI createId(String name) {
    return URI.create("urn:akubra-tck:" + name);
  }

  /**
   * Get the URI prefix for the given name. This is used by the tests to list all blobs created by
   * the test in order to verify things got cleaned up. The <var>name</var> passed in here will be
   * a prefix of the names passed to previous {@link #createId} calls. The expectation is that if
   * <code>createId</code> is overriden then so must this method. In any case the implementation of
   * this method must figure out an appropriate prefix to return that will include all created id's
   * (in the current test).
   *
   * <p>The default implementation returns <code>"urn:akubra-tck:" + name</code>.
   *
   * @param name  the URI's "name"
   * @return the prefix
   */
  protected String getPrefixFor(String name) {
    return "urn:akubra-tck:" + name;
  }

  /**
   * @return an invalid id (for id validation test), or null to skip the tests (e.g. if all id's
   *         are valid)
   */
  protected abstract URI getInvalidId();

  // FIXME: respect isCreateSupp/isDeleteSupp

  @Test(groups={ "init" })
  public void testInit() {
  }

  /*
   * BlobStore Tests
   */

  /**
   * Store id should be what it was initialized with.
   */
  @Test(groups={ "store" })
  public void testGetId() {
    assertEquals(store.getId(), storeId);
  }

  /**
   * Request to open a connection without a transaction.
   */
  @Test(groups={ "store" }, dependsOnGroups={ "init" })
  public void testOpenConnectionNoTransaction() {
    try {
      BlobStoreConnection con = store.openConnection(null);
      if (isTransactional)
        fail("Did not get expected IOException initializing store without a transaction");

      assertNotNull(con, "Null connection returned from openConnection(null)");
      assertSame(con.getBlobStore(), store);
    } catch (IOException ioe) {
      if (!isTransactional)
        fail("Got unexpected IOException initializing store without a transaction", ioe);
    }
  }

  /**
   * Request to open a connection with a transaction.
   */
  @Test(groups={ "store" }, dependsOnGroups={ "init" })
  public void testOpenConnectionWithTransaction() throws Exception {
    tm.begin();
    BlobStoreConnection con = null;

    try {
      con = store.openConnection(tm.getTransaction());
      if (!isTransactional)
        fail("Did not get expected UnsupportedOperationException while initializing store with " +
             "a transaction");

      assertNotNull(con, "Null connection returned from openConnection(txn)");
      assertSame(con.getBlobStore(), store);
    } catch (UnsupportedOperationException uoe) {
      if (isTransactional)
        fail("Got unexpected UnsupportedOperationException initializing store with a transaction",
             uoe);
    } finally {
      tm.rollback();
      if (con != null)
        con.close();
    }
  }

  /**
   * Request to go quiescent and non-quiescent (even when already in those
   * states) should be supported.
   */
  @Test(groups={ "store" }, dependsOnGroups={ "init" })
  public void testSetQuiescent() throws Exception {
    // basic set (non-)quiescent
    assertTrue(store.setQuiescent(true));
    assertTrue(store.setQuiescent(true));
    assertTrue(store.setQuiescent(false));
    assertTrue(store.setQuiescent(false));

    // in quiescent state: read-only should proceed, write should block
    final URI id1 = createId("blobSetQuiescent1");
    final URI id2 = createId("blobSetQuiescent2");

    createBlob(id1, "bar", true);

    runTests(new ConAction() {
        public void run(BlobStoreConnection con) throws Exception {
          final Blob b1 = getBlob(con, id1, "bar");
          final Blob b2 = getBlob(con, id2, false);

          assertTrue(store.setQuiescent(true));

          doWithTimeout(new ERunnable() { public void erun() throws Exception {
              assertTrue(b1.exists());
            }
          }, 100, true);

          doWithTimeout(new ERunnable() {
            public void erun() throws Exception {
              assertFalse(b2.exists());
            }
          }, 100, true);

          doWithTimeout(new ERunnable() {
            public void erun() throws Exception {
              assertEquals(getBody(b1), "bar");
            }
          }, 100, true);

          if (isCreateSupp) {
            doWithTimeout(new ERunnable() {
              public void erun() throws Exception {
                b1.openOutputStream(-1);
              }
            }, 10, false);
          }

          if (isDeleteSupp) {
            doWithTimeout(new ERunnable() {
              public void erun() throws Exception {
                b1.delete();
              }
            }, 10, false);
          }

          if (isCreateSupp) {
            doWithTimeout(new ERunnable() {
              public void erun() throws Exception {
                b2.create();
              }
            }, 10, false);
          }

          if (isMoveToSupp) {
            doWithTimeout(new ERunnable() {
              public void erun() throws Exception {
                b1.moveTo(b2);
              }
            }, 10, false);
          }

          assertTrue(store.setQuiescent(false));
        }
    }, true);

    // going quiescent should wait for in-progress write operations to complete
    if (isCreateSupp) {
      runTests(new ConAction() {
        public void run(BlobStoreConnection con) throws Exception {
          final Blob b1 = getBlob(con, id1, "bar");

          OutputStream os = b1.openOutputStream(-1);

          final boolean[] failed = new boolean[] { false };

          Thread t = doInThread(new ERunnable() {
            public void erun() throws Exception {
              assertTrue(store.setQuiescent(true));
            }
          }, failed);
          t.join(10);
          assertTrue(t.isAlive());

          os.close();

          t.join(10);
          assertFalse(t.isAlive());
          assertFalse(failed[0]);

          assertTrue(store.setQuiescent(false));
        }
      }, true);
    }

    // clean up
    assertTrue(store.setQuiescent(false));
    deleteBlob(id1, "", true);
    assertNoBlobs(getPrefixFor("blobSetQuiescent"));
  }

  protected void doWithTimeout(final ERunnable test, final long timeout, final boolean expSucc)
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

  /*
   * BlobStoreConnection Tests
   */

  /**
   * Test closing a connection.
   */
  @Test(groups={ "connection", "manipulatesBlobs" }, dependsOnGroups={ "init" })
  public void testCloseConnection() throws Exception {
    final URI id = createId("blobCloseConn1");

    // no operations, close then commit
    runTests(new ConAction() {
        public void run(BlobStoreConnection con) throws Exception {
        }
    }, true);

    // no operations, close then roll back
    runTests(new ConAction() {
        public void run(BlobStoreConnection con) throws Exception {
        }
    }, false);

    // one operation, close then commit
    runTests(new ConAction() {
        public void run(BlobStoreConnection con) throws Exception {
          Blob b = getBlob(con, id, null);
          createBlob(con, b, null);
        }
    }, true);

    // one operation, close then roll back
    runTests(new ConAction() {
        public void run(BlobStoreConnection con) throws Exception {
          Blob b = getBlob(con, id, "");
          deleteBlob(con, b);
        }
    }, false);

    // clean up
    if (isTransactional)
      deleteBlob(id, "", true);
    else
      getBlob(id, null, true);

    // special transactional tests
    if (!isTransactional)
      return;

    // no operations, commit then close
    tm.begin();
    BlobStoreConnection con = store.openConnection(tm.getTransaction());
    tm.commit();
    con.close();
    assertTrue(con.isClosed());

    // no operations, roll back then close
    tm.begin();
    con = store.openConnection(tm.getTransaction());
    tm.rollback();
    con.close();
    assertTrue(con.isClosed());

    // one operation, commit then close
    tm.begin();
    con = store.openConnection(tm.getTransaction());
    Blob b = getBlob(con, id, null);
    createBlob(con, b, null);
    tm.commit();
    con.close();
    assertTrue(con.isClosed());

    // one operation, roll back then close
    tm.begin();
    con = store.openConnection(tm.getTransaction());
    b = getBlob(con, id, "");
    deleteBlob(con, b);
    tm.rollback();
    con.close();
    assertTrue(con.isClosed());

    // clean up
    deleteBlob(id, "", true);
    assertNoBlobs(getPrefixFor("blobCloseConn"));
  }

  /**
   * Test a closed connection.
   */
  @Test(groups={ "connection" }, dependsOnGroups={ "init" })
  public void testClosedConnection() throws Exception {
    final URI id1 = createId("blobClosedConn1");
    final URI id2 = createId("blobClosedConn2");

    // test con.isClosed()
    runTests(new Action() {
      public void run(Transaction txn) throws Exception {
        BlobStoreConnection con = store.openConnection(txn);
        assertFalse(con.isClosed());

        con.close();
        assertTrue(con.isClosed());
      }
    });

    // test connection operations on a closed connection
    runTests(new Action() {
      public void run(Transaction txn) throws Exception {
        final BlobStoreConnection con = store.openConnection(txn);
        con.close();
        assertTrue(con.isClosed());

        assertEquals(con.getBlobStore(), store);

        shouldFail(new ERunnable() {
          public void erun() throws Exception {
            con.getBlob(id1, null);
          }
        }, IllegalStateException.class, null);

        shouldFail(new ERunnable() {
          public void erun() throws Exception {
            con.getBlob(null, null);
          }
        }, IllegalStateException.class, null);

        shouldFail(new ERunnable() {
          public void erun() throws Exception {
            con.getBlob(new ByteArrayInputStream(new byte[0]), -1, null);
          }
        }, IllegalStateException.class, null);

        if (isListIdsSupp) {
          shouldFail(new ERunnable() {
            public void erun() throws Exception {
              con.listBlobIds("foo");
            }
          }, IllegalStateException.class, null);

          shouldFail(new ERunnable() {
            public void erun() throws Exception {
              con.listBlobIds(null);
            }
          }, IllegalStateException.class, null);
        }
      }
    });

    // test non-existent blob operations on a closed connection
    runTests(new Action() {
      public void run(Transaction txn) throws Exception {
        final BlobStoreConnection con = store.openConnection(txn);
        final Blob b = getBlob(con, id1, false);
        con.close();
        assertTrue(con.isClosed());

        assertEquals(b.getConnection(), con);
        assertEquals(b.getId(), id1);

        shouldFail(new ERunnable() {
          public void erun() throws Exception {
            b.openInputStream();
          }
        }, IllegalStateException.class, null);

        if (isCreateSupp) {
          shouldFail(new ERunnable() {
            public void erun() throws Exception {
              b.openOutputStream(-1);
            }
          }, IllegalStateException.class, null);
        }

        shouldFail(new ERunnable() {
          public void erun() throws Exception {
            b.exists();
          }
        }, IllegalStateException.class, null);

        if (isCreateSupp) {
          shouldFail(new ERunnable() {
            public void erun() throws Exception {
              b.create();
            }
          }, IllegalStateException.class, null);
        }
      }
    });

    // test existing blob operations on a closed connection
    runTests(new Action() {
      public void run(Transaction txn) throws Exception {
        final BlobStoreConnection con = store.openConnection(txn);
        final Blob b  = getBlob(con, id1, false);
        final Blob b2 = getBlob(con, id2, false);
        createBlob(con, b, "foo");
        con.close();
        assertTrue(con.isClosed());

        assertEquals(b.getConnection(), con);
        assertEquals(b.getId(), id1);

        shouldFail(new ERunnable() {
          public void erun() throws Exception {
            b.openInputStream();
          }
        }, IllegalStateException.class, null);

        if (isCreateSupp) {
          shouldFail(new ERunnable() {
            public void erun() throws Exception {
              b.openOutputStream(-1);
            }
          }, IllegalStateException.class, null);
        }

        shouldFail(new ERunnable() {
          public void erun() throws Exception {
            b.getSize();
          }
        }, IllegalStateException.class, null);

        shouldFail(new ERunnable() {
          public void erun() throws Exception {
            b.exists();
          }
        }, IllegalStateException.class, null);

        if (isDeleteSupp) {
          shouldFail(new ERunnable() {
            public void erun() throws Exception {
              b.delete();
            }
          }, IllegalStateException.class, null);
        }

        if (isMoveToSupp) {
          shouldFail(new ERunnable() {
            public void erun() throws Exception {
              b.moveTo(b2);
            }
          }, IllegalStateException.class, null);
        }
      }
    });

    // clean up
    deleteBlob(id1, "foo", true);
    assertNoBlobs(getPrefixFor("blobClosedConn"));
  }

  /**
   * Test listing blobs.
   */
  @Test(groups={ "connection", "manipulatesBlobs" }, dependsOnGroups={ "init" })
  public void testListBlobs() throws Exception {
    URI id1 = createId("blobBasicList1");
    URI id2 = createId("blobBasicList2");

    listBlobs(getPrefixFor("blobBasicList"), new URI[] { });
    listBlobs(getPrefixFor("blobBasicLisT"), new URI[] { });
    listBlobs(getPrefixFor("blobBasicList2"), new URI[] { });

    createBlob(id1, "hello", true);
    listBlobs(getPrefixFor("blobBasicList"), new URI[] { id1 });
    listBlobs(getPrefixFor("blobBasicLisT"), new URI[] { });
    listBlobs(getPrefixFor("blobBasicList2"), new URI[] { });

    createBlob(id2, "bye", true);
    listBlobs(getPrefixFor("blobBasicList"), new URI[] { id1, id2 });
    listBlobs(getPrefixFor("blobBasicLisT"), new URI[] { });
    listBlobs(getPrefixFor("blobBasicList2"), new URI[] { id2 });

    deleteBlob(id1, "hello", true);
    listBlobs(getPrefixFor("blobBasicList"), new URI[] { id2 });
    listBlobs(getPrefixFor("blobBasicLisT"), new URI[] { });
    listBlobs(getPrefixFor("blobBasicList2"), new URI[] { id2 });

    deleteBlob(id2, "bye", true);
    listBlobs(getPrefixFor("blobBasicList"), new URI[] { });
    listBlobs(getPrefixFor("blobBasicLisT"), new URI[] { });
    listBlobs(getPrefixFor("blobBasicList2"), new URI[] { });
  }


  /*
   * Blob tests.
   */

  /**
   * Test id validation.
   */
  @Test(groups={ "blob", "manipulatesBlobs" }, dependsOnGroups={ "init" })
  public void testIdValidation() throws Exception {
    // valid should work
    URI valId = createId("blobValidId");

    createBlob(valId, null, true);
    deleteBlob(valId, "", true);

    // invalid should not
    final URI invId = getInvalidId();
    if (invId != null) {
      shouldFail(new ERunnable() {
        public void erun() throws Exception {
          createBlob(invId, null, true);
        }
      }, UnsupportedIdException.class, invId);
    }
  }

  /**
   * Test id generation.
   */
  @Test(groups={ "blob", "manipulatesBlobs" }, dependsOnGroups={ "init" })
  public void testIdGeneration() throws Exception {
    // check if id-gen is supported
    if (!isIdGenSupp) {
      shouldFail(new ConAction() {
        public void run(BlobStoreConnection con) throws Exception {
          getBlob(con, null, false);
        }
      }, UnsupportedOperationException.class, null);

      return;
    }

    // null id should work
    runTests(new ConAction() {
        public void run(BlobStoreConnection con) throws Exception {
          Blob b = getBlob(con, null, false);
          if (b.exists())
            deleteBlob(con, b);
        }
    }, false);
  }

  /**
   * Test non-existent blob.
   */
  @Test(groups={ "blob", "manipulatesBlobs" }, dependsOnGroups={ "init" })
  public void testNonExistentBlob() throws Exception {
    final URI id = createId("blobNonExistent1");

    runTests(new ConAction() {
      public void run(BlobStoreConnection con) throws Exception {
        Blob b = getBlob(con, id, false);
        assertEquals(b.getConnection(), con);
        assertEquals(b.getId(), id);
      }
    });

    shouldFail(new ConAction() {
      public void run(BlobStoreConnection con) throws Exception {
        getBlob(con, id, false).getSize();
      }
    }, MissingBlobException.class, id);

    shouldFail(new ConAction() {
      public void run(BlobStoreConnection con) throws Exception {
        getBlob(con, id, false).openInputStream().close();
      }
    }, MissingBlobException.class, id);
  }

  /**
   * Test con.getBlob(InputStream, ...).
   */
  @Test(groups={ "blob", "manipulatesBlobs" }, dependsOnGroups={ "init" })
  public void testBlobFromStream() throws Exception {
    // check if id-gen is supported
    if (!isIdGenSupp) {
      shouldFail(new ConAction() {
        public void run(BlobStoreConnection con) throws Exception {
          con.getBlob(new ByteArrayInputStream(new byte[0]), -1, null);
        }
      }, UnsupportedOperationException.class, null);

      return;
    }

    // null stream should fail
    shouldFail(new ConAction() {
      public void run(BlobStoreConnection con) throws Exception {
        con.getBlob(null, -1, null);
      }
    }, NullPointerException.class, null);

    // basic input-stream should work
    testBlobFromStream("", -1);
    testBlobFromStream("", 0);
    testBlobFromStream("", 10);

    testBlobFromStream("Tyrant, remember?", -1);
    testBlobFromStream("Tyrant, remember?", 0);
    testBlobFromStream("Tyrant, remember?", 10);
    testBlobFromStream("Tyrant, remember?", 17);
    testBlobFromStream("Tyrant, remember?", 27);
  }

  protected void testBlobFromStream(final String body, final long estSize) throws Exception {
    runTests(new ConAction() {
        public void run(BlobStoreConnection con) throws Exception {
          Blob b = con.getBlob(new ByteArrayInputStream(body.getBytes("UTF-8")), estSize, null);
          assertTrue(b.exists());
          assertTrue(con.getBlob(b.getId(), null).exists());

          assertEquals(getBody(b), body);
          assertEquals(getBody(con.getBlob(b.getId(), null)), body);

          deleteBlob(con, b);
        }
    });
  }


  /**
   * Test id validation.
   */
  @Test(groups={ "blob", "manipulatesBlobs" }, dependsOnGroups={ "init" })
  public void testCreate() throws Exception {
    // set up
    final URI id1 = createId("blobCreate1");

    // check if create is supported
    if (!isCreateSupp) {
      shouldFail(new ConAction() {
        public void run(BlobStoreConnection con) throws Exception {
          Blob b = getBlob(con, id1, false);
          createBlob(con, b, "foo");
        }
      }, UnsupportedOperationException.class, null);

      return;
    }

    // create of a non-existent blob
    createBlob(id1, null, true);

    // create of an existing blob should fail
    shouldFail(new ConAction() {
      public void run(BlobStoreConnection con) throws Exception {
        Blob b = getBlob(con, id1, "");
        createBlob(con, b, "foo");
      }
    }, DuplicateBlobException.class, id1);

    // clean up
    deleteBlob(id1, "", true);
    assertNoBlobs(getPrefixFor("blobCreate"));
  }

  /**
   * Test id validation.
   */
  @Test(groups={ "blob", "manipulatesBlobs" }, dependsOnGroups={ "init" })
  public void testDelete() throws Exception {
    // set up
    final URI id1 = createId("blobDelete1");
    createBlob(id1, null, true);

    // check if delete is supported
    if (!isDeleteSupp) {
      shouldFail(new ConAction() {
        public void run(BlobStoreConnection con) throws Exception {
          Blob b = getBlob(con, id1, "");
          b.delete();
        }
      }, UnsupportedOperationException.class, null);

      deleteBlob(id1, "", true);
      return;
    }

    // delete of an existing blob
    deleteBlob(id1, "", true);

    // delete of a non-existent blob should succeed
    deleteBlob(id1, null, true);

    // clean up
    assertNoBlobs(getPrefixFor("blobDelete"));
  }

  /**
   * Test openInputStream.
   */
  @Test(groups={ "blob", "manipulatesBlobs" }, dependsOnGroups={ "init" })
  public void testInputStream() throws Exception {
    // set up
    final URI id1 = createId("blobInputStream1");

    // openInputStream on non-existent blob
    shouldFail(new ConAction() {
      public void run(BlobStoreConnection con) throws Exception {
        Blob b = getBlob(con, id1, false);
        b.openInputStream().close();
      }
    }, MissingBlobException.class, id1);

    // openInputStream on existing blob
    runTests(new ConAction() {
        public void run(BlobStoreConnection con) throws Exception {
          String body = "For the love of God, Montresor";
          Blob b = getBlob(con, id1, null);
          createBlob(con, b, body);

          // read partial body
          byte[] buf = new byte[100];

          InputStream is = b.openInputStream();
          int got = is.read(buf, 0, 10);
          assertTrue(got > 0 && got <= 10, "Invalid number of bytes read: " + got);
          int len = got;
          is.close();
          assertEquals(new String(buf, 0, len, "UTF-8"), body.substring(0, len));

          is = b.openInputStream();
          got = 0;
          while (got < len) {
            int skipped = (int) is.skip(len - got);
            assertTrue(skipped > 0 && skipped <= (len - got),
                       "Invalid number of bytes skipped: " + skipped);
            got += skipped;
          }

          got = is.read(buf, len, 2);
          assertTrue(got > 0 && got <= 2, "Invalid number of bytes read: " + got);
          len += got;
          got = is.read(buf, len, 5);
          assertTrue(got > 0 && got <= 5, "Invalid number of bytes read: " + got);
          len += got;
          is.close();
          assertEquals(new String(buf, 0, len, "UTF-8"), body.substring(0, len));

          // read whole body
          assertEquals(getBody(b), body);

          deleteBlob(con, b);
        }
    });

    // clean up
    assertNoBlobs(getPrefixFor("blobInputStream"));
  }

  /**
   * Test openOutputStream.
   */
  @Test(groups={ "blob", "manipulatesBlobs" }, dependsOnGroups={ "init" })
  public void testOutputStream() throws Exception {
    // check if create is supported
    if (!isCreateSupp) {
      shouldFail(new ConAction() {
        public void run(BlobStoreConnection con) throws Exception {
          Blob b = getBlob(con, createId("blobOutputStream1"), false);
          b.openOutputStream(-1).close();
        }
      }, UnsupportedOperationException.class, null);

      return;
    }

    // set up
    final URI id1 = createId("blobOutputStream1");

    // 0 length blob, no write
    testOutputStream(id1, null, -1);
    testOutputStream(id1, null, 0);
    testOutputStream(id1, null, 20);

    // 0 length blob, empty write
    testOutputStream(id1, "", -1);
    testOutputStream(id1, "", 0);
    testOutputStream(id1, "", 20);

    // >0 length blob
    testOutputStream(id1, "foo bar", -1);
    testOutputStream(id1, "foo bar", 0);
    testOutputStream(id1, "foo bar", 5);
    testOutputStream(id1, "foo bar", 7);
    testOutputStream(id1, "foo bar", 17);

    // clean up
    assertNoBlobs(getPrefixFor("blobOutputStream"));
  }

  protected void testOutputStream(final URI id, final String body, final long estSize)
      throws Exception {
    runTests(new ConAction() {
        public void run(BlobStoreConnection con) throws Exception {
          Blob b = getBlob(con, id, null);
          createBlob(con, b, null);
          if (body != null) {
            setBody(b, body, estSize);
            assertEquals(getBody(b), body);
          } else {
            b.openOutputStream(estSize).close();
            assertEquals(getBody(b), "");
          }
          deleteBlob(con, b);
        }
    });
  }

  /**
   * Test changing a blob's value.
   */
  @Test(groups={ "blob", "manipulatesBlobs" }, dependsOnGroups={ "init" })
  public void testBlobUpdate() throws Exception {
    final URI id = createId("blobBlobUpdate1");

    runTests(new ConAction() {
        public void run(BlobStoreConnection con) throws Exception {
          Blob b = getBlob(con, id, null);
          createBlob(con, b, null);
          setBlob(con, b, "value1");
          setBlob(con, b, "value2");
          setBlob(con, b, "value3");
        }
    }, true);

    runTests(new ConAction() {
        public void run(BlobStoreConnection con) throws Exception {
          Blob b = getBlob(con, id, "value3");
          setBlob(con, b, "value4");
          setBlob(con, b, "value5");
        }
    }, true);

    deleteBlob(id, "value5", true);
    getBlob(id, null, true);

    assertNoBlobs(getPrefixFor("blobBlobUpdate"));
  }

  /**
   * Test move.
   */
  @Test(groups={ "blob", "manipulatesBlobs" }, dependsOnGroups={ "init" })
  public void testMoveTo() throws Exception {
    // check if move is supported
    if (!isMoveToSupp) {
      shouldFail(new ConAction() {
        public void run(BlobStoreConnection con) throws Exception {
          Blob b = getBlob(con, createId("blobMoveTo1"), false);
          b.moveTo(b);
        }
      }, UnsupportedOperationException.class, null);

      return;
    }

    // set up
    final URI id1 = createId("blobMoveTo1");
    final URI id2 = createId("blobMoveTo2");
    final URI id3 = createId("blobMoveTo3");
    final URI id4 = createId("blobMoveTo4");

    createBlob(id1, "foo", true);
    createBlob(id4, "bar", true);

    // move blob from id1 to id2
    renameBlob(id1, id2, "foo", true);

    // move from non-existent blob should fail
    shouldFail(new ConAction() {
      public void run(BlobStoreConnection con) throws Exception {
        Blob ob = getBlob(con, id1, false);
        Blob nb = getBlob(con, id3, false);
        ob.moveTo(nb);
      }
    }, MissingBlobException.class, id1);

    // move to existing blob should fail
    shouldFail(new ConAction() {
      public void run(BlobStoreConnection con) throws Exception {
        Blob ob = getBlob(con, id2, "foo");
        Blob nb = getBlob(con, id4, "bar");
        ob.moveTo(nb);
      }
    }, DuplicateBlobException.class, id4);

    // move a non-existent blob onto itself should fail
    shouldFail(new ConAction() {
      public void run(BlobStoreConnection con) throws Exception {
        Blob b = getBlob(con, id1, false);
        b.moveTo(b);
      }
    }, MissingBlobException.class, id1);

    // move an existing blob onto itself should fail
    shouldFail(new ConAction() {
      public void run(BlobStoreConnection con) throws Exception {
        Blob b = getBlob(con, id2, "foo");
        b.moveTo(b);
      }
    }, DuplicateBlobException.class, id2);

    // move to null should fail
    shouldFail(new ConAction() {
      public void run(BlobStoreConnection con) throws Exception {
        Blob b = getBlob(con, id2, "foo");
        b.moveTo(null);
      }
    }, NullPointerException.class, null);

    // move to incompatible blob should fail
    shouldFail(new ConAction() {
      public void run(BlobStoreConnection con) throws Exception {
        Blob ob = getBlob(con, id2, "foo");
        Blob nb = getIncompatibleBlob(con);
        ob.moveTo(nb);
      }
    }, IllegalArgumentException.class, null);

    // clean up
    deleteBlob(id2, "foo", true);
    deleteBlob(id4, "bar", true);

    assertNoBlobs(getPrefixFor("blobMoveTo"));
  }

  protected Blob getIncompatibleBlob(BlobStoreConnection con) throws Exception {
    return new AbstractBlob(con, createId("foobar")) {
      public InputStream openInputStream() {
        return null;
      }

      public OutputStream openOutputStream(long estimatedSize) {
        return null;
      }

      public long getSize() {
        return -1;
      }

      public boolean exists() {
        return false;
      }

      public void create() {
      }

      public void delete() {
      }

      public void moveTo(Blob blob) {
      }
    };
  }

  /*
   * Transaction tests.
   */

  /**
   * Basic create, get, rename, delete.
   */
  @Test(groups={ "transaction", "manipulatesBlobs" }, dependsOnGroups={ "init" })
  public void testBasicCommit() throws Exception {
    if (!isTransactional)
      return;

    URI id = createId("blobBasicCommit1");

    createBlob(id, "hello", true);
    getBlob(id, "hello", true);

    if (isMoveToSupp) {
      URI id2 = createId("blobBasicCommit2");
      renameBlob(id, id2, "hello", true);
      getBlob(id, null, true);
      getBlob(id2, "hello", true);

      id = id2;
    }

    setBlob(id, "bye bye", true);
    getBlob(id, "bye bye", true);

    deleteBlob(id, "bye bye", true);
    getBlob(id, null, true);

    assertNoBlobs(getPrefixFor("blobBasicCommit"));
  }

  /**
   * Basic create, get, rename, delete with rollbacks.
   */
  @Test(groups={ "transaction", "manipulatesBlobs" }, dependsOnGroups={ "init" })
  public void testBasicRollback() throws Exception {
    if (!isTransactional)
      return;

    URI id = createId("blobBasicRollback1");

    // roll back a create
    createBlob(id, "hello", false);
    getBlob(id, null, false);

    // create, roll back a rename
    createBlob(id, "hello", true);
    getBlob(id, "hello", true);

    if (isMoveToSupp) {
      URI id2 = createId("blobBasicRollback2");
      renameBlob(id, id2, "hello", false);

      getBlob(id2, null, true);
    }

    getBlob(id, "hello", true);

    // update and roll back
    setBlob(id, "bye bye", false);
    getBlob(id, "hello", true);

    // roll back a delete
    deleteBlob(id, "hello", false);
    getBlob(id, "hello", true);

    // delete
    deleteBlob(id, "hello", true);
    getBlob(id, null, true);

    assertNoBlobs(getPrefixFor("blobBasicRollback"));
  }


  /**
   * Test that things get cleaned up. This runs after all other tests that create or otherwise
   * manipulate blobs.
   */
  @Test(groups={ "post" }, dependsOnGroups={ "manipulatesBlobs" })
  public void testCleanup() throws Exception {
    assertNoBlobs(null);
    assertNoBlobs("");
  }
}
