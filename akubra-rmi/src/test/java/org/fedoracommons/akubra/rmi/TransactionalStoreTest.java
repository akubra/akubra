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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;

import javax.transaction.TransactionManager;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.fedoracommons.akubra.Blob;
import org.fedoracommons.akubra.BlobStore;
import org.fedoracommons.akubra.BlobStoreConnection;
import org.fedoracommons.akubra.mem.MemBlobStore;
import org.fedoracommons.akubra.txn.derby.TransactionalStore;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

/**
 * Integration test for a transactional store on the RMI server side.
 *
 * @author Ronald Tschalär
 * @author Pradeep Krishnan
 */
public class TransactionalStoreTest {
  private URI id;
  private TransactionalStore serverStore;
  private BlobStore          store;
  private TransactionManager tm;
  private AkubraRMIServer    server;

  @BeforeSuite
  public void init() throws Exception {
    /*
    java.util.logging.LogManager.getLogManager().readConfiguration(
        new java.io.FileInputStream("/tmp/jdklog.properties"));
    */

    // set up store
    id = new URI("urn:example:txnstore");

    File base = new File(System.getProperty("basedir"), "target");

    File dbDir = new File(base, "txn-db");
    FileUtils.deleteDirectory(dbDir);
    dbDir.getParentFile().mkdirs();

    System.setProperty("derby.stream.error.file", new File(base, "derby.log").toString());
    serverStore = new TransactionalStore(id, dbDir.getAbsolutePath());
    serverStore.setBackingStores(Arrays.<BlobStore>asList(new MemBlobStore()));

    // set up transaction manager
    tm = BtmUtils.getTM();

    int reg = ServiceTest.freePort();
    server = new AkubraRMIServer(serverStore, reg);
    store = AkubraRMIClient.create(URI.create("rmi://localhost:" + reg + "/" +
                                              AkubraRMIServer.DEFAULT_SERVER_NAME), id);
  }

  @AfterSuite
  public void destroy() throws RemoteException, NotBoundException {
    server.shutDown(true);
  }

  /**
   * Store id should be what it was initialized with.
   */
  @Test
  public void testGetId() {
    assertEquals(id, store.getId());
  }

  /**
   * Request to open a connection without a transaction should fail.
   */
  @Test(expectedExceptions={ IOException.class })
  public void testOpenConnectionNoTransaction() throws IOException {
    store.openConnection(null);
  }

  /**
   * Request to open a connection with a transaction is unsupported.
   */
  @Test()
  public void testOpenConnectionWithTransaction() throws Exception {
    tm.begin();
    try {
      store.openConnection(tm.getTransaction());
    } finally {
      tm.rollback();
    }
  }

  /**
   * Request to go quiescent and non-quiescent (even when already in those
   * states) should be supported.
   */
  @Test()
  public void testSetQuiescent() throws IOException {
    assertTrue(store.setQuiescent(true));
    assertTrue(store.setQuiescent(true));
    assertTrue(store.setQuiescent(false));
    assertTrue(store.setQuiescent(false));
  }

  /**
   * Should return transactional capability.
   */
  @Test()
  public void testGetCapabilities() {
    Set<URI> caps = new HashSet<URI>(store.getCapabilities());
    caps.removeAll(serverStore.getBackingStores().get(0).getCapabilities());
    assertEquals(1, caps.size());
    assertTrue(caps.contains(BlobStore.TXN_CAPABILITY));
  }

  /**
   * Basic create, get, rename, delete.
   */
  @Test()
  public void testBasicCommit() throws Exception {
    URI id = URI.create("urn:blob1");

    createBlob(id, "hello", true);
    getBlob(id, "hello", true);

    URI id2 = URI.create("urn:blob2");
    renameBlob(id, id2, "hello", true);
    getBlob(id, null, true);
    getBlob(id2, "hello", true);

    deleteBlob(id2, true, true);
    getBlob(id2, null, true);
  }

  /**
   * Basic create, get, rename, delete with rollbacks.
   */
  @Test()
  public void testBasicRollback() throws Exception {
    URI id = URI.create("urn:blob1");

    // roll back a create
    createBlob(id, "hello", false);
    getBlob(id, null, false);

    // create, rollback a rename
    createBlob(id, "hello", true);
    getBlob(id, "hello", true);

    URI id2 = URI.create("urn:blob2");
    renameBlob(id, id2, "hello", false);

    getBlob(id, "hello", true);
    getBlob(id2, null, true);

    // rollback a delete
    deleteBlob(id, true, false);
    getBlob(id, "hello", true);

    // delete
    deleteBlob(id, true, true);
    getBlob(id, null, true);
  }

  @Test
  public void testMTStress() throws InterruptedException, ExecutionException {
    ScheduledExecutorService executor = Executors.newScheduledThreadPool(10);
    List<Future<Void>> futures = new ArrayList<Future<Void>>();

    for (int loop = 0; loop < 30; loop++) {
      futures.add(executor.submit(new Callable<Void>() {
        public Void call() throws Exception {
          for (int i = 0; i < 10; i++) {
            doInTxn(new Action() {
              public void run(BlobStoreConnection con) throws Exception {
                for (int j = 0; j < 3; j++) {
                  URI id = URI.create("urn:mt:" + UUID.randomUUID());
                  byte[] buf = new byte[4096];
                  Blob b;
                  b = con.getBlob(id, null);
                  OutputStream out;
                  IOUtils.copyLarge(new ByteArrayInputStream(buf),
                                    out = b.openOutputStream(buf.length, true));
                  out.close();

                  InputStream in;
                  assertEquals(buf, IOUtils.toByteArray(in = b.openInputStream()));
                  in.close();
                  b.delete();
                }
              }
            }, true);
          }
          return null;
        }
      }));
    }

    for (Future<Void> res : futures)
      res.get();
  }

  private void createBlob(final URI id, final String val, boolean commit) throws Exception {
    doInTxn(new Action() {
        public void run(BlobStoreConnection con) throws Exception {
          Blob b = con.getBlob(id, null);
          b.openOutputStream(-1, true).write(val.getBytes());
          assertEquals(val, IOUtils.toString(con.getBlob(id, null).openInputStream()));
        }
    }, commit);
  }

  private void deleteBlob(final URI id, final boolean exists, boolean commit) throws Exception {
    doInTxn(new Action() {
        public void run(BlobStoreConnection con) throws Exception {
          Blob blob = con.getBlob(id, null);
          assertEquals(exists, blob.exists());
          blob.delete();
          assertFalse(blob.exists());
          assertFalse(con.getBlob(id, null).exists());
        }
    }, commit);
  }

  private void getBlob(final URI id, final String val, boolean commit) throws Exception {
    doInTxn(new Action() {
        public void run(BlobStoreConnection con) throws Exception {
          Blob b = con.getBlob(id, null);
          if (val != null)
            assertEquals(val, IOUtils.toString(b.openInputStream()));
          else
            assertFalse(b.exists());
        }
    }, commit);
  }

  private void renameBlob(final URI oldId, final URI newId, final String val, boolean commit)
      throws Exception {
    doInTxn(new Action() {
        public void run(BlobStoreConnection con) throws Exception {
          con.getBlob(oldId, null).moveTo(con.getBlob(newId, null));
          assertFalse(con.getBlob(oldId, null).exists());

          Blob b = con.getBlob(newId, null);
          if (val != null)
            assertEquals(val, IOUtils.toString(b.openInputStream()));
          else
            assertFalse(b.exists());
        }
    }, commit);
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

  private static interface Action {
    public void run(BlobStoreConnection con) throws Exception;
  }
}
