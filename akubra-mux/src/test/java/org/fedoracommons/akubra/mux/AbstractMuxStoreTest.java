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
package org.fedoracommons.akubra.mux;

import java.io.IOException;

import java.net.URI;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import javax.transaction.Transaction;

import org.fedoracommons.akubra.BlobStore;
import org.fedoracommons.akubra.BlobStoreConnection;
import org.fedoracommons.akubra.UnsupportedIdException;
import org.fedoracommons.akubra.impl.AbstractBlobStore;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

/**
 * Tests for AbstractMuxStore.
 *
 * @author Pradeep Krishnan
 */
public class AbstractMuxStoreTest {
  private final URI        storeId = URI.create("urn:test:mux:store");
  private AbstractMuxStore store;

  /**
   * Sets up things for all stores.
   */
  @BeforeSuite
  public void setUp() {
    store =
      new AbstractMuxStore(storeId) {
          public BlobStoreConnection openConnection(Transaction tx)
                                             throws UnsupportedOperationException, IOException {
            return new AbstractMuxConnection(this, tx) {
                @Override
                public BlobStore getStore(URI blobId, Map<String, String> hints)
                                   throws IOException, UnsupportedIdException {
                  throw new UnsupportedOperationException("Not needed for this test.");
                }
              };
          }
        };
  }

  @AfterSuite
  public void tearDown() {
  }

  /**
   * Tests the constructor.
   */
  @Test
  public void testAbstractMuxStore() {
    assertEquals(storeId, store.getId());
    assertTrue(store.getDeclaredCapabilities().isEmpty());
  }

  /**
   * Tests the backing store management.
   */
  @Test
  public void testBackingStores() {
    store.setBackingStores(new ArrayList<BlobStore>());
    assertTrue(store.getBackingStores().isEmpty());
    assertTrue(store.getCapabilities().isEmpty());

    BlobStore store1               = createMockStore(URI.create("urn:store:1"));
    store.setBackingStores(Collections.singletonList(store1));
    assertEquals(1, store.getBackingStores().size());
    assertEquals(store1, store.getBackingStores().get(0));
    assertTrue(store.getCapabilities().isEmpty());

    BlobStore store2 = createMockStore(URI.create("urn:store:2"), BlobStore.TXN_CAPABILITY);
    BlobStore store3 = createMockStore(URI.create("urn:store:3"), BlobStore.GENERATE_ID_CAPABILITY);

    store.setBackingStores(Arrays.asList(store1, store2, store3));
    assertEquals(3, store.getBackingStores().size());
    assertEquals(store1, store.getBackingStores().get(0));
    assertEquals(store2, store.getBackingStores().get(1));
    assertEquals(store3, store.getBackingStores().get(2));
    assertTrue(store.getCapabilities()
                     .equals(new HashSet<URI>(Arrays.asList(BlobStore.TXN_CAPABILITY,
                                                             BlobStore.GENERATE_ID_CAPABILITY))));

    try {
      store.setBackingStores(Arrays.asList(store1, store2, store3, store1));
      fail("Failed to rcv expected exception.");
    } catch (IllegalArgumentException e) {
    }
  }

  /**
   * Tests the quiescent state management.
   */
  @SuppressWarnings("unchecked")
  @Test
  public void testSetQuiescent() throws IOException {
    MockBlobStore store1 = createMockStore(URI.create("urn:store:1"));
    MockBlobStore store2 = createMockStore(URI.create("urn:store:2"));
    MockBlobStore store3 = createMockStore(URI.create("urn:store:3"));

    store.setBackingStores(Arrays.asList(store1, store2, store3));

    for (MockBlobStore bs : (List<MockBlobStore>) store.getBackingStores())
      assertFalse(bs.qs);

    for (boolean f : new boolean[] { true, true, false, false }) {
      store.setQuiescent(f);

      for (MockBlobStore bs : (List<MockBlobStore>) store.getBackingStores())
        assertEquals(f, bs.qs);
    }

    // Fail if one of them fails
    store2.ret = false;

    for (boolean f : new boolean[] { true, true, false, false }) {
      store.setQuiescent(f);

      for (MockBlobStore bs : (List<MockBlobStore>) store.getBackingStores())
        assertFalse(bs.qs);
    }

    // Fail if one of them throws an exception
    store2.fake = new IOException("Fake a failure");

    try {
      store.setQuiescent(true);
      fail("Failed to get expected exception.");
    } catch (IOException e) {
      assertEquals(store2.fake, e);
    }

    for (MockBlobStore bs : (List<MockBlobStore>) store.getBackingStores())
      assertFalse(bs.qs);
  }

  private MockBlobStore createMockStore(URI id, URI... decCaps) {
    return new MockBlobStore(id, decCaps);
  }

  private static final class MockBlobStore extends AbstractBlobStore {
    private boolean     qs;
    private boolean     ret  = true;
    private IOException fake;

    private MockBlobStore(URI id, URI... decCaps) {
      super(id, decCaps);
    }

    public BlobStoreConnection openConnection(Transaction tx)
                                       throws UnsupportedOperationException, IOException {
      return null;
    }

    public boolean setQuiescent(boolean quiescent) throws IOException {
      if (quiescent && (fake != null))
        throw fake;

      if (ret)
        qs = quiescent;

      return ret;
    }
  }
}
