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
package org.fedoracommons.akubra;

import java.io.IOException;
import java.net.URI;

import java.util.List;

import javax.transaction.Transaction;

/**
 * Interface to abstract the idea of a general transaction based blob store
 *
 * @author Chris Wilper
 * @author Pradeep Krishnan
 * @author Ronald Tschalär
 */
public interface BlobStore {
  /** The URI representing the id of the transactional capability */
  public static final URI TXN_CAPABILITY =
                        URI.create("http://fedoracommons.org/akubra/capabilities/transactions");
  /**
   * The URI representing the id generation capability. Indicates this store can generate an
   * identifier for blobs when an identifier is not supplied.
   */
  public static final URI GENERATE_ID_CAPABILITY =
                        URI.create("http://fedoracommons.org/akubra/capabilities/generateIds");
  /**
   * The URI representing the id accept capability. Indicates this store can accept any valid
   * absolute URI supplied by the application as a blob identifier.
   */
  public static final URI ACCEPT_APP_ID_CAPABILITY =
                        URI.create("http://fedoracommons.org/akubra/capabilities/acceptAppIds");

  /**
   * Return the identifier associated with the store
   *
   * @return the URI identifying the blob store
   */
  URI getId();

  /**
   * Return the list of stores underlying this store
   *
   * @return list of underlying blob stores
   */
  List<BlobStore> getBackingStores();

  /**
   * Set the list of back stores.
   *
   * @param stores the list of backing store
   * @throws UnsupportedOperationException if setting the backing stores is
   *     not supported by this blob store.
   * @throws IllegalStateException TODO: Document conditions under which this
   *     is thrown.  Also: is it thrown by other methods?
   */
  void setBackingStores(List<BlobStore> stores)
    throws UnsupportedOperationException, IllegalStateException;

  /**
   * Opens a connection to the blob store.
   * <p>
   * If the blob store is transactional, the caller must provide a
   * <code>Transaction</code>.  Once provided, the connection will be valid for
   * the life of the transaction, at the end of which it will be automatically
   * {@link BlobStoreConnection#close close()}'d.
   * <p>
   * If the blob store is not transactional, the caller must provide
   * <code>null</code>.  Once provided, the connection will be valid until
   * explicitly closed.
   * <p>
   * Implementations are expected to do any desired pooling of underlying
   * connections themselves.
   * <p>
   * @param tx the transaction associated with this connection, or null if
   *     the blob store is not transactional
   * @return the connection to the blob store
   * @throws UnsupportedOperationException if the blob store is transactional
   *     but a Transaction is not provided by the caller, or if the blob store
   *     is not transactional and a Transaction is provided by the caller.
   * @throws IOException if an error occurred trying to open the connection.
   */
  BlobStoreConnection openConnection(Transaction tx)
      throws UnsupportedOperationException, IOException;

  /**
   * Makes the blob store quiescent or non-quiescent.
   * <p>
   * When going from non-quiescent to quiescent, this call blocks until all active writes have
   * completed any caches have been flushed.  While in the quiescent state, the store may continue
   * to honor read requests, but must block on write requests.
   *
   * @param quiescent whether to go into the quiescent (true) or non-quiescent (false) state.
   * @return true if successful, false if the thread was interrupted while blocking.
   * @throws IOException if an error occurred trying to quiesce the store.
   */
  boolean setQuiescent(boolean quiescent) throws IOException;

  /**
   * Get capabilities of this blob store instance only
   *
   * @return array of Capability
   */
  Capability[] getDeclaredCapabilities();

  /**
   * Return capabilities of this store plus underlying blob stores, that is of the blob stack
   * starting at this level.
   *
   * @return array of Capability
   */
  Capability[] getCapabilities();
}
