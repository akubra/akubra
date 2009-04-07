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

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.fedoracommons.akubra.BlobStore;
import org.fedoracommons.akubra.impl.AbstractBlobStore;

/**
 * An abstract base class for store implementations that multiplexes its backing stores to give
 * a unified view. The capabilities of this store is a union of all backing store capabilities.<p>This
 * store allows transactional and non transactional stores to be multiplexed together. If at least
 * one of the backing stores is transactional, this store appears as a transactional store. In
 * that case the passed in transaction for an openConnection() is only used for the backing
 * transactional stores. All other non transactional stores will be opened without a transaction
 * being passed in.</p>
 *  <p>For any operation involving a blobId, this store selects a backing store as the target
 * for that operation. This selection is made based on a sub-class's implementation of {@link
 * AbstractMuxConnection#getStore}. It is expected that the store selection made by this method
 * will also take the store capability into consideration.</p>
 *  <p>Renames across BlobStores is implemented as a copy. Note that any error during that
 * process may leave partially completed blobs if the underlying blobs are non transactional.</p>
 *  <p>Iterations and quiescenting etc. are performed across all stores.</p>
 *
 * @author Pradeep Krishnan
 */
public abstract class AbstractMuxStore extends AbstractBlobStore {
  private static final Log log = LogFactory.getLog(AbstractMuxStore.class);

  /**
   * The list of backing stores. Note that the order is same as the user supplied list.
   */
  protected List<BlobStore> backingStores = Collections.emptyList();

  /**
   * The current quiescent state.
   */
  protected boolean quiesced = false;

  /**
   * Creates a new AbstractMuxStore object.
   *
   * @param id an identifier for this store
   */
  protected AbstractMuxStore(URI id) {
    super(id);
  }

  /**
   * Create a new AbstractMuxStore object.
   *
   * @param id the store's id
   * @param decCaps declared capabilities of this store
   */
  protected AbstractMuxStore(URI id, URI... decCaps) {
    super(id, decCaps);
  }

  @Override
  public List<?extends BlobStore> getBackingStores() {
    return backingStores;
  }

  @Override
  public void setBackingStores(List<?extends BlobStore> stores)
                        throws UnsupportedOperationException, IllegalStateException {
    Set<URI> ids               = new HashSet<URI>();

    for (BlobStore s : stores)
      if (!ids.add(s.getId()))
        throw new IllegalArgumentException("Duplicate store-id: " + s.getId());

    backingStores = Collections.unmodifiableList(stores);
  }

  public boolean setQuiescent(boolean quiescent) throws IOException {
    if (quiesced == quiescent)
      return true;

    boolean ret = quiescent ? makeQuiescent() : cancelQuiescent();

    if (ret)
      quiesced = quiescent;

    return ret;
  }

  /**
   * Makes all backing stores quiescent.
   *
   * @return true on success; false otherwise
   *
   * @throws IOException on an error.
   */
  protected boolean makeQuiescent() throws IOException {
    try {
      for (BlobStore store : backingStores) {
        if (!store.setQuiescent(true)) {
          try {
            cancelQuiescent();
          } catch (Exception ce) {
            log.warn("Failed to cancel quiescent mode", ce);
          }

          return false;
        }
      }

      return true;
    } catch (Exception e) {
      try {
        cancelQuiescent();
      } catch (Exception ce) {
        log.warn("Failed to cancel quiescent mode", ce);
      }

      if (e instanceof IOException)
        throw (IOException) e;

      if (e instanceof RuntimeException)
        throw (RuntimeException) e;

      throw new Error("Unexpected exception", e);
    }
  }

  /**
   * Cancel queiscent state for all stores.
   *
   * @return true if all stores successfully canceled a qquiesced
   *
   * @throws IOException on an error
   */
  protected boolean cancelQuiescent() throws IOException {
    boolean pass = true;

    for (BlobStore store : backingStores) {
      if (!store.setQuiescent(false))
        pass = false;
    }

    return pass;
  }
}
