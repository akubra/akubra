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

package org.fedoracommons.akubra.txn;

import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.fedoracommons.akubra.AbstractBlobStore;
import org.fedoracommons.akubra.BlobStore;
import org.fedoracommons.akubra.Capability;
import org.fedoracommons.akubra.FixedCapability;

/**
 * A basic superclass for transactional stores. This implements the capability and backing-store
 * handling.
 *
 * <p>Subclasses must implement {@link BlobStore#openConnection openConnection}. In there they
 * should first check that {@link #wrappedStore} is not null, and then set {@link #started} to true.
 *
 * @author Ronald Tschalär
 */
public abstract class AbstractTransactionalStore extends AbstractBlobStore {
  /** The URI representing the id of the transactional capability */
  public static final URI TXN_CAP_ID =
                        URI.create("http://fedoracommons.org/akubra/capabilities/transactions");

  private static final Log        logger      = LogFactory.getLog(AbstractTransactionalStore.class);
  private static final Capability txnCapability = new FixedCapability(TXN_CAP_ID);

  /** whether this store has been started */
  protected       boolean   started = false;
  /** the underlying blob-store used for the actual storage */
  protected       BlobStore wrappedStore;

  /**
   * Create a new transactional store. Exactly one backing store must be set before this can
   * be used.
   *
   * @param id the id of this store
   */
  public AbstractTransactionalStore(URI id) throws IOException {
    super(id);
  }

  @Override
  public List<BlobStore> getBackingStores() {
    return (wrappedStore != null) ? Collections.singletonList(wrappedStore) :
                                    Collections.<BlobStore>emptyList();
  }

  /**
   * Set the backing stores. This must be called before {@link #openConnection openConnection} and
   * it must contain exactly one store.
   *
   * @param stores the backing stores to use
   * @throws IllegalStateException if this store has already been started
   * @throws IllegalArgumentException if <var>stores</var> doesn't contain exactly one store
   */
  @Override
  public void setBackingStores(List<BlobStore> stores)
      throws IllegalStateException, IllegalArgumentException {
    if (started)
      throw new IllegalStateException("Already started");
    if (stores.size() != 1)
      throw new IllegalArgumentException("Only one backing store supported; got " + stores.size());

    this.wrappedStore = stores.get(0);
  }

  //@Override
  public boolean setQuiescent(boolean quiescent) throws IOException {
    if (wrappedStore == null)
      throw new IllegalStateException("no backing store has been set yet");

    return wrappedStore.setQuiescent(quiescent);
  }

  /**
   * This store supports exactly one capability, the transactional capability.
   *
   * @return an array of one containing the transactional capability
   */
  @Override
  public Capability[] getDeclaredCapabilities() {
    return new Capability[] { txnCapability };
  }
}
