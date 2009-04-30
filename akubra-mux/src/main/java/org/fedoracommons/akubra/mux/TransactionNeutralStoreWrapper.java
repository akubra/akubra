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
import java.util.Map;

import javax.transaction.Transaction;

import org.fedoracommons.akubra.BlobStore;
import org.fedoracommons.akubra.BlobStoreConnection;
import org.fedoracommons.akubra.impl.AbstractBlobStore;

/**
 * A BlobStore wrapper that allows mixing in non-transactional stores
 * with transactional stores in a mux. Note that the returned BlobStoreConnection
 * is not wrapped and therefore {@link BlobStoreConnection#getBlobStore()} will
 * return the original wrapped store.
 *
 * @author Pradeep Krishnan
 */
public class TransactionNeutralStoreWrapper extends AbstractBlobStore {
  private final BlobStore store;
  /**
   * Creates a new TransactionNeutralStoreWrapper object.
   *
   * @param store the wrapped non-transactional store
   */
  public TransactionNeutralStoreWrapper(BlobStore store) {
    super(store.getId());
    this.store = store;
  }

  public BlobStoreConnection openConnection(Transaction tx, Map<String, String> hints)
      throws UnsupportedOperationException, IOException {
    return store.openConnection(null, hints);
  }
}
