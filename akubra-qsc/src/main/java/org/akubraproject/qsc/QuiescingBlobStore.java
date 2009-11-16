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
package org.akubraproject.qsc;

import java.io.IOException;

import java.net.URI;

import java.util.Map;

import javax.transaction.Transaction;

import org.akubraproject.BlobStore;
import org.akubraproject.BlobStoreConnection;
import org.akubraproject.impl.AbstractBlobStore;

/**
 * Provide quiescing capability to a stack. While quiescent, all writes and updates are blocked,
 * but read operations are still allowed.
 *
 * <p>This implementation is transaction aware: when asked to quiesce, it will wait for all
 * transactions with modifications to complete. It will allow new transactions to begin, but
 * will block all updates.
 *
 * @author Ronald Tschalär
 */
public class QuiescingBlobStore extends AbstractBlobStore {
  private final BlobStore              store;
  private final QuiescingStreamManager streamMgr = new QuiescingStreamManager();

  /**
   * Creates an instance.
   *
   * @param id the id associated with this store.
   * @param store the store to wrap.
   */
  public QuiescingBlobStore(URI id, BlobStore store) {
    super(id);
    this.store = store;
  }

  //@Override
  public BlobStoreConnection openConnection(Transaction tx, Map<String, String> hints)
      throws IOException {
    BlobStoreConnection connection = store.openConnection(tx, hints);
    return new QuiescingBlobStoreConnection(this, connection, tx, streamMgr);
  }

  /**
   * Makes the stack quiescent or non-quiescent.
   *
   * <p>When going from non-quiescent to quiescent, this call blocks until all active writes and
   * all transactions with writes have completed and any caches have been flushed. While in the
   * quiescent state, the stack will continue to honor read requests, but will block all write
   * requests including moves and deletes.
   *
   * <p>This method is idempotent.
   *
   * @param quiescent whether to go into the quiescent (true) or non-quiescent (false) state.
   * @return true if successful, false if the thread was interrupted while blocking.
   * @throws IOException if an error occurred trying to quiesce the stack.
   */
  public boolean setQuiescent(boolean quiescent) throws IOException {
    return streamMgr.setQuiescent(quiescent);
  }
}
