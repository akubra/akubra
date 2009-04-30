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

package org.fedoracommons.akubra.qsc;

import java.io.IOException;
import java.io.InputStream;

import java.net.URI;

import java.util.Map;

import javax.transaction.Transaction;

import org.fedoracommons.akubra.Blob;
import org.fedoracommons.akubra.BlobStore;
import org.fedoracommons.akubra.BlobStoreConnection;
import org.fedoracommons.akubra.UnsupportedIdException;
import org.fedoracommons.akubra.impl.BlobStoreConnectionWrapper;

/**
 * Provides wrapped-blobs for quiescing management.
 *
 * @author Ronald Tschalär
 */
class QuiescingBlobStoreConnection extends BlobStoreConnectionWrapper {
  private final Transaction transaction;
  private       boolean     modified = false;   // synchronized by stateLock

  /**
   * Creates an instance.
   *
   * @param store the store from which this connection originated.
   * @param connection the wrapped connection.
   * @param tx         the associated transaction, if any
   * @param streamMgr  the stream manager.
   */
  public QuiescingBlobStoreConnection(BlobStore store, BlobStoreConnection connection,
                                      Transaction tx, QuiescingStreamManager streamMgr)
      throws IOException {
    super(store, connection, streamMgr);
    this.transaction = tx;

    streamMgr.register(this, tx);
  }

  //@Override
  public Blob getBlob(URI blobId, Map<String, String> hints)
      throws IOException, UnsupportedIdException, UnsupportedOperationException {
    Blob internalBlob = delegate.getBlob(blobId, hints);
    return new QuiescingBlob(this, internalBlob, streamManager);
  }

  @Override
  public Blob getBlob(InputStream content, long estimatedSize, Map<String, String> hints)
      throws IOException, UnsupportedOperationException {
    waitUnquiescedAndMarkModified();

    Blob internalBlob = delegate.getBlob(content, estimatedSize, hints);
    return new QuiescingBlob(this, internalBlob, streamManager);
  }

  @Override
  public void sync() throws IOException {
    waitUnquiescedAndMarkModified();
    delegate.sync();
  }

  @Override
  public void close() {
    try {
      super.close();
    } finally {
      ((QuiescingStreamManager) streamManager).unregister(this, transaction != null);
    }
  }

  /**
   * If quiesced, wait till unquiesced; then mark this connection as having modifications.
   */
  void waitUnquiescedAndMarkModified() throws IOException {
    ((QuiescingStreamManager) streamManager).lockUnquiesced();
    modified = true;
    ((QuiescingStreamManager) streamManager).unlockState();
  }

  boolean hasModifications() {
    return modified;
  }
}
