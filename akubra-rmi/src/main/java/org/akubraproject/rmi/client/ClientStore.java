/* $HeadURL::                                                                            $
 * $Id$
 *
 * Copyright (c) 2009-2010 DuraSpace
 * http://duraspace.org
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
package org.akubraproject.rmi.client;

import java.io.IOException;

import java.net.URI;

import java.util.Map;

import javax.transaction.Transaction;

import org.akubraproject.BlobStoreConnection;
import org.akubraproject.impl.AbstractBlobStore;
import org.akubraproject.impl.StreamManager;
import org.akubraproject.rmi.remote.RemoteConnection;
import org.akubraproject.rmi.remote.RemoteStore;

/**
 * A BlobStore that forwards calls to a RemoteStore.
 *
 * @author Pradeep Krishnan
 */
public class ClientStore extends AbstractBlobStore {
  private final StreamManager streamManager = new StreamManager();
  private final RemoteStore   server;

  /**
   * Creates a new ClientStore object.
   *
   * @param localId the id of this local store
   * @param server the remote store stub
   *
   * @throws IOException on an error in talking to the remote
   */
  public ClientStore(URI localId, RemoteStore server) throws IOException {
    super(localId);
    this.server = server;
  }

  @Override
  public BlobStoreConnection openConnection(Transaction tx, Map<String, String> hints)
                                     throws UnsupportedOperationException, IOException {
    RemoteConnection con = (tx == null) ? server.openConnection(hints)
        : new ClientTransactionListener(server.startTransactionListener(hints), tx).getConnection();

    return new ClientConnection(this, streamManager, con);
  }
}
