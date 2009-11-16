/* $HeadURL::                                                                            $
 * $Id$
 *
 * Copyright (c) 2009 DuraSpace
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
package org.akubraproject.rmi.server;

import java.io.IOException;
import java.rmi.RemoteException;
import java.util.Map;

import org.akubraproject.BlobStore;
import org.akubraproject.rmi.remote.RemoteConnection;
import org.akubraproject.rmi.remote.RemoteStore;
import org.akubraproject.rmi.remote.RemoteTransactionListener;

/**
 * Server side implementation of the RemoteStore.
 *
 * @author Pradeep Krishnan
 */
public class ServerStore extends Exportable implements RemoteStore {
  private static final long serialVersionUID = 1L;
  private final BlobStore store;

  /**
   * Creates a new RemoteStoreServer object.
   *
   * @param store the blob store
   * @param exporter the exporter to use
   *
   * @throws RemoteException on an export error
   */
  public ServerStore(BlobStore store, Exporter exporter) throws RemoteException {
    super(exporter);
    this.store = store;
  }

  public RemoteConnection openConnection(Map<String, String> hints) throws IOException {
    return new ServerConnection(store.openConnection(null, hints), getExporter());
  }

  public RemoteTransactionListener startTransactionListener(Map<String, String> hints)
      throws RemoteException {
    return new ServerTransactionListener(store, hints, getExporter());
  }
}
