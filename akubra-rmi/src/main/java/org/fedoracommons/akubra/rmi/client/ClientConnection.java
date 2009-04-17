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
package org.fedoracommons.akubra.rmi.client;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import java.net.URI;

import java.util.Iterator;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.fedoracommons.akubra.Blob;
import org.fedoracommons.akubra.BlobStore;
import org.fedoracommons.akubra.impl.AbstractBlobStoreConnection;
import org.fedoracommons.akubra.impl.StreamManager;
import org.fedoracommons.akubra.rmi.remote.RemoteBlobCreator;
import org.fedoracommons.akubra.rmi.remote.RemoteConnection;
import org.fedoracommons.akubra.rmi.remote.RemoteIterator;

/**
 * Connection returned by the akubra-rmi-client that wraps a remote connection.
 *
 * @author Pradeep Krishnan
 */
class ClientConnection extends AbstractBlobStoreConnection {
  private static final Log       log    = LogFactory.getLog(ClientConnection.class);
  private final RemoteConnection remote;

  /**
   * Number of items to pre-fetch during iteration
   */
  public static int ITERATOR_BATCH_SIZE = 100;

  /**
   * Creates a new ClienteConnection object.
   *
   * @param store the blob store
   * @param strMgr the StreamManager
   * @param con the remote connection stub
   */
  public ClientConnection(BlobStore store, StreamManager strMgr, RemoteConnection con) {
    super(store, strMgr);
    this.remote = con;
  }

  @Override
  public void close() {
    if (!isClosed()) {
      super.close();

      try {
        remote.close();
      } catch (IOException e) {
        log.warn("Failed to close connection on remote server.", e);
      }
    }
  }

  public Blob getBlob(URI blobId, Map<String, String> hints) throws IOException {
    if (isClosed())
      throw new IOException("Connection closed");

    return new ClientBlob(this, streamManager, remote.getBlob(blobId, hints), hints);
  }

  @Override
  public Blob getBlob(InputStream in, long estimatedSize, Map<String, String> hints)
               throws IOException {
    if (isClosed())
      throw new IOException("Connection closed");

    RemoteBlobCreator bc = remote.getBlobCreator(estimatedSize, hints);
    OutputStream out = new ClientOutputStream(bc);
    IOUtils.copyLarge(in, out);
    out.close();

    return new ClientBlob(this, streamManager, bc.getBlob(), hints);
  }

  public Iterator<URI> listBlobIds(String filterPrefix) throws IOException {
    if (isClosed())
      throw new IOException("Connection closed");

    RemoteIterator<URI> ri = remote.listBlobIds(filterPrefix);

    return new ClientIterator<URI>(ri, ITERATOR_BATCH_SIZE);
  }

}
