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
package org.fedoracommons.akubra.rmi.client;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import java.net.URI;

import java.rmi.RemoteException;

import java.util.Map;

import org.fedoracommons.akubra.Blob;
import org.fedoracommons.akubra.BlobStoreConnection;
import org.fedoracommons.akubra.impl.AbstractBlob;
import org.fedoracommons.akubra.impl.StreamManager;
import org.fedoracommons.akubra.rmi.remote.RemoteBlob;

/**
 * A wrapper for a server side Blob.
 *
 * @author Pradeep Krishnan
 */
class ClientBlob extends AbstractBlob {
  private final RemoteBlob          remote;
  private final StreamManager       streamMgr;

  /**
   * Creates a new ClientBlob object.
   *
   * @param con the connection
   * @param streamMgr the stream manager
   * @param remote stub for a remote blob
   *
   * @throws RemoteException on an error in obtaining the blob-id from remote
   */
  public ClientBlob(BlobStoreConnection con, StreamManager streamMgr,
                    RemoteBlob remote) throws RemoteException {
    super(con, remote.getId());
    this.streamMgr   = streamMgr;
    this.remote      = remote;
  }

  @Override
  public URI getCanonicalId() throws IOException {
    ensureOpen();

    return remote.getCanonicalId();
  }

  public InputStream openInputStream() throws IOException {
    ensureOpen();

    return streamMgr.manageInputStream(getConnection(),
                                       new ClientInputStream(remote.openInputStream()));
  }

  public OutputStream openOutputStream(long estSize, boolean overwrite) throws IOException {
    ensureOpen();

    if (!streamMgr.lockUnquiesced()) {
      throw new IOException("Interrupted waiting for writable state");
    }
    try {
      return streamMgr.manageOutputStream(getConnection(),
                              new ClientOutputStream(remote.openOutputStream(estSize, overwrite)));

    } finally {
      streamMgr.unlockState();
    }
  }

  public long getSize() throws IOException {
    ensureOpen();

    return remote.getSize();
  }

  public boolean exists() throws IOException {
    ensureOpen();

    return remote.exists();
  }

  public void delete() throws IOException {
    ensureOpen();

    remote.delete();
  }

  public Blob moveTo(URI blobId, Map<String, String> hints) throws IOException {
    ensureOpen();

    return new ClientBlob(getConnection(), streamMgr, remote.moveTo(blobId, hints));
  }
}
