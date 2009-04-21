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

import java.util.HashMap;
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
  private final Map<String, String> hints;

  /**
   * Creates a new ClientBlob object.
   *
   * @param con the connection
   * @param streamMgr the stream manager
   * @param remote stub for a remote blob
   * @param hints hints used while creating this blob
   *
   * @throws RemoteException on an error in obtaining the blob-id from remote
   */
  public ClientBlob(BlobStoreConnection con, StreamManager streamMgr, RemoteBlob remote,
                    Map<String, String> hints) throws RemoteException {
    super(con, remote.getId());
    this.streamMgr   = streamMgr;
    this.remote      = remote;
    this.hints       = (hints == null) ? null : new HashMap<String, String>(hints);
  }

  @Override
  public URI getCanonicalId() throws IOException {
    if (getConnection().isClosed())
      throw new IOException("Connection closed");

    return remote.getCanonicalId();
  }

  public InputStream openInputStream() throws IOException {
    if (getConnection().isClosed())
      throw new IOException("Connection closed");

    return streamMgr.manageInputStream(getConnection(),
                                       new ClientInputStream(remote.openInputStream()));
  }

  public OutputStream openOutputStream(long estSize) throws IOException {
    if (getConnection().isClosed())
      throw new IOException("Connection closed");

    if (!streamMgr.lockUnquiesced()) {
      throw new IOException("Interrupted waiting for writable state");
    }
    try {
      return streamMgr.manageOutputStream(getConnection(),
                                          new ClientOutputStream(remote.openOutputStream(estSize)));

    } finally {
      streamMgr.unlockState();
    }
  }

  public long getSize() throws IOException {
    if (getConnection().isClosed())
      throw new IOException("Connection closed");

    return remote.getSize();
  }

  public boolean exists() throws IOException {
    if (getConnection().isClosed())
      throw new IOException("Connection closed");

    return remote.exists();
  }

  public void create() throws IOException {
    if (getConnection().isClosed())
      throw new IOException("Connection closed");

    remote.create();
  }

  public void delete() throws IOException {
    if (getConnection().isClosed())
      throw new IOException("Connection closed");

    remote.delete();
  }

  public void moveTo(Blob blob) throws IOException {
    if (getConnection().isClosed())
      throw new IOException("Connection closed");

    if ((blob == null) || (blob.getId() == null))
      throw new NullPointerException("Blob and Blob#getId must be non-null");

    if (!(blob instanceof ClientBlob))
      throw new IllegalArgumentException("Blob must be an instance of '" + ClientBlob.class
                                         + "' instead found " + blob.getClass());

    remote.moveTo(blob.getId(), ((ClientBlob) blob).hints);
  }
}
