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
package org.fedoracommons.akubra.rmi.remote;

import java.io.IOException;

import java.net.URI;

import java.rmi.Remote;
import java.rmi.RemoteException;

import java.util.Map;

import org.fedoracommons.akubra.DuplicateBlobException;
import org.fedoracommons.akubra.MissingBlobException;

/**
 * Reperesents a blob on the remote server.
 *
 * @author Pradeep Krishnan
 */
public interface RemoteBlob extends Remote {
  /**
   * Gets the id of the blob.
   *
   * @return the non-null immutable blob id
   *
   * @throws RemoteException on an error in rmi transport
   */
  URI getId() throws RemoteException;

  /**
   * Tests if this blob exists on remote.
   *
   * @return the size
   *
   * @throws RemoteException on an error in rmi transport
   * @throws IOException error reported by the remote
   */
  boolean exists() throws RemoteException, IOException;

  /**
   * Gets the size of the blob from remote.
   *
   * @return the size
   *
   * @throws RemoteException on an error in rmi transport
   * @throws IOException error reported by the remote
   * @throws MissingBlobException error reported by the remote
   */
  long getSize() throws RemoteException, IOException, MissingBlobException;

  /**
   * Creates a blob on the remote.
   *
   * @throws RemoteException on an error in rmi transport
   * @throws DuplicateBlobException error reported by the remote
   * @throws IOException error reported by the remote
   */
  void create() throws RemoteException, DuplicateBlobException, IOException;

  /**
   * Deletes a blob on the remote.
   *
   * @throws RemoteException on an error in rmi transport
   * @throws IOException error reported by the remote
   */
  void delete() throws RemoteException, IOException;

  /**
   * Moves the blob contents to another on the remote.
   *
   * @param id the other blob
   * @param hints the hints to pass to the remote
   *
   * @throws RemoteException on an error in rmi transport
   * @throws DuplicateBlobException error reported by remote
   * @throws MissingBlobException error reported by remote
   * @throws UnsupportedOperationException error reported by remote
   * @throws IOException error reported by the remote
   */
  void moveTo(URI id, Map<String, String> hints)
       throws RemoteException, DuplicateBlobException, MissingBlobException,
              UnsupportedOperationException, IOException;

  /**
   * Create a stream to read from the blob on the remote.
   *
   * @return a new stream to read from the remote server blob.
   *
   * @throws RemoteException on an error in rmi transport
   * @throws IOException error reported by remote
   * @throws MissingBlobException error reported by remote
   */
  RemoteInputStream openInputStream() throws RemoteException, IOException, MissingBlobException;

  /**
   * Create a stream to write to the blob on the remote.
   *
   * @param estimatedSize the estimated size of this blob when the  write is complete
   *
   * @return a new stream to write to the remote server blob
   *
   * @throws RemoteException on an error in rmi transport
   * @throws IOException error reported by remote
   * @throws MissingBlobException error reported by remote
   */
  RemoteOutputStream openOutputStream(long estimatedSize)
                               throws RemoteException, IOException, MissingBlobException;
}
