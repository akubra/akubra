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
package org.akubraproject.rmi.remote;

import java.io.IOException;

import java.net.URI;

import java.rmi.Remote;
import java.rmi.RemoteException;

import java.util.Map;

import org.akubraproject.UnsupportedIdException;

/**
 * Represents a connection to a blob-store on a remote server.
 *
 * @author Pradeep Krishnan
  */
public interface RemoteConnection extends Remote {
  /**
   * Gets a blob handle for a remote blob.
   *
   * @param id the blob id to pass to the remote server
   * @param hints the hints to pass to the remote server
   *
   * @return the remote blob handle
   *
   * @throws RemoteException on an error in rmi transport
   * @throws IOException error reported by the remote server
   * @throws UnsupportedIdException error reported by the remote server
   */
  RemoteBlob getBlob(URI id, Map<Object, Object> hints)
              throws RemoteException, IOException, UnsupportedIdException;

  /**
   * Gets a blob creator for creating a blob from user supplied content.
   *
   * @param estimatedSize estimated size to pass to the remote server
   * @param hints the hints to pass to the remote server
   *
   * @return the remote blob handle
   *
   * @throws RemoteException on an error in rmi transport
   * @throws IOException error reported by the remote server
   */
  RemoteBlobCreator getBlobCreator(long estimatedSize, Map<Object, Object> hints)
              throws RemoteException, IOException;

  /**
   * Gets an iterator from the remote server for listing matching blobs.
   *
   * @param filterPrefix the filterPrefix to pass to the remote
   *
   * @return the remote iterator
   *
   * @throws RemoteException on an error in rmi transport
   * @throws IOException error reported by remote server
   */
  RemoteIterator<URI> listBlobIds(String filterPrefix)
                           throws RemoteException, IOException;

  /**
   * Flush all blobs associated with the remote connection and fsync.
   *
   * @throws RemoteException on an error in rmi transport
   * @throws IOException error reported by remote server
   */
  void sync() throws RemoteException, IOException;

  /**
   * Close this on the remote side.
   *
   * @throws RemoteException on an error in rmi transport
   */
  void close() throws RemoteException;
}
