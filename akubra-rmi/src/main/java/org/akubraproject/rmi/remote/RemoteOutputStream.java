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
package org.akubraproject.rmi.remote;

import java.io.IOException;

import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * An interface that lets clients write to an output stream on a remote server.
 *
 * @author Pradeep Krishnan
 */
public interface RemoteOutputStream extends Remote {
  /**
   * Writes a byte to the remote server stream.
   *
   * @param b the byte to write
   *
   * @throws IOException error reported by the remote stream
   * @throws RemoteException error in rmi transport
   */
  void write(int b) throws IOException, RemoteException;

  /**
   * Writes an array of bytes to the remote server stream.
   *
   * @param b the array of bytes to write
   *
   * @throws IOException error reported by the remote stream
   * @throws RemoteException error in rmi transport
   */
  void write(byte[] b) throws IOException, RemoteException;

  /**
   * Writes a partial buffer to the remote server stream.
   *
   * @param b the partial buffer
   *
   * @throws IOException error reported by the remote stream
   * @throws RemoteException error in rmi transport
   */
  void write(PartialBuffer b) throws IOException, RemoteException;

  /**
   * Flushes the remote server stream.
   *
   * @throws IOException error reported by the remote stream
   * @throws RemoteException error in rmi transport
   */
  void flush() throws IOException, RemoteException;

  /**
   * Close this on the remote side.
   *
   * @throws RemoteException on an error in rmi transport
   * @throws IOException on an error reported by remote
   */
  void close() throws RemoteException, IOException;
}
