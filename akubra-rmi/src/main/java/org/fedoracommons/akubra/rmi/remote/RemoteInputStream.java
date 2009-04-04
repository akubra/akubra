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

import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * Represents an input stream on remote.
 *
 * @author Pradeep Krishnan
  */
public interface RemoteInputStream extends Remote {
  /**
   * Reads the next byte from the remote input stream.
   *
   * @return the byte read
   *
   * @throws RemoteException error on rmi transport
   * @throws IOException error reported by remote
   */
  int read() throws RemoteException, IOException;

  /**
   * Reads from the server stream.
   *
   * @param len number of bytes to read from remote
   *
   * @return the returned buffer or null for end of file
   *
   * @throws RemoteException error on rmi transport
   * @throws IOException error reported by remote
   */
  PartialBuffer read(int len) throws RemoteException, IOException;

  /**
   * Skips bytes on the remote stream.
   *
   * @param n the number of bytes to skip
   *
   * @return the bytes skipped by server
   *
   * @throws RemoteException error on rmi transport
   * @throws IOException error reported by remote
   */
  long skip(long n) throws RemoteException, IOException;

  /**
   * Close this on the remote side.
   *
   * @throws RemoteException on an error in rmi transport
   * @throws IOException on an error reported by remote
   */
  void close() throws RemoteException, IOException;
}
