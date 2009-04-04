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

import java.util.Set;

import org.fedoracommons.akubra.rmi.server.Exporter;

/**
 * Represents a remote blob store.
 * 
 * @author Pradeep Krishnan
 */
public interface RemoteStore extends Remote {
  /**
   * Open a connection on the remote blob-store.
   *
   * @param txn the jta transaction on the application/calling side
   *
   * @return a remote-connection handle
   *
   * @throws RemoteException on an RMI communication failure
   * @throws IOException on an error in opening a connection on the remote
   * @throws IllegalStateException if the remote is not ready to open up connections
   */
  public RemoteConnection openConnection(RemoteTransaction txn)
                                  throws RemoteException, IOException, IllegalStateException;

  /**
   * Gets the capabilities of the remote blob-store
   *
   * @return the list of capabilities of the remote blob-store
   *
   * @throws RemoteException on an RMI communication failure
   * @throws IllegalStateException if the remote is not ready yet
   */
  public Set<URI> getCapabilities() throws RemoteException, IllegalStateException;

  /**
   * Gets the export configuration from remote.
   *
   * @return the export configuration
   *
   * @throws RemoteException on an RMI communication failure
   */
  public Exporter getExporter() throws RemoteException;

  /**
   * Sets the remote store into a quiescent state.
   *
   * @param quiescent the state to switch the remote state
   *
   * @return the response value from remote
   *
   * @throws RemoteException on an RMI communication failure
   * @throws IOException on an error from the remote
   */
  public boolean setQuiescent(boolean quiescent) throws RemoteException, IOException;
}
