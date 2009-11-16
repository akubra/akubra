/* $HeadURL$
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

import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * Represents a JTA Synchronization listener on the server side.
 *
 * @author Pradeep Krishnan
  */
public interface RemoteSynchronization extends Remote {
  /**
   * Called before txn completion.
   *
   * @throws RemoteException on an error in RMI transaport
   */
  void beforeCompletion() throws RemoteException;

  /**
   * Called after txn completion
   *
   * @param status the current transaction status
   *
   * @throws RemoteException on an error in RMI transport
   */
  void afterCompletion(int status) throws RemoteException;
}
