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
package org.akubraproject.rmi.client;

import java.rmi.RemoteException;

import javax.transaction.Synchronization;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.akubraproject.rmi.remote.RemoteSynchronization;

/**
 * A client side Synchronization wrapper that forwards to a RemoteSynchronization.
 *
 * @author Pradeep Krishnan
  */
class ClientSynchronization implements Synchronization {
  private static final Log            log    = LogFactory.getLog(ClientSynchronization.class);
  private final RemoteSynchronization remote;

  /**
   * Creates a new ClientSynchronization object.
   *
   * @param remote the remote stub
   */
  public ClientSynchronization(RemoteSynchronization remote) {
    this.remote = remote;
  }

  public void afterCompletion(int status) {
    try {
      remote.afterCompletion(status);
    } catch (RemoteException e) {
      log.warn("Failed to execute afterCompletion() on remote", e);
      throw new RuntimeException("Failure to contact remote", e);
    }
  }

  public void beforeCompletion() {
    try {
      remote.beforeCompletion();
    } catch (RemoteException e) {
      log.warn("Failed to execute beforeCompletion on remote", e);
      throw new RuntimeException("Failure to contact remote", e);
    }
  }
}
