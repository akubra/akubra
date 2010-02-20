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

import java.rmi.Remote;
import java.rmi.RemoteException;

import javax.transaction.xa.XAException;
import javax.transaction.xa.Xid;

/**
 * Represents an XA Resource on tha akubra server side.
 * 
 * @author Pradeep Krishnan
 */
public interface RemoteXAResource extends Remote {
  /**
   * Commit directive.
   *
   * @param xid the transaction id
   * @param onePhase true if doing a single phase commit
   *
   * @throws XAException error reported by remote
   * @throws RemoteException on an error in RMI transport
   */
  void commit(Xid xid, boolean onePhase) throws XAException, RemoteException;

  /**
   * End directive.
   *
   * @param xid the transaction id
   * @param flags transaction flags
   *
   * @throws XAException error reported by remote
   * @throws RemoteException on an error in RMI transport
   */
  void end(Xid xid, int flags) throws XAException, RemoteException;

  /**
   * Forget directive.
   *
   * @param xid the transaction id
   *
   * @throws XAException error reported by remote
   * @throws RemoteException on an error in RMI transport
   */
  void forget(Xid xid) throws XAException, RemoteException;

  /**
   * Gets the transaction timeout.
   *
   * @return the timeout
   *
   * @throws XAException error reported by remote
   * @throws RemoteException on an error in RMI transport
   */
  int getTransactionTimeout() throws XAException, RemoteException;

  /**
   * Tests if this is the same Resource Manager
   *
   * @param xares the other resource
   *
   * @return true if they are the same; false otherwise
   *
   * @throws XAException error reported by remote
   * @throws RemoteException on an error in RMI transport
   */
  boolean isSameRM(RemoteXAResource xares) throws XAException, RemoteException;

  /**
   * Prepare directive
   *
   * @param xid the transaction id
   *
   * @return status code
   *
   * @throws XAException error reported by remote
   * @throws RemoteException on an error in RMI transport
   */
  int prepare(Xid xid) throws XAException, RemoteException;

  /**
   * Gets the recoverable transactions.
   *
   * @param flags transaction flags
   *
   * @return list of recoverable transactions
   *
   * @throws XAException error reported by remote
   * @throws RemoteException on an error in RMI transport
   */
  Xid[] recover(int flags) throws XAException, RemoteException;

  /**
   * Rollback directive.
   *
   * @param xid the transaction id
   *
   * @throws XAException error reported by remote
   * @throws RemoteException on an error in RMI transport
   */
  void rollback(Xid xid) throws XAException, RemoteException;

  /**
   * Sets the transaction timeout.
   *
   * @param seconds the timeout
   *
   * @return true on success; false otherwise
   *
   * @throws XAException error reported by remote
   * @throws RemoteException on an error in RMI transport
   */
  boolean setTransactionTimeout(int seconds) throws XAException, RemoteException;

  /**
   * Start directive.
   *
   * @param xid the transaction id
   * @param flags transaction flags
   *
   * @throws XAException error reported by remote
   * @throws RemoteException on an error in RMI transport
   */
  void start(Xid xid, int flags) throws XAException, RemoteException;
}
