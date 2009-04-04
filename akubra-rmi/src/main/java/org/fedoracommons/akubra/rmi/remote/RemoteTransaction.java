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

import java.rmi.Remote;
import java.rmi.RemoteException;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;

/**
 * Represents a transaction on the akubra client side. Note that all method invocations
 * here are from the akubra server side and hence are considered RMI callbacks.
 * 
 * @author Pradeep Krishnan
 */
public interface RemoteTransaction extends Remote {
  /**
   * Enlists an akubra server side resource.
   *
   * @param xaRes the xa resource
   *
   * @return true for successful enlist; false otherwise
   *
   * @throws RemoteException on an error in RMI transport
   * @throws RollbackException error reported by remote
   * @throws SystemException error reported by remote
   */
  boolean enlistResource(RemoteXAResource xaRes)
                  throws RemoteException, RollbackException, SystemException;

  /**
   * Delists an akubra server side resource.
   *
   * @param xaRes the xa resource
   * @param flags delist flags
   *
   * @return true for successful delist; false otherwise
   *
   * @throws RemoteException on an error in RMI transport
   * @throws SystemException error reported by remote
   */
  boolean delistResource(RemoteXAResource xaRes, int flags)
                  throws RemoteException, SystemException;

  /**
   * Gets the current status.
   *
   * @return the status
   *
   * @throws RemoteException on an error in RMI transport
   * @throws SystemException error reported by remote
   */
  int getStatus() throws RemoteException, SystemException;

  /**
   * Registers a synchronization call to akubra server side.
   *
   * @param sync the synchronization to register
   *
   * @throws RemoteException on an error in RMI transport
   * @throws RollbackException error reported by remote
   * @throws SystemException error reported by remote
   */
  void registerSynchronization(RemoteSynchronization sync)
                        throws RemoteException, RollbackException, SystemException;

  /**
   * Marks this transaction as a roll-back only transaction.
   *
   * @throws RemoteException on an error in RMI transport
   * @throws SystemException error reported by remote
   */
  void setRollbackOnly() throws RemoteException, SystemException;

  /**
   * Commits this transaction.
   *
   * @throws RemoteException on an error in RMI transport
   * @throws RollbackException error reported by remote
   * @throws HeuristicMixedException error reported by remote
   * @throws HeuristicRollbackException error reported by remote
   * @throws SystemException error reported by remote
   */
  void commit()
       throws RemoteException, RollbackException, HeuristicMixedException,
              HeuristicRollbackException, SystemException;

  /**
   * Rollsback this transaction.
   *
   * @throws RemoteException on an error in RMI transport
   * @throws SystemException error reported by remote
   */
  void rollback() throws RemoteException, SystemException;
}
