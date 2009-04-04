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
package org.fedoracommons.akubra.rmi.client;

import java.rmi.RemoteException;

import java.util.HashMap;
import java.util.Map;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.RollbackException;
import javax.transaction.Synchronization;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.xa.XAResource;

import org.fedoracommons.akubra.rmi.remote.RemoteSynchronization;
import org.fedoracommons.akubra.rmi.remote.RemoteTransaction;
import org.fedoracommons.akubra.rmi.remote.RemoteXAResource;
import org.fedoracommons.akubra.rmi.server.Exporter;
import org.fedoracommons.akubra.rmi.server.ServerSynchronization;
import org.fedoracommons.akubra.rmi.server.ServerXAResource;

/**
 * A transaction wrapper for use in akubra-rmi-server side that forwards calls to the
 * akubra-rmi-client side Transaction.
 *
 * @author Pradeep Krishnan
 */
public class ClientTransaction implements Transaction {
  private final RemoteTransaction                 remote;
  private final Exporter                          exporter;
  private final Map<XAResource, RemoteXAResource> map = new HashMap<XAResource, RemoteXAResource>();

  /**
   * Creates a new ClientTransaction object.
   *
   * @param remote the remote transaction stub
   * @param exporter exporter for exporting XAResource and Synchronization
   */
  public ClientTransaction(RemoteTransaction remote, Exporter exporter) {
    this.remote     = remote;
    this.exporter   = exporter;
  }

  /**
   * Gets the akubra-rmi-server side XAResource for the given remote stub.
   *
   * @param res the remote stub.
   *
   * @return a corresponding enlisted XAResource
   */
  public XAResource getXAResource(RemoteXAResource res) {
    if (res == null)
      return null;

    for (Map.Entry<XAResource, RemoteXAResource> e : map.entrySet())
      if (res.equals(e.getValue()))
        return e.getKey();

    return null;
  }

  public void commit()
              throws RollbackException, HeuristicMixedException, HeuristicRollbackException,
                     SecurityException, IllegalStateException, SystemException {
    try {
      remote.commit();
    } catch (RemoteException e) {
      throw (SystemException) new SystemException("RMI failure").initCause(e);
    }
  }

  public boolean delistResource(XAResource xaRes, int flags)
                         throws IllegalStateException, SystemException {
    try {
      RemoteXAResource rxaRes = map.get(xaRes);

      if (rxaRes == null)
        return false;

      boolean ret = remote.delistResource(rxaRes, flags);

      if (ret)
        map.remove(xaRes);

      return ret;
    } catch (RemoteException e) {
      throw (SystemException) new SystemException("RMI failure").initCause(e);
    }
  }

  public boolean enlistResource(XAResource xaRes)
                         throws RollbackException, IllegalStateException, SystemException {
    try {
      RemoteXAResource rxaRes = map.get(xaRes);

      if ((rxaRes != null) || (xaRes == null))
        return false;

      rxaRes = (RemoteXAResource) new ServerXAResource(xaRes, this, exporter).getExported();

      boolean ret = remote.enlistResource(rxaRes);

      if (ret)
        map.put(xaRes, rxaRes);

      return ret;
    } catch (RemoteException e) {
      throw (SystemException) new SystemException("RMI failure").initCause(e);
    }
  }

  public int getStatus() throws SystemException {
    try {
      return remote.getStatus();
    } catch (RemoteException e) {
      throw (SystemException) new SystemException("RMI failure").initCause(e);
    }
  }

  public void registerSynchronization(Synchronization sync)
                               throws RollbackException, IllegalStateException, SystemException {
    try {
      RemoteSynchronization rsync = new ServerSynchronization(sync, exporter);
      remote.registerSynchronization(rsync);
    } catch (RemoteException e) {
      throw (SystemException) new SystemException("RMI failure").initCause(e);
    }
  }

  public void rollback() throws IllegalStateException, SystemException {
    try {
      remote.rollback();
    } catch (RemoteException e) {
      throw (SystemException) new SystemException("RMI failure").initCause(e);
    }
  }

  public void setRollbackOnly() throws IllegalStateException, SystemException {
    try {
      remote.setRollbackOnly();
    } catch (RemoteException e) {
      throw (SystemException) new SystemException("RMI failure").initCause(e);
    }
  }

  // for tests
  RemoteXAResource getRemoteXAResource(XAResource res) {
    return map.get(res);
  }
}
