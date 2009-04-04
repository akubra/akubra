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
package org.fedoracommons.akubra.rmi.server;

import java.rmi.RemoteException;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.xa.XAResource;

import org.fedoracommons.akubra.rmi.client.ClientSynchronization;
import org.fedoracommons.akubra.rmi.client.ClientXAResource;
import org.fedoracommons.akubra.rmi.remote.RemoteSynchronization;
import org.fedoracommons.akubra.rmi.remote.RemoteTransaction;
import org.fedoracommons.akubra.rmi.remote.RemoteXAResource;

/**
 * The server side (akubra-rmi-client side) implementation of a Transaction.
 *
 * @author Pradeep Krishnan
 */
public class ServerTransaction extends Exportable implements RemoteTransaction {
  private static final long serialVersionUID = 1L;
  private final Transaction                       txn;
  private final Map<RemoteXAResource, XAResource> map = new HashMap<RemoteXAResource, XAResource>();

  /**
   * Creates a new ServerTransaction object.
   *
   * @param txn the real transaction to forward calls to
   * @param exporter the exporter
   *
   * @throws RemoteException on an export error
   */
  public ServerTransaction(Transaction txn, Exporter exporter)
                    throws RemoteException {
    super(exporter);
    this.txn = txn;
  }

  /**
   * Gets the remote XAResource corresponding to the local enlisted one.
   *
   * @param res the XAResource
   *
   * @return the remote stub
   */
  public RemoteXAResource getRemoteXAResource(XAResource res) {
    if (res == null)
      return null;

    for (Map.Entry<RemoteXAResource, XAResource> e : map.entrySet())
      if (res.equals(e.getValue()))
        return e.getKey();

    return null;
  }

  /**
   * Gets the list of all enlisted resource.
   *
   * @return the list 
   */
  public Collection<XAResource> getEnlistedResources() {
    return map.values();
  }

  public void commit()
              throws RollbackException, HeuristicMixedException, HeuristicRollbackException,
                     SystemException {
    txn.commit();
  }

  public boolean delistResource(RemoteXAResource xaRes, int flags)
                         throws SystemException {
    XAResource res = map.get(xaRes);

    if (res == null)
      return false;

    boolean ret = txn.delistResource(res, flags);

    if (ret)
      map.remove(xaRes);

    return ret;
  }

  public boolean enlistResource(RemoteXAResource xaRes)
                         throws RollbackException, SystemException {
    XAResource res = map.get(xaRes);

    if (res != null)
      return false;

    res = (xaRes == null) ? null : new ClientXAResource(xaRes, this);

    boolean ret = txn.enlistResource(res);

    if (ret)
      map.put(xaRes, res);

    return ret;
  }

  public int getStatus() throws SystemException {
    return txn.getStatus();
  }

  public void registerSynchronization(RemoteSynchronization sync)
                               throws RollbackException, SystemException {
    txn.registerSynchronization(new ClientSynchronization(sync));
  }

  public void rollback() throws SystemException {
    txn.rollback();
  }

  public void setRollbackOnly() throws SystemException {
    txn.setRollbackOnly();
  }

  // for testing
  XAResource getXAResource(RemoteXAResource remote) {
    return map.get(remote);
  }
}
