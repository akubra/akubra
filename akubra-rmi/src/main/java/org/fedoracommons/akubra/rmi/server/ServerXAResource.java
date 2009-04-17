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

import java.io.Serializable;

import java.rmi.RemoteException;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.fedoracommons.akubra.rmi.remote.RemoteXAResource;
import org.fedoracommons.akubra.rmi.remote.SerializedXid;

/**
 * The server side XAResource implementation.
 *
 * @author Pradeep Krishnan
  */
public class ServerXAResource extends UnicastExportable implements RemoteXAResource {
  private static final long       serialVersionUID = 1L;
  private final XAResource        xaRes;
  private final ServerTransactionListener txnListener;

  /**
   * Creates a new ServerXAResource object.
   *
   * @param xaRes the real XAResource
   * @param txnListener the transaction listener that created it
   * @param exporter the exporter to use
   *
   * @throws RemoteException on an export error
   */
  public ServerXAResource(XAResource xaRes, ServerTransactionListener txnListener, Exporter exporter)
                   throws RemoteException {
    super(exporter);
    this.xaRes = xaRes;
    this.txnListener = txnListener;
  }

  public void commit(Xid xid, boolean onePhase) throws XAException {
    xaRes.commit(xid, onePhase);
  }

  public void end(Xid xid, int flags) throws XAException {
    xaRes.end(xid, flags);
  }

  public void forget(Xid xid) throws XAException {
    xaRes.forget(xid);
  }

  public int getTransactionTimeout() throws XAException {
    return xaRes.getTransactionTimeout();
  }

  public boolean isSameRM(RemoteXAResource remote) throws XAException {
    /*
     * Only the ones enlisted from this server is compared. Others are assumed to be not same.
     * This is because we are constructing proxies and the only information used in building
     * the proxies is the one in the public XAResource interface and so we can make an early
     * decision here.
     */
    XAResource local = (remote == null) ? null : txnListener.getXAResource(remote);

    return (local == null) ? false : xaRes.isSameRM(local);
  }

  public int prepare(Xid xid) throws XAException {
    return xaRes.prepare(xid);
  }

  public Xid[] recover(int flag) throws XAException {
    Xid[] ids = xaRes.recover(flag);

    if (ids == null)
      return null;

    for (int i = 0; i < ids.length; i++)
      if ((ids[i] != null) && !(ids[i] instanceof Serializable))
        ids[i] = new SerializedXid(ids[i]);

    return ids;
  }

  public void rollback(Xid xid) throws XAException {
    xaRes.rollback(xid);
  }

  public boolean setTransactionTimeout(int seconds) throws XAException {
    return xaRes.setTransactionTimeout(seconds);
  }

  public void start(Xid xid, int flags) throws XAException {
    xaRes.start(xid, flags);
  }

  // for testing
  XAResource getXAResource() {
    return xaRes;
  }
}
