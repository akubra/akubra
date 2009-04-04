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

import java.io.Serializable;

import java.rmi.RemoteException;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.fedoracommons.akubra.rmi.remote.RemoteXAResource;
import org.fedoracommons.akubra.rmi.remote.SerializedXid;
import org.fedoracommons.akubra.rmi.server.ServerTransaction;

/**
 * An XAResource wrapper that forwards all calls to a RemoteXAResource.
 *
 * @author Pradeep Krishnan
  */
public class ClientXAResource implements XAResource {
  private final RemoteXAResource  remote;
  private final ServerTransaction txn;

  /**
   * Creates a new ClientXAResource object.
   *
   * @param xaRes the remote stub
   * @param txn the akubra-rmi-client side RemoteTransaction implementation that created this
   */
  public ClientXAResource(RemoteXAResource xaRes, ServerTransaction txn) {
    this.remote   = xaRes;
    this.txn      = txn;

    if (txn == null)
      throw new IllegalArgumentException("txn must be non-null");
  }

  public void commit(Xid xid, boolean onePhase) throws XAException {
    try {
      remote.commit(serialized(xid), onePhase);
    } catch (RemoteException e) {
      throw (XAException) new XAException("RMI faiure").initCause(e);
    }
  }

  public void end(Xid xid, int flags) throws XAException {
    try {
      remote.end(serialized(xid), flags);
    } catch (RemoteException e) {
      throw (XAException) new XAException("RMI faiure").initCause(e);
    }
  }

  public void forget(Xid xid) throws XAException {
    try {
      remote.forget(serialized(xid));
    } catch (RemoteException e) {
      throw (XAException) new XAException("RMI faiure").initCause(e);
    }
  }

  public int getTransactionTimeout() throws XAException {
    try {
      return remote.getTransactionTimeout();
    } catch (RemoteException e) {
      throw (XAException) new XAException("RMI faiure").initCause(e);
    }
  }

  public boolean isSameRM(XAResource xa) throws XAException {
    try {
      /*
       * Only the ones enlisted from the remote is compared. Others are assumed to be not same.
       * This is because we are constructing proxies and the only information used in building
       * the proxies is the one in the public XAResource interface and so we can make an early
       * decision here.
       */
      RemoteXAResource rxa = txn.getRemoteXAResource(xa);

      return (rxa == null) ? false : remote.isSameRM(rxa);
    } catch (RemoteException e) {
      throw (XAException) new XAException("RMI faiure").initCause(e);
    }
  }

  public int prepare(Xid xid) throws XAException {
    try {
      return remote.prepare(serialized(xid));
    } catch (RemoteException e) {
      throw (XAException) new XAException("RMI faiure").initCause(e);
    }
  }

  public Xid[] recover(int flag) throws XAException {
    try {
      return remote.recover(flag);
    } catch (RemoteException e) {
      throw (XAException) new XAException("RMI faiure").initCause(e);
    }
  }

  public void rollback(Xid xid) throws XAException {
    try {
      remote.rollback(serialized(xid));
    } catch (RemoteException e) {
      throw (XAException) new XAException("RMI faiure").initCause(e);
    }
  }

  public boolean setTransactionTimeout(int seconds) throws XAException {
    try {
      return remote.setTransactionTimeout(seconds);
    } catch (RemoteException e) {
      throw (XAException) new XAException("RMI faiure").initCause(e);
    }
  }

  public void start(Xid xid, int flag) throws XAException {
    try {
      remote.start(serialized(xid), flag);
    } catch (RemoteException e) {
      throw (XAException) new XAException("RMI faiure").initCause(e);
    }
  }

  private static Xid serialized(Xid xid) {
    return ((xid == null) || (xid instanceof Serializable)) ? xid : new SerializedXid(xid);
  }

  // for testing
  RemoteXAResource getRemote() {
    return remote;
  }
}
