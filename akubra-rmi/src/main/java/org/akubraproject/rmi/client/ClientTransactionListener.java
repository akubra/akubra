/* $HeadURL$
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
package org.akubraproject.rmi.client;

import java.io.IOException;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.xa.XAResource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.akubraproject.rmi.remote.RemoteCallListener.Operation;
import org.akubraproject.rmi.remote.RemoteCallListener.Result;
import org.akubraproject.rmi.remote.RemoteConnection;
import org.akubraproject.rmi.remote.RemoteTransactionListener;
import org.akubraproject.rmi.remote.RemoteTransactionListener.DelistXAResource;
import org.akubraproject.rmi.remote.RemoteTransactionListener.EnlistXAResource;
import org.akubraproject.rmi.remote.RemoteTransactionListener.GetStatus;
import org.akubraproject.rmi.remote.RemoteTransactionListener.RegisterSynchronization;
import org.akubraproject.rmi.remote.RemoteTransactionListener.Rollback;
import org.akubraproject.rmi.remote.RemoteXAResource;

/**
 * A listener for operations on Transaction from akubra-server side and forwards calls to the
 * real akubra-client side Transaction. Currently the listener stops listening after the
 * BlobStoreConnection is opened.
 *
 * @author Pradeep Krishnan
 */
class ClientTransactionListener {
  private static final Log log = LogFactory.getLog(ClientTransactionListener.class);

  // Stub from remote
  private final RemoteTransactionListener remote;

  // Local txn to forward calls to. 
  private final Transaction txn;

  // Cached remote connection stub;
  private RemoteConnection con;

  // To support isSameRM
  private final Map<RemoteXAResource, XAResource> map = new HashMap<RemoteXAResource, XAResource>();

  /**
   * Creates a new ClientTransactionListener object.
   *
   * @param remote the remote transaction stub
   * @param exporter exporter for exporting XAResource and Synchronization
   */
  public ClientTransactionListener(RemoteTransactionListener remote, Transaction txn) {
    this.remote   = remote;
    this.txn      = txn;
  }

  /**
   * Gets the connection stub from the server.
   *
   * @return the remote connection stub
   *
   * @throws IOException on an error in getting the connection
   * @throws RuntimeException runtime errors reported locally as well as from remote
   */
  @SuppressWarnings("unchecked")
  public RemoteConnection getConnection() throws IOException {
    while (con == null) {
      Operation<?> op;

      try {
        op = remote.getNextOperation();
      } catch (InterruptedException e) {
        throw new IOException("Interrupted while waiting for next operation");
      }

      if (!(op instanceof Result)) {
        try {
          remote.postResult(processRequest(op));
        } catch (InterruptedException e) {
          throw new IOException("Interrupted while waiting for posted result to be picked up");
        }
      } else {
        try {
          con = ((Result<RemoteConnection>) op).get();

          if (log.isDebugEnabled())
            log.debug("Received a connection stub from remote");
        } catch (ExecutionException e) {
          Throwable t = e.getCause();

          if (log.isDebugEnabled())
            log.debug("Received an error in opencCnnection() from emote", t);

          if (t instanceof IOException)
            throw (IOException) t;

          if (t instanceof RuntimeException)
            throw (RuntimeException) t;

          throw new RuntimeException("Unexpected exception", t);
        }
      }
    }

    return con;
  }

  @SuppressWarnings("unchecked")
  private Result<?> processRequest(Operation<?> op) {
    try {
      Result<?> result;

      if (op instanceof EnlistXAResource)
        result = enlistResource((EnlistXAResource) op);
      else if (op instanceof DelistXAResource)
        result = delistResource((DelistXAResource) op);
      else if (op instanceof RegisterSynchronization)
        result = registerSynchronization((RegisterSynchronization) op);
      else if (op instanceof GetStatus)
        result = getStatus();
      else if (op instanceof Rollback)
        result = rollback();
      else
        throw new RuntimeException("Unrecognized operation: " + op.getClass());

      if (log.isDebugEnabled())
        log.debug("Posting response '" + result.get() + "'");

      return result;
    } catch (Throwable t) {
      if (log.isDebugEnabled())
        log.debug("Posting an error response", t);

      return new Result(t);
    }
  }

  private Result<Boolean> delistResource(DelistXAResource op)
                                  throws IllegalStateException, SystemException {
    if (log.isDebugEnabled())
      log.debug("Processing delistResource ...");

    XAResource xa = map.get(op.getXAResource());

    if (xa == null)
      return new Result<Boolean>(false);

    boolean ret = txn.delistResource(xa, op.getFlags());

    if (ret)
      map.remove(op.getXAResource());

    return new Result<Boolean>(ret);
  }

  private Result<Boolean> enlistResource(EnlistXAResource op)
                                  throws RollbackException, IllegalStateException, SystemException {
    if (log.isDebugEnabled())
      log.debug("Processing enlistResource ...");

    RemoteXAResource rxa = op.getXAResource();
    XAResource       xa  = map.get(rxa);

    if (xa != null)
      return new Result<Boolean>(false);

    boolean ret = txn.enlistResource(xa = new ClientXAResource(rxa, this));

    if (ret)
      map.put(rxa, xa);

    return new Result<Boolean>(ret);
  }

  private Result<Integer> getStatus() throws SystemException {
    return new Result<Integer>(txn.getStatus());
  }

  private Result<Void> registerSynchronization(RegisterSynchronization sync)
                                        throws RollbackException, IllegalStateException,
                                               SystemException {
    if (log.isDebugEnabled())
      log.debug("Processing registerSynchronization ...");

    txn.registerSynchronization(new ClientSynchronization(sync.getSynchronization()));

    return new Result<Void>();
  }

  private Result<Void> rollback() throws IllegalStateException, SystemException {
    if (log.isDebugEnabled())
      log.debug("Processing rollback ...");

    txn.rollback();

    return new Result<Void>();
  }

  /**
   * Gets the enlisted remote stub for the given XAResource.
   *
   * @param xa the XAResource
   *
   * @return the remote stub
   */
  public RemoteXAResource getRemoteXAResource(XAResource xa) {
    if (xa == null)
      return null;

    for (Map.Entry<RemoteXAResource, XAResource> e : map.entrySet())
      if (xa.equals(e.getValue()))
        return e.getKey();

    return null;
  }
}
