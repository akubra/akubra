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
package org.akubraproject.rmi.server;

import java.rmi.RemoteException;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.RollbackException;
import javax.transaction.Synchronization;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.xa.XAResource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.akubraproject.BlobStore;
import org.akubraproject.BlobStoreConnection;
import org.akubraproject.rmi.remote.RemoteConnection;
import org.akubraproject.rmi.remote.RemoteSynchronization;
import org.akubraproject.rmi.remote.RemoteTransactionListener;
import org.akubraproject.rmi.remote.RemoteXAResource;

/**
 * A transaction wrapper for use in akubra-rmi-server side that forwards calls to the
 * akubra-rmi-client side Transaction.
 *
 * @author Pradeep Krishnan
 */
public class ServerTransactionListener extends UnicastExportable
                                       implements Transaction, RemoteTransactionListener {
  private static final Log  log              = LogFactory.getLog(ServerTransactionListener.class);
  private static final long serialVersionUID = 1L;

  // For call forwarding
  private final SynchronousQueue<Operation<?>> operations    = new SynchronousQueue<Operation<?>>();
  private final SynchronousQueue<Result<?>>    results       = new SynchronousQueue<Result<?>>();

  // For isSameRM support
  private final Map<XAResource, ServerXAResource> xas = new HashMap<XAResource, ServerXAResource>();

  /**
   * Creates a new ServerTransactionListener
   *
   * @param store the store to open a connection to
   * @param hints A set of hints for openConnection
   * @param exporter exporter for exporting XAResource and Synchronization
   * @throws RemoteException on an error in export
   */
  public ServerTransactionListener(final BlobStore store, final Map<String, String> hints,
                                   Exporter exporter) throws RemoteException {
    super(exporter);
    Executors.newSingleThreadExecutor(new ThreadFactory() {
        public Thread newThread(Runnable r) {
          Thread t = new Thread(r, "akubra-rmi-open-connection");
          t.setDaemon(true);

          return t;
        }
      }).submit(new Runnable() {
        public void run() {
          try {
            openConnection(store, hints);
          } catch (Throwable t) {
            log.warn("Uncaught exception in open-connection", t);
            unExport(false);
          }
        }
      });
  }

  private void openConnection(BlobStore store, Map<String, String> hints) {
    Result<RemoteConnection> result;

    ServerConnection         con = null;

    try {
      BlobStoreConnection res = store.openConnection(this, hints);
      con      = new ServerConnection(res, getExporter());
      result   = new Result<RemoteConnection>(con);
    } catch (Throwable t) {
      if (log.isDebugEnabled())
        log.debug("openConnection failed. Sending an error result ...", t);

      result = new Result<RemoteConnection>(t);
    }

    try {
      operations.put(result);
    } catch (Throwable t) {
      log.warn("Failed to send results of openConnection back to client", t);

      try {
        if (con != null)
          con.close();
      } catch (Throwable e) {
        log.warn("Failed to close", e);
      }

    } finally {
      /*
       * Note with posting of the openConnection results we are done and
       * so this object can be unExported. Because of the SynchronousQueue
       * usage we know that the client side has received the results. But
       * in case the put(result) failed, the unExport will cause the client
       * to abort.
       */
      unExport(false);
    }
  }

  public Operation<?> getNextOperation() throws InterruptedException, RemoteException {
    if (log.isDebugEnabled())
      log.debug("getting next operation ....");

    return operations.take();
  }

  public void postResult(Result<?> result) throws InterruptedException, RemoteException {
    if (log.isDebugEnabled())
      log.debug("posting a result ....");

    results.put(result);

    if (log.isDebugEnabled())
      log.debug("finished posting a result ....");
  }

  @SuppressWarnings("unchecked")
  private <T> T executeOnClient(Operation<T> operation)
                        throws ExecutionException, SystemException {
    if (getExported() == null)
      throw new IllegalStateException("No longer referenced by any clients.");

    try {
      if (log.isDebugEnabled())
        log.debug("posting an operation ....");

      operations.put(operation);

      if (log.isDebugEnabled())
        log.debug("finished posting an operation ....");
    } catch (InterruptedException e) {
      if (log.isDebugEnabled())
        log.debug("interrupted while posting an operation ....", e);

      throw (SystemException) new SystemException("interrupted while posting an operation")
        .initCause(e);
    }

    if (getExported() == null)
      throw new IllegalStateException("No longer referenced by any clients.");

    Result<?> result;

    try {
      if (log.isDebugEnabled())
        log.debug("waiting for result...");

      result = results.take();

      if (log.isDebugEnabled())
        log.debug("got a result");
    } catch (InterruptedException e) {
      if (log.isDebugEnabled())
        log.debug("interrupted while waiting for a result ....", e);

      throw (SystemException) new SystemException("interrupted while waiting for a result")
        .initCause(e);
    }

    if (getExported() == null)
      throw new IllegalStateException("No longer referenced by any clients.");

    return (T) result.get();
  }

  public void commit()
              throws RollbackException, HeuristicMixedException, HeuristicRollbackException,
                     SecurityException, IllegalStateException, SystemException {
    throw new SecurityException("Cannot commit from this JVM. Transaction is on remote client.");
  }

  public boolean delistResource(XAResource xaRes, int flags)
                         throws IllegalStateException, SystemException {
    ServerXAResource xa = xas.get(xaRes);

    if (xa == null)
      return false;

    boolean ret;

    try {
      ret = executeOnClient(new DelistXAResource(xa, flags));
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();

      if (cause instanceof SystemException)
        throw (SystemException) cause;

      if (cause instanceof RuntimeException)
        throw (RuntimeException) cause;

      throw new RuntimeException("Error reported by server", cause);
    }

    if (ret)
      xas.remove(xaRes);

    return ret;
  }

  public boolean enlistResource(XAResource xaRes)
                         throws RollbackException, IllegalStateException, SystemException {
    if (xaRes == null)
      return false;

    ServerXAResource xa;

    try {
      xa = new ServerXAResource(xaRes, null, getExporter());
    } catch (RemoteException e) {
      throw (SystemException) new SystemException("Failed to export XAResource").initCause(e);
    }

    boolean ret;

    try {
      ret = executeOnClient(new EnlistXAResource(xa));
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();

      if (cause instanceof RollbackException)
        throw (RollbackException) cause;

      if (cause instanceof SystemException)
        throw (SystemException) cause;

      if (cause instanceof RuntimeException)
        throw (RuntimeException) cause;

      throw new RuntimeException("Error reported by server", cause);
    }

    if (ret)
      xas.put(xaRes, xa);

    return ret;
  }

  public int getStatus() throws SystemException {
    try {
      return executeOnClient(new GetStatus());
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();

      if (cause instanceof SystemException)
        throw (SystemException) cause;

      if (cause instanceof RuntimeException)
        throw (RuntimeException) cause;

      throw new RuntimeException("Error reported by server", cause);
    }
  }

  public void registerSynchronization(Synchronization sync)
                               throws RollbackException, IllegalStateException, SystemException {
    RemoteSynchronization rsync;

    try {
      rsync = new ServerSynchronization(sync, getExporter());
    } catch (RemoteException e) {
      throw (SystemException) new SystemException("Failed to export Synchronization").initCause(e);
    }

    try {
      executeOnClient(new RegisterSynchronization(rsync));
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();

      if (cause instanceof RollbackException)
        throw (RollbackException) cause;

      if (cause instanceof SystemException)
        throw (SystemException) cause;

      if (cause instanceof RuntimeException)
        throw (RuntimeException) cause;

      throw new RuntimeException("Error reported by server", cause);
    }
  }

  public void rollback() throws IllegalStateException, SystemException {
    try {
      executeOnClient(new Rollback());
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();

      if (cause instanceof SystemException)
        throw (SystemException) cause;

      if (cause instanceof RuntimeException)
        throw (RuntimeException) cause;

      throw new RuntimeException("Error reported by server", cause);
    }
  }

  public void setRollbackOnly() throws IllegalStateException, SystemException {
    throw new IllegalStateException("Cannot change the transaction state from this JVM. "
                                    + "Transaction is on remote client.");
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

    for (Map.Entry<XAResource, ServerXAResource> e : xas.entrySet())
      if (res.equals(e.getValue()) || res.equals(e.getValue().getExported()))
        return e.getKey();

    return null;
  }
}
