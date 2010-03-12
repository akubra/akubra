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
package org.akubraproject.rmi.server;

import java.rmi.NoSuchObjectException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.server.RMIClientSocketFactory;
import java.rmi.server.RMIServerSocketFactory;
import java.rmi.server.UnicastRemoteObject;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RMI object exporter. Encapsulates the configuration needed to export an RMI object. It also
 * holds a reference to the exported objects so that exported objects stays alive until
 * un-exported.
 *
 * @author Pradeep Krishnan
 */
public class Exporter {
  private static final Logger  log              = LoggerFactory.getLogger(Exporter.class);

  /**
   * The number of milliseconds to wait before re-attempting an unexport. Note that
   * there is a bit of a back-off mechanism - each subsequent retry will use a multiple
   * of this delay.
   */
  public static final long RETRY_DELAY = 10;
  private final int                          port;
  private final RMIClientSocketFactory       csf;
  private final RMIServerSocketFactory       ssf;
  private final ScheduledExecutorService     executor;
  private final Set<Exportable>              exportedObjects;

  /**
   * Creates a new Exporter object.
   *
   * @param port the port number on which the remote object receives calls (if port is zero, an
   *          anonymous port is chosen)
   */
  public Exporter(int port) {
    this(port, null, null);
  }

  /**
   * Creates a new Exporter object.
   *
   * @param port the port number on which the remote object receives calls (if port is zero, an
   *          anonymous port is chosen)
   * @param csf the client-side socket factory for making calls to the remote object
   * @param ssf the server-side socket factory for receiving remote calls
   */
  public Exporter(int port, RMIClientSocketFactory csf, RMIServerSocketFactory ssf) {
    this.port         = port;
    this.csf          = csf;
    this.ssf          = ssf;
    this.executor     = createExecutor();
    exportedObjects   = Collections.synchronizedSet(new HashSet<Exportable>());
  }

  private static ScheduledExecutorService createExecutor() {
    return Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
          public Thread newThread(Runnable r) {
            Thread t = new Thread(r, "akubra-rmi-unexporter");
            t.setPriority(Thread.MIN_PRIORITY);
            t.setDaemon(true);

            return t;
          }
        });
  }

  /**
   * Gets the port where the exported objects listen.
   *
   * @return the port
   */
  public int getPort() {
    return port;
  }

  /**
   * Exports the remote object to make it available to receive incoming calls using this
   * configuration. Note that this exporter holds a reference to the exported object (not the
   * stub, but the Exportable itself) so that the exported object remains in scope and does not
   * get garbage collected. (This is something that the RMI spec claims to provide, but in reality
   * does not. The sun.rmi.transport.ObjectTable only keeps weak references.) So this means, all
   * exported objects using this exporter must be {@link #unexportObject un-exported} using this
   * so that the reference held here is cleared. Failure to do so will result in memory leaks.
   *
   * @param object the remote object to be exported
   *
   * @return remote object stub
   *
   * @throws RemoteException if export fails
   */
  public Remote exportObject(Exportable object) throws RemoteException {
    Remote stub =
      (csf == null) ? UnicastRemoteObject.exportObject(object, port)
      : UnicastRemoteObject.exportObject(object, port, csf, ssf);
    exportedObjects.add(object);

    return stub;
  }

  /**
   * Removes the remote object from the RMI runtime. If successful, the object can no longer
   * accept incoming RMI calls. If the force parameter is true, the object is forcibly unexported
   * even if there are pending calls to the remote object or the remote object still has calls in
   * progress. If the force parameter is false, the object is only unexported when there are no
   * pending or in progress calls to the object. If there are pending calls, the unexport is
   * re-scheduled for later.
   *
   * @param object the remote object to be unexported
   * @param force if true, unexports the object even if there are pending or in-progress calls; if
   *        false, only unexports the object when there are no pending or in-progress calls
   *
   * @throws NoSuchObjectException if the object is not exported
   */
  public void unexportObject(Exportable object, boolean force)
                      throws NoSuchObjectException {
    exportedObjects.remove(object);
    unexportObject(object, force, 0);
  }

  private void unexportObject(Remote object, boolean force, int trial)
                       throws NoSuchObjectException {
    if (!UnicastRemoteObject.unexportObject(object, force))
      scheduleRetry(object, force, trial);
  }

  private void scheduleRetry(final Remote object, final boolean force, final int trial) {
    if (log.isDebugEnabled())
      log.debug("Scheduling retry #" + trial + " of unexport for " + object);

    Runnable job =
      new Runnable() {
        public void run() {
          try {
            if (log.isDebugEnabled())
              log.debug("Retrying unexport for " + object);

            unexportObject(object, force, trial + 1);
          } catch (NoSuchObjectException e) {
            if (log.isDebugEnabled())
              log.debug(object + " is already unexported", e);
          }
        }
      };

    executor.schedule(job, RETRY_DELAY * trial, TimeUnit.MILLISECONDS);
  }

  /**
   * Gets the set of all exported objects.
   *
   * @return a copy of the exported objects.
   */
  public Set<Exportable> getExportedObjects() {
    synchronized (exportedObjects) {
      return new HashSet<Exportable>(exportedObjects);
    }
  }
}
