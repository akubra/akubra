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
package org.fedoracommons.akubra.rmi.server;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import java.rmi.NoSuchObjectException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.server.RMIClientSocketFactory;
import java.rmi.server.RMIServerSocketFactory;
import java.rmi.server.UnicastRemoteObject;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * RMI object exporter. Encapsulates the configuration needed to export an RMI object.
 *
 * @author Pradeep Krishnan
 */
public class Exporter implements Serializable {
  private static final long serialVersionUID = 1L;
  private static final Log  log              = LogFactory.getLog(Exporter.class);

  /**
   * The number of milliseconds to wait before re-attempting an unexport. Note that
   * there is a bit of a back-off mechanism - each subsequent retry will use a multiple
   * of this delay.
   */
  public static final int RETRY_DELAY = 10;
  private final int                                port;
  private final RMIClientSocketFactory             csf;
  private final RMIServerSocketFactory             ssf;
  private transient ScheduledExecutorService       executor;

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
    this.port       = port;
    this.csf        = csf;
    this.ssf        = ssf;
    this.executor   = createExecutor();
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
   * configuration.
   *
   * @param object the remote object to be exported
   *
   * @return remote object stub
   *
   * @throws RemoteException if export fails
   */
  public Remote exportObject(Remote object) throws RemoteException {
    return (csf == null) ? UnicastRemoteObject.exportObject(object, port)
           : UnicastRemoteObject.exportObject(object, port, csf, ssf);
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
  public void unexportObject(Remote object, boolean force)
                      throws NoSuchObjectException {
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

  private void writeObject(ObjectOutputStream out) throws IOException {
    out.defaultWriteObject();
  }

  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    in.defaultReadObject();
    executor = createExecutor();
  }
}
