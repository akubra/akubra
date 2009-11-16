/* $HeadURL$
 * $Id$
 *
 * Copyright (c) 2009 DuraSpace
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

import java.lang.ref.WeakReference;

import java.rmi.NoSuchObjectException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.server.RemoteServer;
import java.rmi.server.ServerNotActiveException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A base class for all objects that are exported.
 *
 * @author Pradeep Krishnan
 */
public abstract class Exportable extends RemoteServer {
  private static final Log log = LogFactory.getLog(Exportable.class);
  private static final long serialVersionUID = 1L;
  private final Exporter        exporter;
  private WeakReference<Remote> exported;

  /**
   * Creates a new Exportable object.
   *
   * @param exporter the exporter to use
   *
   * @throws RemoteException on an export error
   */
  protected Exportable(Exporter exporter) throws RemoteException {
    this.exporter   = exporter;
    exported        = new WeakReference<Remote>(exporter.exportObject(this));

    if (log.isDebugEnabled()) {
      String client = getClient();

      if (client != null)
        log.debug("Exported " + this + " for " + client);
      else
        log.debug("Exported " + this + " for all clients.");
    }
  }

  /**
   * Unexport this object.
   *
   * @param force if true, will abort all current in progress calls
   */
  public void unExport(boolean force) {
    if (getExported() == null)
      return;

    if (log.isDebugEnabled()) {
      String client = getClient();

      if (client != null)
        log.debug("Unexporting " + this + " used by " + client);
      else
        log.debug("Unexporting " + this);
    }

    try {
      getExporter().unexportObject(this, force);
    } catch (NoSuchObjectException e) {
      if (log.isDebugEnabled())
        log.debug(this + " was already unexported (or was not exported)", e);
    }

    exported = null;
  }

  /**
   * Gets the exporter used by this.
   *
   * @return the exporter
   */
  public Exporter getExporter() {
    return exporter;
  }

  /**
   * Gets the exported stub.
   *
   * @return the exported stub
   */
  public Remote getExported() {
    return (exported == null) ? null : exported.get();
  }

  @Override
  public String toString() {
    return getClass().getSimpleName();
  }

  /**
   * Gets the client host from this call context or null if called outside any call context.
   *
   * @return the client host or null
   */
  protected static String getClient() {
    try {
      return getClientHost();
    } catch (ServerNotActiveException e) {
      return null;
    }
  }
}
