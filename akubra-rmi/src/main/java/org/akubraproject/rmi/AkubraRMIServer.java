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
package org.akubraproject.rmi;

import java.rmi.AccessException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.RMIClientSocketFactory;
import java.rmi.server.RMIServerSocketFactory;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.akubraproject.BlobStore;
import org.akubraproject.rmi.server.Exportable;
import org.akubraproject.rmi.server.Exporter;
import org.akubraproject.rmi.server.ServerConnection;
import org.akubraproject.rmi.server.ServerStore;

/**
 * Akubra RMI server.
 *
 * @author Pradeep Krishnan
 */
public class AkubraRMIServer {
  /**
   * A default name for akubra-rmi-servers. Used for RMI registration when none supplied.
   */
  public static final String DEFAULT_SERVER_NAME = "akubra-rmi";
  private static final Log log = LogFactory.getLog(AkubraRMIServer.class);
  private static final long serialVersionUID = 1L;

  private final Registry    registry;
  private final String      name;
  private final ServerStore store;

  /**
   * Exports the given Blob Store for remote access.
   *
   * @param store the store to export.
   *
   * @throws AccessException when the caller does not have permission to export
   * @throws RemoteException on any other error in export
   */
  public AkubraRMIServer(BlobStore store) throws AccessException, RemoteException {
    this(store, DEFAULT_SERVER_NAME, 0);
  }

  /**
   * Creates an AkubraRMIServer that exports the given Blob Store for remote access.
   *
   * @param store the store to export.
   * @param port the RMI registry port
   *
   * @throws AccessException when the caller does not have permission to export
   * @throws RemoteException on any other error in export
   */
  public AkubraRMIServer(BlobStore store, int port)
                     throws AccessException, RemoteException {
    this(store, DEFAULT_SERVER_NAME, port);
  }

  /**
   * Creates an AkubraRMIServer that exports the given Blob Store for remote access.
   *
   * @param store the store to export.
   * @param name the name to register with RMI registry
   *
   * @throws AccessException when the caller does not have permission to export
   * @throws RemoteException on any other error in export
   */
  public AkubraRMIServer(BlobStore store, String name)
                     throws AccessException, RemoteException {
    this(store, name, 0);
  }

  /**
   * Creates an AkubraRMIServer that exports the given Blob Store for remote access.
   *
   * @param store the store to export.
   * @param name the name to register with RMI registry
   * @param port the RMI registry port
   *
   * @throws AccessException when the caller does not have permission to export
   * @throws RemoteException on any other error in export
   */
  public AkubraRMIServer(BlobStore store, String name, int port)
                     throws AccessException, RemoteException {
    this(store, name, port, port);
  }

  /**
   * Creates an AkubraRMIServer that exports the given Blob Store for remote access.
   *
   * @param store the store to export.
   * @param name the name to register with RMI registry
   * @param registryPort the RMI registry port
   * @param port the port where the BlobStore is exported at
   *
   * @throws AccessException when the caller does not have permission to export
   * @throws RemoteException on any other error in export
   */
  public AkubraRMIServer(BlobStore store, String name, int registryPort, int port)
                     throws AccessException, RemoteException {
    this(store, name, ensureRegistry(registryPort), registryPort, port);
  }

  /**
   * Creates an AkubraRMIServer that exports the given Blob Store for remote access.
   *
   * @param store the store to export.
   * @param name the name to register with RMI registry
   * @param registry the RMI registry
   * @param registryPort the RMI registry port
   * @param port the port where the BlobStore is exported at
   *
   * @throws AccessException when the caller does not have permission to export
   * @throws RemoteException on any other error in export
   */
  public AkubraRMIServer(BlobStore store, String name, Registry registry, int registryPort,
                         int port) throws AccessException, RemoteException {
    this(store, name, registry, registryPort, new Exporter(port));
  }

  /**
   * Creates an AkubraRMIServer that exports the given Blob Store for remote access.
   *
   * @param store the store to export.
   * @param name the name to register with RMI registry
   * @param registry the RMI registry
   * @param registryPort the RMI registry port
   * @param exporter the exporter for the BlobStore
   *
   * @throws AccessException when the caller does not have permission to export
   * @throws RemoteException on any other error in export
   */
  public AkubraRMIServer(BlobStore store, String name, Registry registry, int registryPort,
                         Exporter exporter) throws AccessException, RemoteException {
    log.info("Starting server '" + name + "'  ...");
    this.store = new ServerStore(store, exporter);
    this.registry = registry;
    this.name     = name;
    registry.rebind(name, this.store);
    log.info("Server '" + name + "' is bound to registry on port " + registryPort);
  }

  /**
   * Shutdown this server.
   *
   * @param abort if true, will abort all current in progress calls and close all connections.
   *
   * @throws AccessException when the caller does not have permission to shutdown
   * @throws RemoteException on any other error in registry unbind
   */
  public void shutDown(boolean abort) throws AccessException, RemoteException {
    if (abort)
      log.info("Starting shutdown(abort) of server '" + name + "'");
    else
      log.info("Starting shutdown of server '" + name + "'");

    try {
      log.info("Ubinding from registry ...");
      registry.unbind(name);
    } catch (NotBoundException e) {
      log.info("Server '" + name + "' was already unbound.", e);
    }

    log.info("Unexporting server instance...");
    store.unExport(true);

    Set<ServerConnection> cons = new HashSet<ServerConnection>();
    do {
      int others = 0;
      cons.clear();
      for (Exportable exported : store.getExporter().getExportedObjects()) {
        if (exported instanceof ServerConnection)
          cons.add((ServerConnection) exported);
        else
          others++;
      }

      if (cons.isEmpty() && (others == 0)) {
        log.info("No connections are open and no exported objects. Shutdown is complete.");
        return;
      }

      log.info("There are " + cons.size() + " open connections and " + others + " exported objects.");

      if (!cons.isEmpty() && !abort) {
        log.info("Shutdown completed. Existing connections will continue till closed.");
        return;
      }

      if (!cons.isEmpty()) {
        log.info("Shutting down connections ...");

        for (ServerConnection con : cons) {
          try {
            con.unExport(true);
            con.close();
          } catch (Exception e) {
            log.info("Ignoring failure in connection close for " + con, e);
          }
        }
      }
    } while (!cons.isEmpty());

    log.info("Unexporting all exported objects ...");
    for (Exportable exported : store.getExporter().getExportedObjects()) {
      try {
        exported.unExport(true);
      } catch (Exception e) {
        log.info("Ignoring failure in unexport for " + exported, e);
      }
    }

    if (abort)
      log.info("Shutdown(abort) completed.");
    else
      log.info("Shutdown completed.");
  }

  /**
   * Ensures that an RMI registry is running at the given port on localhost.
   *
   * @param port the RMI registry port
   *
   * @return the client side stub to RMI Rgistry
   *
   * @throws RemoteException on an error in starting up the registry
   */
  public static Registry ensureRegistry(int port) throws RemoteException {
    return ensureRegistry("localhost", port, null, null);
  }

  /**
   * Ensures that an RMI registry is running at the given port on the given server.
   *
   * @param host the RMI registry host
   * @param port the RMI registry port
   * @param csf the client-side socket factory for making calls to the remote object
   * @param ssf the server-side socket factory for receiving remote calls
   *
   * @return the client side stub to RMI Rgistry
   *
   * @throws RemoteException on an error in starting up the registry
   */
  public static Registry ensureRegistry(String host, int port, RMIClientSocketFactory csf,
                                        RMIServerSocketFactory ssf)
                                 throws RemoteException {
    if (port <= 0)
      port = Registry.REGISTRY_PORT;

    Registry reg = LocateRegistry.getRegistry(host, port, csf);

    if (exists(reg))
      log.info("Located an RMI registry at '" + host + ":" + port + "'");
    else {
      log.info("Starting RMI registry on 'localhost:" + port + "'");
      reg = LocateRegistry.createRegistry(port, csf, ssf);
    }

    return reg;
  }

  private static boolean exists(Registry reg) {
    try {
      reg.lookup("akubra-server");
    } catch (NotBoundException e) {
      log.debug("'akubra-server' not bound", e);
    } catch (RemoteException e) {
      log.debug("failed to communicate with registry - assuming it's not running", e);
      reg = null;
    }

    return (reg != null);
  }

  @Override
  protected void finalize() throws Throwable {
    if (store.getExported() != null) {
      log.info("Shutting down due to finalize()");
      try {
        shutDown(true);
      } catch (Exception e) {
        log.info("Shutdown failed", e);
      }
    }
    super.finalize();
  }
}
