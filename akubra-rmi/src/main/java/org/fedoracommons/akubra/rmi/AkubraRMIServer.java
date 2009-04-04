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
package org.fedoracommons.akubra.rmi;

import java.rmi.AccessException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.RMIClientSocketFactory;
import java.rmi.server.RMIServerSocketFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.fedoracommons.akubra.BlobStore;
import org.fedoracommons.akubra.rmi.server.Exporter;
import org.fedoracommons.akubra.rmi.server.ServerStore;

/**
 * A utility class that helps applications to export BlobStores for remote access.
 *
 * @author Pradeep Krishnan
 */
public class AkubraRMIServer {
  /**
   * A default name for akubra-rmi-servers. Used for RMI registration when none supplied.
   */
  public static final String DEFAULT_SERVER_NAME = "akubra-rmi";
  private static final Log log = LogFactory.getLog(AkubraRMIServer.class);

  /**
   * Disabled constructor. Use static methods.
   */
  private AkubraRMIServer() {
  }

  /**
   * Exports the given Blob Store for remote access.
   *
   * @param store the store to export.
   *
   * @throws AccessException when the caller does not have permission to export
   * @throws RemoteException on any other error in export
   */
  public static void export(BlobStore store) throws AccessException, RemoteException {
    export(store, DEFAULT_SERVER_NAME, 0);
  }

  /**
   * Exports the given Blob Store for remote access.
   *
   * @param store the store to export.
   * @param port the RMI registry port
   *
   * @throws AccessException when the caller does not have permission to export
   * @throws RemoteException on any other error in export
   */
  public static void export(BlobStore store, int port)
                     throws AccessException, RemoteException {
    export(store, DEFAULT_SERVER_NAME, port);
  }

  /**
   * Exports the given Blob Store for remote access.
   *
   * @param store the store to export.
   * @param name the name to register with RMI registry
   *
   * @throws AccessException when the caller does not have permission to export
   * @throws RemoteException on any other error in export
   */
  public static void export(BlobStore store, String name)
                     throws AccessException, RemoteException {
    export(store, name, 0);
  }

  /**
   * Exports the given Blob Store for remote access.
   *
   * @param store the store to export.
   * @param name the name to register with RMI registry
   * @param port the RMI registry port
   *
   * @throws AccessException when the caller does not have permission to export
   * @throws RemoteException on any other error in export
   */
  public static void export(BlobStore store, String name, int port)
                     throws AccessException, RemoteException {
    export(store, name, port, port);
  }

  /**
   * Exports the given Blob Store for remote access.
   *
   * @param store the store to export.
   * @param name the name to register with RMI registry
   * @param registryPort the RMI registry port
   * @param port the port where the BlobStore is exported at
   *
   * @throws AccessException when the caller does not have permission to export
   * @throws RemoteException on any other error in export
   */
  public static void export(BlobStore store, String name, int registryPort, int port)
                     throws AccessException, RemoteException {
    export(store, name, ensureRegistry(registryPort), port);
  }

  /**
   * Exports the given Blob Store for remote access.
   *
   * @param store the store to export.
   * @param name the name to register with RMI registry
   * @param registry the RMI registry
   * @param port the port where the BlobStore is exported at
   *
   * @throws AccessException when the caller does not have permission to export
   * @throws RemoteException on any other error in export
   */
  public static void export(BlobStore store, String name, Registry registry, int port)
                     throws AccessException, RemoteException {
    export(store, name, registry, new Exporter(port));
  }

  /**
   * Exports the given Blob Store for remote access.
   *
   * @param store the store to export.
   * @param name the name to register with RMI registry
   * @param registry the RMI registry
   * @param exporter the exporter for the BlobStore
   *
   * @throws AccessException when the caller does not have permission to export
   * @throws RemoteException on any other error in export
   */
  public static void export(BlobStore store, String name, Registry registry, Exporter exporter)
                     throws AccessException, RemoteException {
    log.info("Exporting server '" + name + "'  ...");
    registry.rebind(name, new ServerStore(store, exporter));
    log.info("Server '" + name + "' exported.");
  }

  /**
   * Unexport the given Blob Store and make it un-available for remote access.
   *
   * @throws RemoteException on any other errors
   * @throws NotBoundException when the store is already unexported (or was never exported)
   */
  public static void unExport() throws RemoteException, NotBoundException {
    unExport(DEFAULT_SERVER_NAME);
  }

  /**
   * Unexport the given Blob Store and make it un-available for remote access.
   *
   * @param name the name of the akubra-rmi-server to un-register from the RMI registry
   *
   * @throws RemoteException on any other errors
   * @throws NotBoundException when the store is already unexported (or was never exported)
   */
  public static void unExport(String name) throws RemoteException, NotBoundException {
    unExport(name, LocateRegistry.getRegistry());
  }

  /**
   * Unexport the given Blob Store and make it un-available for remote access.
   *
   * @param registryPort the RMI registry port
   *
   * @throws RemoteException on any other errors
   * @throws NotBoundException when the store is already unexported (or was never exported)
   */
  public static void unExport(int registryPort) throws RemoteException, NotBoundException {
    unExport(DEFAULT_SERVER_NAME, registryPort);
  }

  /**
   * Unexport the given Blob Store and make it un-available for remote access.
   *
   * @param name the name of the akubra-rmi-server to un-register from the RMI registry
   * @param registryPort the RMI registry port
   *
   * @throws RemoteException on any other errors
   * @throws NotBoundException when the store is already unexported (or was never exported)
   */
  public static void unExport(String name, int registryPort)
                       throws RemoteException, NotBoundException {
    unExport(name, LocateRegistry.getRegistry(registryPort));
  }

  /**
   * Unexport the given Blob Store and make it un-available for remote access.
   *
   * @param name the name of the akubra-rmi-server to un-register from the RMI registry
   * @param registry the RMI registry
   *
   * @throws RemoteException on any other errors
   * @throws NotBoundException when the store is already unexported (or was never exported)
   */
  public static void unExport(String name, Registry registry)
                       throws RemoteException, NotBoundException {
    log.info("Unexporting server '" + name + "' ...");
    registry.unbind(name);
    log.info("Server '" + name + "' unexported.");
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
    } catch (RemoteException e) {
      reg = null;
    }

    return (reg != null);
  }
}
