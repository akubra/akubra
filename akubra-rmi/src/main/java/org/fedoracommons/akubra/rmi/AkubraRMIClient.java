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

import java.io.IOException;

import java.net.URI;
import java.net.URISyntaxException;

import java.rmi.NotBoundException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

import org.fedoracommons.akubra.BlobStore;
import org.fedoracommons.akubra.rmi.client.ClientStore;
import org.fedoracommons.akubra.rmi.remote.RemoteStore;
import org.fedoracommons.akubra.rmi.server.Exporter;

/**
 * A Utility class that helps applications to locate an Akubra RMI Server and create a
 * BlobStore that forwards all operations to that server.
 *
 * @author Pradeep Krishnan
 */
public class AkubraRMIClient {
  /**
   * Disabled constructor. Use the static methods.
   */
  private AkubraRMIClient() {
  }

  /**
   * Creates a client to a default akubra-rmi-server running on localhost. The default RMI
   * registry running on localhost is used for the lookup.
   *
   * @return an RMI client BlobStore with a generated id
   *
   * @throws IOException on an error in contacting the server
   * @throws NotBoundException when a server with {@link AkubraRMIServer#DEFAULT_SERVER_NAME}
   *         is not bound
   */
  public static BlobStore create() throws IOException, NotBoundException {
    return create(-1);
  }

  /**
   * Creates a client to a default akubra-rmi-server running on localhost. An RMI registry
   * running on localhost at the given port is used for the lookup.
   *
   * @param registryPort the port on local host where RMI registry is listening
   *
   * @return an RMI client BlobStore with a generated id
   *
   * @throws IOException on an error in contacting the server
   * @throws NotBoundException when a server with {@link AkubraRMIServer#DEFAULT_SERVER_NAME}
   *         is not bound
   */
  public static BlobStore create(int registryPort) throws IOException, NotBoundException {
    try {
      return create(AkubraRMIServer.DEFAULT_SERVER_NAME, registryPort);
    } catch (URISyntaxException e) {
      throw new Error("Default Server Name is invalid", e);
    }
  }

  /**
   * Creates a client to the given akubra-rmi-server running on localhost. The default RMI
   * registry running on localhost is used for the lookup.
   *
   * @param serverName the registered name of the akubra-rmi-server at the RMI registry
   *
   * @return an RMI client BlobStore with a generated id
   *
   * @throws IOException on an error in contacting the server
   * @throws NotBoundException when a server with the given serverName is not bound
   * @throws URISyntaxException when a valid URI can not be formed with the given serverName
   */
  public static BlobStore create(String serverName)
                          throws IOException, NotBoundException, URISyntaxException {
    return create(serverName, -1);
  }

  /**
   * Creates a client to the given akubra-rmi-server running on localhost. An RMI registry
   * running on localhost at the given port is used for the lookup.
   *
   * @param serverName the registered name of the akubra-rmi-server at the RMI registry
   * @param registryPort the port on local host where RMI registry is listening. A value of -1 or 0
   *        denotes the default RMI registy port.
   *
   * @return an RMI client BlobStore with a generated id
   *
   * @throws IOException on an error in contacting the server
   * @throws NotBoundException when a server with the given serverName is not bound
   * @throws URISyntaxException when a valid URI can not be formed with the given serverName
   */
  public static BlobStore create(String serverName, int registryPort)
                          throws IOException, NotBoundException, URISyntaxException {
    return create(serverName, "localhost", registryPort);
  }

  /**
   * Creates a client to the given akubra-rmi-server registered at the given RMI registry.
   *
   * @param serverName the registered name of the akubra-rmi-server at the RMI registry
   * @param registryHost the host where the RMI registry is running.
   * @param registryPort the port on local host where RMI registry is listening. A value of -1 or 0
   *        denotes the default RMI registy port.
   *
   * @return an RMI client BlobStore with a generated id
   *
   * @throws IOException on an error in contacting the server
   * @throws NotBoundException when a server with the given serverName is not bound
   * @throws URISyntaxException when a valid URI can not be formed with the given serverName and
   *         registryHost
   */
  public static BlobStore create(String serverName, String registryHost, int registryPort)
                          throws IOException, NotBoundException, URISyntaxException {
    URI localId;

    if (registryPort <= 0)
      localId = new URI("rmi://" + registryHost + "/" + serverName);
    else
      localId = new URI("rmi://" + registryHost + ":" + registryPort + "/" + serverName);

    return create(locateServer(serverName, registryHost, registryPort), localId);
  }

  /**
   * Creates a client to the given akubra-rmi-server.
   *
   * @param serverUri a URI of the form rmi://registryHost:registryPort/serverName. The
   *        registryPort can be omitted in which case it defaults to the default RMI registry
   *        port. The 'serverName' is the registered name of the akubra-rmi-server at the RMI
   *        registry.
   *
   * @return an RMI client BlobStore with a generated id
   *
   * @throws IOException on an error in contacting the server
   * @throws NotBoundException when a server with the given serverName is not bound
   */
  public static BlobStore create(URI serverUri) throws IOException, NotBoundException {
    return create(serverUri, serverUri);
  }

  /**
   * Creates a client to the given akubra-rmi-server.
   *
   * @param serverUri a URI of the form rmi://registryHost:registryPort/serverName. The
   *        registryPort can be omitted in which case it defaults to the default RMI registry
   *        port. The 'serverName' is the registered name of the akubra-rmi-server at the RMI
   *        registry.
   * @param localId the identifier for the RMI client BlobStore
   *
   * @return an RMI client BlobStore
   *
   * @throws IOException on an error in contacting the server
   * @throws NotBoundException when a server with the given serverName is not bound
   */
  public static BlobStore create(URI serverUri, URI localId)
                          throws IOException, NotBoundException {
    return create(locateServer(serverUri), localId, null);
  }

  /**
   * Creates a client to the given akubra-rmi-server.
   *
   * @param serverUri a URI of the form rmi://registryHost:registryPort/serverName. The
   *        registryPort can be omitted in which case it defaults to the default RMI registry
   *        port. The 'serverName' is the registered name of the akubra-rmi-server at the RMI
   *        registry.
   * @param localId the identifier for the RMI client BlobStore
   * @param callbackPort the port on which server calls back to the client. This is the port used
   *        by the server to make calls to the Transaction object. This is also used for creating
   *        a Blob from an application supplied InputStream.
   *
   * @return an RMI client BlobStore
   *
   * @throws IOException on an error in contacting the server
   * @throws NotBoundException when a server with the given serverName is not bound
   */
  public static BlobStore create(URI serverUri, URI localId, int callbackPort)
                          throws IOException, NotBoundException {
    return create(locateServer(serverUri), localId, new Exporter(callbackPort));
  }

  /**
   * Creates a client to the given akubra-rmi-server.
   *
   * @param server the akubra-rmi-server stub
   * @param localId the identifier for the RMI client BlobStore
   *
   * @return an RMI client BlobStore
   *
   * @throws IOException on an error in contacting the server
   */
  public static BlobStore create(RemoteStore server, URI localId)
                          throws IOException {
    return create(server, localId, null);
  }

  /**
   * Creates a client to the given akubra-rmi-server.
   *
   * @param server the akubra-rmi-server stub
   * @param localId the identifier for the RMI client BlobStore
   * @param callbackPort the port on which server calls back to the client. This is the port used
   *        by the server to make calls to the Transaction object. This is also used for creating
   *        a Blob from an application supplied InputStream.
   *
   * @return an RMI client BlobStore
   *
   * @throws IOException on an error in contacting the server
   */
  public static BlobStore create(RemoteStore server, URI localId, int callbackPort)
                          throws IOException {
    return create(server, localId, new Exporter(callbackPort));
  }

  /**
   * Creates a client to the given akubra-rmi-server.
   *
   * @param server the akubra-rmi-server stub
   * @param localId the identifier for the RMI client BlobStore
   * @param exporter the exporter to use for call-backs from the server.
   *
   * @return an RMI client BlobStore
   *
   * @throws IOException on an error in contacting the server
   */
  public static BlobStore create(RemoteStore server, URI localId, Exporter exporter)
                          throws IOException {
    return new ClientStore(localId, server, exporter);
  }

  /**
   * Locates an akubra-rmi-server using the given uri.
   *
   * @param uri a URI of the form rmi://registryHost:registryPort/serverName. The registryPort can
   *        be omitted in which case it defaults to the default RMI registry port. The
   *        'serverName' is the registered name of the akubra-rmi-server at the RMI registry.
   *
   * @return a client side stub to talk to the remote akubra-rmi-server
   *
   * @throws RemoteException on contacting the RMI registry
   * @throws NotBoundException when a server with the given serverName is not bound
   */
  public static RemoteStore locateServer(URI uri) throws RemoteException, NotBoundException {
    return locateServer(getServerName(uri), uri.getHost(), uri.getPort());
  }

  /**
   * Locates an akubra-rmi-server with the default serverName from the default RMI registry
   * on localhost.
   *
   * @return a client side stub to talk to the akubra-rmi-server
   *
   * @throws RemoteException on contacting the RMI registry
   * @throws NotBoundException when a server with the given serverName is not bound
   */
  public static RemoteStore locateServer() throws RemoteException, NotBoundException {
    return locateServer(AkubraRMIServer.DEFAULT_SERVER_NAME, "localhost", 0);
  }

  /**
   * Locates an akubra-rmi-server with the given serverName from the default RMI registry  on
   * localhost.
   *
   * @param serverName the registered name of the akubra-rmi-server at the RMI registry
   *
   * @return a client side stub to talk to the akubra-rmi-server
   *
   * @throws RemoteException on contacting the RMI registry
   * @throws NotBoundException when a server with the given serverName is not bound
   */
  public static RemoteStore locateServer(String serverName)
                                  throws RemoteException, NotBoundException {
    return locateServer(serverName, "localhost", 0);
  }

  /**
   * Locates an akubra-rmi-server registered at the given RMI registry.
   *
   * @param name the registered name of the akubra-rmi-server at the RMI registry
   * @param registryHost the host where the RMI registry is running.
   * @param registryPort the port on local host where RMI registry is listening. A value of -1 or 0
   *        denotes the default RMI registy port.
   *
   * @return a client side stub to talk to the remote akubra-rmi-server
   *
   * @throws RemoteException on contacting the RMI registry
   * @throws NotBoundException when a server with the given serverName is not bound
   */
  public static RemoteStore locateServer(String name, String registryHost, int registryPort)
                                  throws RemoteException, NotBoundException {
    return locateServer(name, LocateRegistry.getRegistry(registryHost, registryPort));
  }

  /**
   * Locates an akubra-rmi-server registered at the given RMI registry.
   *
   * @param name the registered name of the akubra-rmi-server at the RMI registry
   * @param registry a client side stub to talk to the RMI registry.
   *
   * @return a client side stub to talk to the remote akubra-rmi-server
   *
   * @throws RemoteException on contacting the RMI registry
   * @throws NotBoundException when a server with the given serverName is not bound
   */
  public static RemoteStore locateServer(String name, Registry registry)
                                  throws RemoteException, NotBoundException {
    Remote remote = registry.lookup(name);

    if (remote instanceof RemoteStore)
      return (RemoteStore) remote;

    throw new NotBoundException("Server '" + name + "' is not a RemoteStore. It is a "
                                + remote.getClass());
  }

  /**
   * Gets the name of the server registered at this given registry server URI.
   *
   * @param serverUri a URI of the form rmi://registryHost:registryPort/serverName. The
   *        registryPort can be omitted in which case it defaults to the default RMI registry
   *        port.
   *
   * @return the serverName from the registry server URI
   */
  public static String getServerName(URI serverUri) {
    String name = serverUri.getPath();

    if (name.startsWith("/"))
      name = name.substring(1);

    return name;
  }
}
