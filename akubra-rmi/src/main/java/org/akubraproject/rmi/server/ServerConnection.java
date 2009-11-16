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
package org.akubraproject.rmi.server;

import java.io.IOException;

import java.net.URI;

import java.rmi.RemoteException;

import java.util.Map;

import org.akubraproject.BlobStoreConnection;
import org.akubraproject.rmi.remote.RemoteBlob;
import org.akubraproject.rmi.remote.RemoteBlobCreator;
import org.akubraproject.rmi.remote.RemoteConnection;
import org.akubraproject.rmi.remote.RemoteIterator;

/**
 * Server side impl of RemoteConnection. Note that this implements Unreferenced
 * and therefore if the client goes away prematurely, the connection can be closed.
 *
 * @author Pradeep Krishnan
 */
public class ServerConnection extends UnicastExportable implements RemoteConnection {
  private static final long         serialVersionUID = 1L;
  private final BlobStoreConnection con;

  /**
   * Creates a new ServerConnection object.
   *
   * @param con the real connection object
   * @param exporter the exporter to use
   *
   * @throws RemoteException on an error in export
   */
  public ServerConnection(BlobStoreConnection con, Exporter exporter)
                   throws RemoteException {
    super(exporter);
    this.con = con;
  }

  public RemoteBlob getBlob(URI id, Map<String, String> hints)
                     throws IOException {
    return new ServerBlob(con.getBlob(id, hints), getExporter());
  }

  public RemoteBlobCreator getBlobCreator(long estimatedSize, Map<String, String> hints)
                     throws IOException {
    return new ServerBlobCreator(con, estimatedSize, hints, getExporter());
  }

  public RemoteIterator<URI> listBlobIds(String filterPrefix)
                                  throws IOException {
    return new ServerIterator<URI>(con.listBlobIds(filterPrefix), getExporter());
  }

  //@Override
  public void sync() throws IOException {
    con.sync();
  }

  public void close() {
    unExport(false);
    con.close();
  }

  @Override
  public void unreferenced() {
    close();
  }

  // for tests
  BlobStoreConnection getConnection() {
    return con;
  }
}
