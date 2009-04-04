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

import java.net.URI;

import java.rmi.RemoteException;
import java.rmi.server.Unreferenced;

import java.util.Map;

import org.fedoracommons.akubra.Blob;
import org.fedoracommons.akubra.BlobStoreConnection;
import org.fedoracommons.akubra.rmi.client.ClientInputStream;
import org.fedoracommons.akubra.rmi.remote.RemoteBlob;
import org.fedoracommons.akubra.rmi.remote.RemoteConnection;
import org.fedoracommons.akubra.rmi.remote.RemoteInputStream;
import org.fedoracommons.akubra.rmi.remote.RemoteIterator;

/**
 * Server side impl of RemoteConnection. Note that this implements Unreferenced
 * and therefore if the client goes away prematurely, the connection can be closed.
 *
 * @author Pradeep Krishnan
 */
public class ServerConnection extends Exportable implements RemoteConnection, Unreferenced {
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

  public RemoteBlob getBlob(RemoteInputStream stream, long estimatedSize, Map<String, String> hints)
                     throws IOException {
    Blob blob = con.getBlob(new ClientInputStream(stream), estimatedSize, hints);

    return new ServerBlob(blob, getExporter());
  }

  public RemoteIterator<URI> listBlobIds(String filterPrefix)
                                  throws IOException {
    return new ServerIterator<URI>(con.listBlobIds(filterPrefix), getExporter());
  }

  public void close() {
    unExport(false);
    con.close();
  }

  public void unreferenced() {
    close();
  }

  // for tests
  BlobStoreConnection getConnection() {
    return con;
  }
}
