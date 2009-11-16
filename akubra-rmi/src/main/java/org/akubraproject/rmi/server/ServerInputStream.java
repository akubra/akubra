/* $HeadURL::                                                                            $
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

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;

import java.rmi.RemoteException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.akubraproject.rmi.remote.PartialBuffer;
import org.akubraproject.rmi.remote.RemoteInputStream;

/**
 * The server side implementation that delegates to an InputStream. Closes the stream when it
 * becomes unreferenced such as when the client aborts.
 *
 * @author Pradeep Krishnan
 */
public class ServerInputStream extends UnicastExportable implements RemoteInputStream, Closeable {
  private static final Log log = LogFactory.getLog(ServerInputStream.class);
  private static final long serialVersionUID = 1L;
  private final InputStream in;

  /**
   * Creates a new ServerInputStream object.
   *
   * @param in the backing input stream
   * @throws RemoteException on an error in exporting
   */
  public ServerInputStream(InputStream in, Exporter exporter) throws RemoteException {
    super(exporter);
    this.in = in;
  }

  public int read() throws IOException {
    return in.read();
  }

  public PartialBuffer read(int len) throws IOException {
    byte[] b   = new byte[len];
    int    ret = in.read(b);

    return (ret < 0) ? null : new PartialBuffer(b, 0, ret);
  }

  public long skip(long n) throws IOException {
    return in.skip(n);
  }

  public void close() throws IOException {
    unExport(false);
    in.close();
  }

  @Override
  public void unreferenced() {
    try {
      close();
    } catch (IOException e) {
      log.warn("Ignoring error in close of this unreferenced InputStream", e);
    }
  }

  InputStream getInputStream() {
    return in;
  }
}
