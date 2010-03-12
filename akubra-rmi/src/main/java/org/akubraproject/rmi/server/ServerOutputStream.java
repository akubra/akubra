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

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;

import java.rmi.RemoteException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.akubraproject.rmi.remote.PartialBuffer;
import org.akubraproject.rmi.remote.RemoteOutputStream;

/**
 * Server side implementation of a RemoteOutputStream.
 *
 * @author Pradeep Krishnan
 */
public class ServerOutputStream extends UnicastExportable implements RemoteOutputStream, Closeable {
  private static final Logger   log              = LoggerFactory.getLogger(ServerOutputStream.class);
  private static final long  serialVersionUID = 1L;
  protected final OutputStream out;

  /**
   * Creates a new ServerOutputStream object.
   *
   * @param out the output stream on the server to write to
   * @param exporter the exporter to use
   *
   * @throws RemoteException  on an export error
   */
  public ServerOutputStream(OutputStream out, Exporter exporter)
                     throws RemoteException {
    super(exporter);
    this.out = out;
  }

  public void write(int b) throws IOException {
    out.write(b);
  }

  public void write(byte[] b) throws IOException {
    out.write(b);
  }

  public void write(PartialBuffer b) throws IOException {
    out.write(b.getBuffer(), b.getOffset(), b.getLength());
  }

  public void flush() throws IOException {
    out.flush();
  }

  public void close() throws IOException {
    unExport(false);
    out.close();
  }

  @Override
  public void unreferenced() {
    try {
      close();
    } catch (IOException e) {
      log.warn("Ignoring error in close of unreferenced OutputStream", e);
    }
  }

  // for testing
  OutputStream getOutputStream() {
    return out;
  }
}
