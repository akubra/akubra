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
package org.akubraproject.rmi.client;

import java.io.IOException;
import java.io.OutputStream;

import org.akubraproject.rmi.remote.PartialBuffer;
import org.akubraproject.rmi.remote.RemoteOutputStream;

/**
 * An OutputStream wrapper that wraps a RemoteOutputStream.
 *
 * @author Pradeep Krishnan
  */
class ClientOutputStream extends OutputStream {
  private final RemoteOutputStream remote;

  /**
   * Creates a new ClientOutputStream object.
   *
   * @param out the remote output stream to write to
   */
  public ClientOutputStream(RemoteOutputStream out) {
    this.remote = out;
  }

  @Override
  public void close() throws IOException {
    remote.close();
  }

  @Override
  public void flush() throws IOException {
    remote.flush();
  }

  @Override
  public void write(int b) throws IOException {
    remote.write(b);
  }

  @Override
  public void write(byte[] buf) throws IOException {
    remote.write(buf);
  }

  @Override
  public void write(byte[] buf, int off, int len) throws IOException {
    if ((off < 0) || (off > buf.length))
      throw new IllegalArgumentException("offset " + off + " is out of bounds");

    if ((len < 0) || ((off + len) > buf.length))
      throw new IllegalArgumentException("length " + len + " is out of bounds");

    if (len == 0)
      return;

    remote.write(new PartialBuffer(buf, off, len));
  }
}
