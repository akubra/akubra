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
package org.akubraproject.rmi.client;

import java.io.IOException;
import java.io.InputStream;

import org.akubraproject.rmi.remote.PartialBuffer;
import org.akubraproject.rmi.remote.RemoteInputStream;

/**
 * InputStream wrapper that forwards all reads to a RemoteInputStream.
 *
 * @author Pradeep Krishnan
 */
class ClientInputStream extends InputStream {
  private final RemoteInputStream remote;

  /**
   * Creates a new ClientInputStream object.
   *
   * @param remote the rmi stub for the server side of this stream
   */
  public ClientInputStream(RemoteInputStream remote) {
    this.remote = remote;
  }

  @Override
  public int read() throws IOException {
    return remote.read();
  }

  @Override
  public int read(byte[] buf, int off, int len) throws IOException {
    if ((off < 0) || (off > buf.length))
      throw new IllegalArgumentException("offset " + off + " is out of bounds");

    if ((len < 0) || ((off + len) > buf.length))
      throw new IllegalArgumentException("length " + len + " is out of bounds");

    if (len == 0)
      return 0;

    PartialBuffer pb = remote.read(len);

    if (pb == null)
      return -1;

    if (pb.getLength() > len)
      throw new IOException("Server mis-behavior. Asked to read " + len + " got back "
                            + pb.getLength());

    System.arraycopy(pb.getBuffer(), pb.getOffset(), buf, off, pb.getLength());

    return pb.getLength();
  }

  @Override
  public long skip(long n) throws IOException {
    return remote.skip(n);
  }

  @Override
  public void close() throws IOException {
    remote.close();
  }
}
