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
package org.akubraproject.rmi.remote;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/**
 * Represents the parameters of {@link java.io.OutputStream#write(byte[], int, int)} call.
 * Using this is more efficient in terms of network transfer since the unused portions of the
 * buffer is not transferred across.
 *
 * @author Pradeep Krishnan
 */
public class PartialBuffer implements Serializable {
  private static final long serialVersionUID = 1L;
  private byte[]            b;
  private int               off;
  private int               len;

  /**
   * Creates a new PartialBuffer object.
   * 
   * @param b the buffer
   * @param off the offset in buffer
   * @param len the length in buffer
   */
  public PartialBuffer(byte[] b, int off, int len) {
    this.b     = b;
    this.off   = off;
    this.len   = len;
  }

  /**
   * Gets the length of data in buffer.
   *
   * @return the length
   */
  public int getLength() {
    return len;
  }

  /**
   * Gets the start offset of data in the buffer.
   *
   * @return the offset
   */
  public int getOffset() {
    return off;
  }

  /**
   * Gets the buffer.
   *
   * @return the buffer
   */
  public byte[] getBuffer() {
    return b;
  }

  private void writeObject(ObjectOutputStream out) throws IOException {
    out.writeInt(len);
    out.write(b, off, len);
  }

  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    off   = 0;
    len   = in.readInt();
    b     = new byte[len];
    in.readFully(b);
  }
}
