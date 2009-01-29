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
package org.fedoracommons.akubra.util;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Wraps an <code>OutputStream</code> to provide notification to a
 * <code>CloseListener</code> when closed.
 *
 * @author Chris Wilper
 */
class ManagedOutputStream extends OutputStream {

  private final CloseListener listener;
  private final OutputStream stream;

  /**
   * Creates an instance.
   *
   * @param listener the CloseListener to notify when closed.
   * @param stream the stream to wrap.
   */
  ManagedOutputStream(CloseListener listener, OutputStream stream) {
    this.listener = listener;
    this.stream = stream;
  }

  @Override
  public void write(int b) throws IOException {
    stream.write(b);
  }

  @Override
  public void write(byte[] b) throws IOException {
    stream.write(b);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    stream.write(b, off, len);
  }

  @Override
  public void flush() throws IOException {
    stream.flush();
  }

  /**
   * Closes the stream, then notifies the CloseListener.
   */
  @Override
  public void close() throws IOException {
    stream.close();
    listener.notifyClosed(this);
  }

}
