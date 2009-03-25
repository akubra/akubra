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
package org.fedoracommons.akubra.impl;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Wraps an <code>OutputStream</code> to provide notification to a
 * <code>CloseListener</code> when closed.
 *
 * @author Chris Wilper
 */
class ManagedOutputStream extends FilterOutputStream {
  private final CloseListener listener;
  private boolean closed = false;

  /**
   * Creates an instance.
   *
   * @param listener the CloseListener to notify when closed.
   * @param stream the stream to wrap.
   */
  ManagedOutputStream(CloseListener listener, OutputStream stream) {
    super(stream);
    this.listener = listener;
  }

  /**
   * Closes the stream, then notifies the CloseListener.
   */
  @Override
  public void close() throws IOException {
    if (!closed) {
      super.close();
      closed = true;
      listener.notifyClosed(this);
    }
  }
}
