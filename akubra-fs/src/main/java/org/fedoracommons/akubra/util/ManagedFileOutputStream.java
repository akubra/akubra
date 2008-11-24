/* $HeadURL$
 * $Id$
 *
 * Copyright (c) 2007-2008 by Fedora Commons Inc.
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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * A <code>FileOutputStream</code> that provides notification to a
 * <code>StreamManager</code> when closed.
 *
 * @author Chris Wilper
 */
class ManagedFileOutputStream extends FileOutputStream {

  private final StreamManager manager;

  /**
   * Creates an instance.
   *
   * @param manager the StreamManager to notify when closed.
   * @param file the file to open for writing.
   * @throws FileNotFoundException if the file exists but is a directory rather
   *     than a regular file, does not exist but cannot be created, or cannot be
   *     opened for any other reason.
   */
  ManagedFileOutputStream(StreamManager manager, File file)
      throws FileNotFoundException {
    super(file);
    this.manager = manager;
  }

  /**
   * Closes the stream, then notifies the StreamManager.
   */
  @Override
  public void close() throws IOException {
    super.close();
    manager.notifyClosed(this);
  }

}
