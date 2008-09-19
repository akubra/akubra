/* $HeadURL::                                                                            $
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
package org.fedoracommons.akubra;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import java.net.URI;

/**
 * Interface to abstract the idea of a blob in the blob store.
 *
 * @author Chris Wilper
 * @author Pradeep Krishnan
 * @author Ronald Tschalär
 */
public interface Blob {
  /**
   * Gets the connection that provided this blob.
   */
  BlobStoreConnection getConnection();

  /**
   * Gets the id of the blob.
   *
   * @return the id, or null if it has not yet been assigned.
   */
  URI getId();

  /**
   * Gets the locator-id of the blob.
   *
   * @return the locator-id, or null if it has not yet been assigned.
   */
  URI getLocatorId();

  /**
   * Opens a new InputStream for reading the content.
   *
   * @return the input stream.
   * @throws IOException if no content has yet been written or the stream
   *         cannot be opened for any other reason.
   */
  InputStream openInputStream() throws IOException;

  /**
   * Opens a new OutputStream for writing the content.
   *
   * @return the output stream.
   * @throws IOException if the stream cannot be opened for any reason.
   */
  OutputStream openOutputStream() throws IOException;

  /**
   * Gets the size of the blob, in bytes.
   *
   * @return the size in bytes, or -1 if unknown.
   */
  long getSize();
}
