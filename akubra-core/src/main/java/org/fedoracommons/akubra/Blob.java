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
   *
   * @return the blob store connection that created this blob
   */
  BlobStoreConnection getConnection();

  /**
   * Gets the id of the blob. Blob ids are URIs as defined by RFC 3986, and therefore will
   * always have a scheme.
   *
   * @return the non-null immutable blob id
   */
  URI getId();

  /**
   * Opens a new InputStream for reading the content.
   *
   * @return the input stream.
   * @throws MissingBlobException if the blob does not {@link #exists exist}.
   * @throws IOException if the stream cannot be opened for any other reason.
   */
  InputStream openInputStream() throws IOException, MissingBlobException;

  /**
   * Opens a new OutputStream for writing the content.
   *
   * @param estimatedSize the estimated size of the data if known (or -1 if unknown).
   *                      This can allow for the implementation to make better decisions
   *                      on buffering or reserving space.
   * @return the output stream.
   * @throws MissingBlobException if the blob does not {@link #exists exist}.
   * @throws IOException if the stream cannot be opened for any other reason.
   */
  OutputStream openOutputStream(long estimatedSize) throws IOException, MissingBlobException;

  /**
   * Gets the size of the blob, in bytes.
   *
   * @return the size in bytes, or -1 if unknown
   * @throws MissingBlobException if the blob does not {@link #exists exist}.
   * @throws IOException if an error occurred during
   */
  long getSize() throws IOException, MissingBlobException;

  /**
   * Tests if a blob with this id exists in this blob-store.
   *
   * @return true if the blob denoted by this id exists; false otherwise.
   *
   * @throws IOException if an error occurred during existence check
   * @see #create
   * @see #delete
   */
  boolean exists() throws IOException;

  /**
   * Creates a new empty blob. This operation is not idempotent and will
   * throw an exception if the blob already {@link #exists}.
   *
   * @throws DuplicateBlobException if a blob with the given id already exists
   * @throws IOException if the blob cannot be created for any other reason
   */
  void create() throws DuplicateBlobException, IOException;

  /**
   * Removes this blob from the store. This operation is idempotent and does
   * not throw an exception if the blob does not {@link #exists exist}.
   *
   * @throws IOException if the blob cannot be deleted for any reason
   */
  void delete() throws IOException;

  /**
   * Move this blob under the new id. Before the move, this blob must exist and the destination
   * blob must not. After the move, this blob will not exist but the destination blob will.
   *
   * @param blob the blob to move to
   *
   * @throws NullPointerException if the blob is null
   * @throws UnsupportedIdException if the blob is not recognized in this store
   * @throws MissingBlobException if this blob does not exist
   * @throws DuplicateBlobException if a blob with new blob-id already exists
   * @throws IOException if an error occurs while attempting the operation
   */
  void moveTo(Blob blob) throws DuplicateBlobException, IOException, MissingBlobException,
                                NullPointerException, UnsupportedIdException;
}
