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

import java.net.URI;

import java.util.Iterator;
import java.util.Map;

/**
 * Interface to abstract the idea of a connection to a transaction based blob store
 *
 * @author Chris Wilper
 * @author Pradeep Krishnan
 * @author Ronald Tschalär
 */
public interface BlobStoreConnection {
  /**
   * Gets the blob store associated with this session.
   *
   * @return the blob store.
   */
  BlobStore getBlobStore();

  /**
   * Gets the blob with the given id.
   *
   * @param blobId the blob id
   * @param hints A set of hints to allow the implementation to optimize the operation (can be
   *              null)
   *
   * @return the blob. Cannot be null and must have the passed in blobId. If the passed in blobId
   *         is null, an id must be generated if the store is capable of generating ids. There is
   *         no requirement that the returned blob must {@link Blob#exists exist}. However there
   *         is a requirement that the {@link Blob#getConnection getConnection()} method of the
   *         returned Blob must return this connection object.
   *
   * @throws IOException for IO errors
   * @throws UnsupportedIdException if blobId is not in a recognized/usable pattern by this store
   *
   * @see BlobWrapper
   */
  Blob getBlob(URI blobId, Map<String, String> hints) throws IOException, IllegalArgumentException;

  /**
   * Creates a blob with the given content. For Content Addressible Storage (CAS) systems,
   * this is the only way to create a Blob. For other stores, there is also the {@link Blob#create
   * create}
   *
   * @param content the contents of the blob
   * @param estimatedSize the estimated size of the data if known (or -1 if unknown).
   *                      This can allow for the implementation to make better decisions
   *                      on buffering or reserving space.
   * @param hints A set of hints to allow the implementation to optimize the operation (can be
   *              null)
   *
   * @return the blob. Cannot be null and must have a generated id. The {@link Blob#getConnection
   *                   getConnection()} method must return this connection object.
   *
   * @throws IOException for IO errors
   * @throws UnsupportedOperationException if this store cannot generate new id and create a new blob
   *
   * @see BlobWrapper
   */
  Blob getBlob(InputStream content, long estimatedSize, Map<String, String> hints)
        throws IOException, UnsupportedOperationException;

  /**
   * Gets an iterator over the ids of all blobs in this store.
   *
   * @param filterPrefix If provided, the list will be limited to those blob-ids beginning with this prefix
   *
   * @return The iterator of blob-ids.
   * @throws IOException if an error occurred getting the list of blob ids
   */
  Iterator<URI> listBlobIds(String filterPrefix) throws IOException;

  /**
   * Close the connection to the blob store
   */
  void close();

  /**
   * Tests if the connection to the blob store is closed.
   *
   * @return true if the connection is closed.
   */
  boolean isClosed();
}
