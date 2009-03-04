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
package org.fedoracommons.akubra.www;

import java.io.IOException;

import java.net.URI;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.fedoracommons.akubra.Blob;
import org.fedoracommons.akubra.BlobStore;
import org.fedoracommons.akubra.BlobStoreConnection;
import org.fedoracommons.akubra.DuplicateBlobException;
import org.fedoracommons.akubra.MissingBlobException;

/**
 * A connection for the BlobStore.
 *
 * @author Pradeep Krishnan
 */
class WWWConnection implements BlobStoreConnection {
  private WWWStore          store;
  private Map<URI, WWWBlob> blobs = new HashMap<URI, WWWBlob>();

  /**
   * Creates a new WWWStoreConnection object.
   *
   * @param store the BlobStore
   */
  public WWWConnection(WWWStore store) {
    this.store = store;
  }

  public void close() {
    blobs.clear();
    blobs = null;
  }

  /**
   * Gets a WWWBlob instance for an id. Instances are cached so that instance equality is
   * guaranteed for the same connection.
   *
   * @param blobId the blob identifier
   * @param create wether to create new blob instances
   *
   * @return the WWWBlob instance
   *
   * @throws IOException if the connection is closed or on a failure to create a WWWBlob instance
   * @throws IllegalArgumentException if the blobId is null or not an absolute URL
   */
  WWWBlob getWWWBlob(URI blobId, boolean create) throws IOException {
    if (blobs == null)
      throw new IOException("Connection closed.");

    if (blobId == null)
      throw new IllegalArgumentException("blobId is null");

    WWWBlob blob = blobs.get(blobId);

    if ((blob == null) && create) {
      blob = new WWWBlob(blobId.toURL(), this);
      blobs.put(blobId, blob);
    }

    return blob;
  }

  public Blob createBlob(URI blobId, Map<String, String> hints)
                  throws DuplicateBlobException, IOException {
    return getWWWBlob(blobId, true);
  }

  public Blob getBlob(URI blobId, Map<String, String> hints)
               throws IOException {
    return getWWWBlob(blobId, true);
  }

  public BlobStore getBlobStore() {
    return store;
  }

  public Iterator<URI> listBlobIds(String filterPrefix)
                            throws IOException {
    return null;
  }

  public URI removeBlob(URI blobId, Map<String, String> hints)
                 throws IOException {
    return null;
  }

  public void renameBlob(URI oldBlobId, URI newBlobId, Map<String, String> hints)
                  throws DuplicateBlobException, IOException, MissingBlobException {
  }
}
