/* $HeadURL::                                                                            $
 * $Id$
 *
 * Copyright (c) 2009 DuraSpace
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
package org.akubraproject.www;

import java.io.IOException;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.net.URLStreamHandler;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.akubraproject.Blob;
import org.akubraproject.UnsupportedIdException;
import org.akubraproject.impl.AbstractBlobStoreConnection;
import org.akubraproject.impl.StreamManager;

/**
 * A connection for the BlobStore.
 *
 * @author Pradeep Krishnan
 */
class WWWConnection extends AbstractBlobStoreConnection {
  private final Map<String, URLStreamHandler> handlers = new HashMap<String, URLStreamHandler>();
  private       Map<URI, WWWBlob>             blobs = new HashMap<URI, WWWBlob>();

  /**
   * Creates a new WWWStoreConnection object.
   *
   * @param store         the BlobStore
   * @param handlers      the url stream-handlers (keyed by uri scheme) to use; if a handler is not
   *                      found then the java default one is used. May be null.
   * @param streamManager the stream-manager
   */
  public WWWConnection(WWWStore store, Map<String, URLStreamHandler> handlers,
                       StreamManager streamManager) {
    super(store, streamManager);
    if (handlers != null)
      this.handlers.putAll(handlers);
  }

  @Override
  public void close() {
    if (blobs != null) {
      for (WWWBlob blob : blobs.values())
        blob.closed();

      blobs.clear();
      blobs = null;
    }
    super.close();
  }

  /**
   * Gets a WWWBlob instance for an id. Instances are cached so that instance equality is
   * guaranteed for the same connection.
   *
   * @param blobId the blob identifier
   * @param create whether to create new blob instances
   *
   * @return the WWWBlob instance
   *
   * @throws IOException if the connection is closed or on a failure to create a WWWBlob instance
   * @throws IllegalArgumentException if the blobId is null or not an absolute URL
   */
  WWWBlob getWWWBlob(URI blobId, boolean create) throws IOException {
    if (blobs == null)
      throw new IllegalStateException("Connection closed.");

    if (blobId == null)
      throw new UnsupportedOperationException("Must supply a valid URL as the blob-id. " +
          "This store has no id generation capability.");

    WWWBlob blob = blobs.get(blobId);

    if ((blob == null) && create) {
      try {
        URL url = new URL(null, blobId.toString(), handlers.get(blobId.getScheme()));
        blob = new WWWBlob(url, this, streamManager);
      } catch (MalformedURLException e) {
        throw new UnsupportedIdException(blobId,  " must be a valid URL", e);
      }
      blobs.put(blobId, blob);
    }

    return blob;
  }

  @Override
  public Blob getBlob(URI blobId, Map<String, String> hints)
               throws IOException, IllegalArgumentException {
    return getWWWBlob(blobId, true);
  }

  @Override
  public Iterator<URI> listBlobIds(String filterPrefix) throws IOException {
    throw new UnsupportedOperationException("blob-id listing not supported");
  }

  @Override
  public void sync() throws IOException {
    throw new UnsupportedOperationException("sync'ing not supported");
  }
}
