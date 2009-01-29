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

package org.fedoracommons.akubra.mem;

import java.io.IOException;
import java.net.URI;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.collections.Predicate;
import org.apache.commons.collections.iterators.FilterIterator;

import org.fedoracommons.akubra.Blob;
import org.fedoracommons.akubra.BlobStore;
import org.fedoracommons.akubra.BlobStoreConnection;
import org.fedoracommons.akubra.BlobWrapper;
import org.fedoracommons.akubra.DuplicateBlobException;
import org.fedoracommons.akubra.MissingBlobException;
import org.fedoracommons.akubra.util.StreamManager;

/**
 * Connection implementation for in-memory blob store.
 *
 * @author Ronald Tschalär
 */
class MemConnection implements BlobStoreConnection {
  private final BlobStore         owner;
  private final Map<URI, MemBlob> blobs;
  private final StreamManager     streamMgr;

  /**
   * Create a new connection.
   *
   * @param owner     the owning blob-store
   * @param blobs     the blob-map to use (shared, hence needs to be synchronized)
   * @param streamMgr the stream-manager to use
   */
  MemConnection(MemBlobStore owner, Map<URI, MemBlob> blobs, StreamManager streamMgr) {
    this.owner     = owner;
    this.blobs     = blobs;
    this.streamMgr = streamMgr;
  }

  //@Override
  public BlobStore getBlobStore() {
    return owner;
  }

  //@Override
  public Blob createBlob(URI blobId, Map<String, String> hints) throws DuplicateBlobException {
    synchronized (blobs) {
      if (blobs.containsKey(blobId))
        throw new DuplicateBlobException(blobId);

      if (blobId == null) {
        do {
          blobId = MemBlobStore.getRandomId("urn:mem-store:gen-id:");
        } while (blobs.containsKey(blobId));
      }

      MemBlob res = new MemBlob(blobId, streamMgr);
      blobs.put(blobId, res);
      return new ConBlob(res);
    }
  }

  //@Override
  public Blob getBlob(URI blobId, Map<String, String> hints) {
    synchronized (blobs) {
      MemBlob b = blobs.get(blobId);
      return (b != null) ? new ConBlob(b) : null;
    }
  }

  //@Override
  public void renameBlob(URI oldBlobId, URI newBlobId, Map<String, String> hints)
      throws DuplicateBlobException, MissingBlobException {
    synchronized (blobs) {
      if (blobs.containsKey(newBlobId))
        throw new DuplicateBlobException(newBlobId);

      MemBlob b = blobs.remove(oldBlobId);
      if (b == null)
        throw new MissingBlobException(oldBlobId);

      b.setId(newBlobId);
      blobs.put(newBlobId, b);
    }
  }

  //@Override
  public URI removeBlob(URI blobId, Map<String, String> hints) throws IOException {
    synchronized (blobs) {
      return (blobs.remove(blobId) != null) ? blobId : null;
    }
  }

  //@Override
  public Iterator<URI> listBlobIds(final String filterPrefix) {
    synchronized (blobs) {
      return new FilterIterator(blobs.keySet().iterator(), new Predicate() {
         public boolean evaluate(Object object) {
           return (filterPrefix == null) || object.toString().startsWith(filterPrefix);
         }
      });
    }
  }

  //@Override
  public void close() {
  }

  private class ConBlob extends BlobWrapper {
    ConBlob(Blob b) {
      super(b);
    }

    @Override
    public BlobStoreConnection getConnection() {
      return MemConnection.this;
    }
  }
}
