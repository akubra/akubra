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
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.collections.Predicate;
import org.apache.commons.collections.iterators.FilterIterator;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.fedoracommons.akubra.Blob;
import org.fedoracommons.akubra.BlobStore;
import org.fedoracommons.akubra.BlobStoreConnection;
import org.fedoracommons.akubra.BlobWrapper;
import org.fedoracommons.akubra.DuplicateBlobException;
import org.fedoracommons.akubra.util.StreamManager;

/**
 * Connection implementation for in-memory blob store.
 *
 * @author Ronald Tschalär
 */
class MemConnection implements BlobStoreConnection {
  private static final Log log  = LogFactory.getLog(MemConnection.class);
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
  public Blob getBlob(URI blobId, Map<String, String> hints) {
    synchronized (blobs) {
      if (blobId == null) {
        do {
          blobId = MemBlobStore.getRandomId("urn:mem-store:gen-id:");
        } while (blobs.containsKey(blobId));
      }

      MemBlob b = blobs.get(blobId);
      if (b == null) {
        b = new MemBlob(blobId, streamMgr);
        blobs.put(blobId, b);
      }
      return new ConBlob(b);
    }
  }

  //@Override
  public Blob getBlob(InputStream content, Map<String, String> hints)
    throws IOException, UnsupportedOperationException {
    Blob blob = null;
    while (blob == null) {
      blob = getBlob((URI)null, hints);
      try {
        blob.create();
      } catch (DuplicateBlobException e) {
        blob = null;
        log.warn("Newly created blob already exists. Trying another ...", e);
      }
    }

    OutputStream out = null;
    try {
      IOUtils.copyLarge(content, out = blob.openOutputStream(-1));
      out.close();
      out = null;
    } finally {
      if (out != null)
        IOUtils.closeQuietly(out);
    }

    return blob;
  }

  //@Override
  public Iterator<URI> listBlobIds(final String filterPrefix) {
    synchronized (blobs) {
      return new FilterIterator(blobs.keySet().iterator(), new Predicate() {
         public boolean evaluate(Object object) {
           MemBlob blob = blobs.get(object);
           try {
             return blob.exists() && ((filterPrefix == null) || object.toString().startsWith(filterPrefix));
           } catch (IOException e) {
             return false;
           }
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

    @Override
    public void moveTo(Blob blob) throws IOException {
      if ((blob == null) || (blob.getId() == null))
        throw new NullPointerException("Blob can't be null");

      if (!(blob instanceof ConBlob))
        throw new IllegalArgumentException("Blob '" + blob.getId() + "' is not an instanceof " + ConBlob.class);

      delegate.moveTo(((ConBlob)blob).delegate);
    }
  }

}
