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

import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.Map;

import org.fedoracommons.akubra.Blob;
import org.fedoracommons.akubra.BlobStoreConnection;
import org.fedoracommons.akubra.DuplicateBlobException;
import org.fedoracommons.akubra.MissingBlobException;
import org.fedoracommons.akubra.impl.AbstractBlob;
import org.fedoracommons.akubra.impl.StreamManager;

/**
 * Simple in-memory blob store.
 *
 * @author Ronald Tschalär
 */
class MemBlob extends AbstractBlob {
  private final Map<URI, MemData> blobs;
  private final StreamManager     streamMgr;

  /**
   * Create a new in-memory blob.
   *
   * @param id        the blob's id
   * @param blobs     the blob-map to use (shared, hence needs to be synchronized)
   * @param streamMgr the stream-manager to use
   * @param owner     the blob-store connection we belong to
   */
  MemBlob(URI id, Map<URI, MemData> blobs, StreamManager streamMgr, BlobStoreConnection owner) {
    super(owner, id);
    this.blobs     = blobs;
    this.streamMgr = streamMgr;
  }

  //@Override
  public boolean exists() throws IOException {
    synchronized (blobs) {
      return blobs.containsKey(id);
    }
  }

  //@Override
  public void create() throws IOException {
    if (!streamMgr.lockUnquiesced())
      throw new IOException("Interrupted waiting for writable state");

    try {
      synchronized (blobs) {
        if (blobs.containsKey(id))
          throw new DuplicateBlobException(id);

        blobs.put(id, new MemData(0));
      }
    } finally {
      streamMgr.unlockState();
    }
  }

  //@Override
  public void delete() throws IOException {
    if (!streamMgr.lockUnquiesced())
      throw new IOException("Interrupted waiting for writable state");

    try {
      synchronized (blobs) {
        blobs.remove(id);
      }
    } finally {
      streamMgr.unlockState();
    }
  }

  //@Override
  public void moveTo(Blob dest) throws IOException, MissingBlobException, NullPointerException,
         IllegalArgumentException, DuplicateBlobException {
    if (!streamMgr.lockUnquiesced())
      throw new IOException("Interrupted waiting for writable state");

    try {
      if (dest == null)
        throw new NullPointerException("Destination blob may not be null");
      if (!(dest instanceof MemBlob))
        throw new IllegalArgumentException("Destination blob must be an instance of " +
                                           MemBlob.class);

      synchronized (blobs) {
        if (dest.exists())
          throw new DuplicateBlobException(dest.getId(), "Destination blob already exists");

        MemData data = blobs.remove(id);
        if (data == null)
          throw new MissingBlobException(getId());

        blobs.put(dest.getId(), data);
      }
    } finally {
      streamMgr.unlockState();
    }
  }

  //@Override
  public InputStream openInputStream() throws IOException {
    return streamMgr.manageInputStream(getConnection(), getData().getInputStream());
  }

  //@Override
  public OutputStream openOutputStream(long estimatedSize) throws IOException {
    if (!streamMgr.lockUnquiesced())
      throw new IOException("Interrupted waiting for writable state");

    try {
      MemData data = getData();

      if (estimatedSize > data.bufferSize()) {
        data = new MemData((int) Math.min(estimatedSize, Integer.MAX_VALUE));
        synchronized (blobs) {
          blobs.put(id, data);
        }
      }

      data.reset();
      return streamMgr.manageOutputStream(getConnection(), data);
    } finally {
      streamMgr.unlockState();
    }
  }

  //@Override
  public long getSize() throws IOException {
    return getData().size();
  }

  private MemData getData() throws MissingBlobException {
    synchronized (blobs) {
      MemData data = blobs.get(id);
      if (data == null)
        throw new MissingBlobException(getId());
      return data;
    }
  }
}
