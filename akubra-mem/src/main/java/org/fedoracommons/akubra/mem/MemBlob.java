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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;

import org.fedoracommons.akubra.Blob;
import org.fedoracommons.akubra.BlobStoreConnection;
import org.fedoracommons.akubra.DuplicateBlobException;
import org.fedoracommons.akubra.MissingBlobException;
import org.fedoracommons.akubra.util.StreamManager;

/**
 * Simple in-memory blob store.
 *
 * @author Ronald Tschalär
 */
class MemBlob implements Blob {
  private MemOutputStream data;
  private final StreamManager   streamMgr;
  private       URI             id;

  /**
   * Create a new in-memory blob.
   *
   * @param id        the blob's id
   * @param streamMgr the stream-manager to use
   */
  MemBlob(URI id, StreamManager streamMgr) {
    this.id        = id;
    this.streamMgr = streamMgr;
    data = null;
  }

  //@Override
  public URI getId() {
    return id;
  }

  //@Override
  public BlobStoreConnection getConnection() {
    throw new Error("this blob implementation needs to be wrapped");
  }

  //@Override
  public synchronized boolean exists() throws IOException {
    return data != null;
  }

  //@Override
  public synchronized void create() throws IOException {
    if (!streamMgr.lockUnquiesced())
      throw new IOException("Interrupted waiting for writable state");

    try {
      if (data != null)
        throw new DuplicateBlobException(id);

      data = new MemOutputStream();
    } finally {
      streamMgr.unlockState();
    }
  }

  //@Override
  public synchronized void delete() throws IOException {
    if (!streamMgr.lockUnquiesced())
      throw new IOException("Interrupted waiting for writable state");

    try {
      data = null;
    } finally {
      streamMgr.unlockState();
    }
  }

  private synchronized void atomicMove(MemOutputStream data) throws IOException {
    if (this.data != null)
       throw new DuplicateBlobException(getId());
    this.data = data;
  }

  //@Override
  public synchronized void moveTo(Blob blob) throws IOException,
        MissingBlobException, NullPointerException, IllegalArgumentException {
    if (!streamMgr.lockUnquiesced())
      throw new IOException("Interrupted waiting for writable state");

    try {
      if (!(blob instanceof MemBlob))
        throw new IllegalArgumentException("Blob must be an instance of " + MemBlob.class);

      if (!exists())
        throw new MissingBlobException(getId());

      ((MemBlob)blob).atomicMove(data);
      data = null;
    } finally {
      streamMgr.unlockState();
    }
  }

  //@Override
  public synchronized InputStream openInputStream() throws IOException {
    if (data == null)
      throw new MissingBlobException(getId());

    return new ByteArrayInputStream(data.getBuf(), 0, data.size());
  }

  //@Override
  public synchronized OutputStream openOutputStream(long estimatedSize) throws IOException {
    if (!streamMgr.lockUnquiesced())
      throw new IOException("Interrupted waiting for writable state");

    try {
      if (data == null)
        throw new MissingBlobException(getId());

      data.reset();
      return streamMgr.manageOutputStream(data);
    } finally {
      streamMgr.unlockState();
    }
  }

  //@Override
  public synchronized long getSize() throws IOException {
    if (data == null)
      throw new MissingBlobException(getId());

    return data.size();
  }

  /**
   * A byte-array output stream in which we can access the buffer directly.
   */
  private static class MemOutputStream extends ByteArrayOutputStream {
    byte[] getBuf() {
      return buf;
    }
  }

}
