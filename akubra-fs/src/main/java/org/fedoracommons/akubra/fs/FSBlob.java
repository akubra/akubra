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
package org.fedoracommons.akubra.fs;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import java.net.URI;

import org.fedoracommons.akubra.Blob;
import org.fedoracommons.akubra.DuplicateBlobException;
import org.fedoracommons.akubra.MissingBlobException;
import org.fedoracommons.akubra.UnsupportedIdException;
import org.fedoracommons.akubra.impl.AbstractBlob;
import org.fedoracommons.akubra.impl.StreamManager;

/**
 * File-backed Blob implementation.
 *
 * @author Chris Wilper
 */
class FSBlob extends AbstractBlob {
  static final String uriPrefix = "file:";
  private final File file;
  private final StreamManager manager;

  /**
   * Create a file based blob
   *
   * @param connection the blob store connection
   * @param blobId the identifier for the blob
   * @param file the file associated with the blob
   * @param manager the stream manager
   */
  FSBlob(FSBlobStoreConnection connection, URI blobId, File file, StreamManager manager) {
    super(connection, blobId);
    this.file = file;
    this.manager = manager;
  }

  //@Override
  public InputStream openInputStream() throws IOException {
    if (getConnection().isClosed())
      throw new IllegalStateException("Connection closed.");

    if (!file.exists())
      throw new MissingBlobException(getId());

    return manager.manageInputStream(getConnection(), new FileInputStream(file));
  }

  //@Override
  public OutputStream openOutputStream(long estimatedSize) throws IOException {
    if (getConnection().isClosed())
      throw new IllegalStateException("Connection closed.");

    if (!manager.lockUnquiesced())
      throw new IOException("Interrupted waiting for writable state");

    try {
      if (!file.exists())
        throw new MissingBlobException(getId());

      return manager.manageOutputStream(getConnection(), new FileOutputStream(file));
    } finally {
      manager.unlockState();
    }
  }

  //@Override
  public long getSize() throws IOException {
    if (getConnection().isClosed())
      throw new IllegalStateException("Connection closed.");

    if (!file.exists())
      throw new MissingBlobException(getId());

    return file.length();
  }

  //@Override
  public boolean exists() throws IOException {
    if (getConnection().isClosed())
      throw new IllegalStateException("Connection closed.");

    return file.exists();
  }

  //@Override
  public void create() throws IOException {
    if (getConnection().isClosed())
      throw new IllegalStateException("Connection closed.");

    if (!manager.lockUnquiesced())
      throw new IOException("Interrupted waiting for writable state");

    try {
      makeParentDirs(file);

      if (!file.createNewFile())
        throw new DuplicateBlobException(getId(), file + " exists");
    } finally {
      manager.unlockState();
    }
  }

  //@Override
  public void delete() throws IOException {
    if (getConnection().isClosed())
      throw new IllegalStateException("Connection closed.");

    if (!manager.lockUnquiesced())
      throw new IOException("Interrupted waiting for writable state");

    try {
      if (!file.delete() && file.exists())
        throw new IOException("Failed to delete file: " + file);
    } finally {
      manager.unlockState();
    }
  }

  //@Override
  public void moveTo(Blob blob) throws IOException {
    if (getConnection().isClosed())
      throw new IllegalStateException("Connection closed.");

    if (!manager.lockUnquiesced())
      throw new IOException("Interrupted waiting for writable state");

    try {
      if (blob == null)
        throw new NullPointerException("Blob can't be null");

      FSBlob.validateId(blob.getId());

      if (!(blob instanceof FSBlob))
        throw new IllegalArgumentException("Blob must be an instance of " + FSBlob.class);

      File other = ((FSBlob)blob).file;

      makeParentDirs(other);

      if (!file.renameTo(other)) {

        if (!file.exists())
          throw new MissingBlobException(getId());

        if (other.exists())
          throw new DuplicateBlobException(blob.getId());

        throw new IOException("Rename failed for an unknown reason.");
      }
    } finally {
      manager.unlockState();
    }
  }

  static void validateId(URI blobId) throws UnsupportedIdException {
    if (blobId == null)
      throw new NullPointerException("Blob id cannot be null");
    String str = blobId.toString();
    if (!str.startsWith(uriPrefix))
      throw new UnsupportedIdException(blobId, "Blob id must start with " + uriPrefix);
    if (str.charAt(uriPrefix.length()) == '/')
      throw new UnsupportedIdException(blobId, "Blob id cannot specify an absolute file path");
    if (str.endsWith("/"))
      throw new UnsupportedIdException(blobId, "Blob id cannot end with /");
    for (String seg: str.substring(uriPrefix.length()).split("\\/")) {
      if (seg.length() == 0)
        throw new UnsupportedIdException(blobId, "Blob id cannot contain //");
      if (seg.equals("..")) {
        throw new UnsupportedIdException(blobId, "Blob id cannot contain path navigation elements");
      }
    }
  }

  private static void makeParentDirs(File file) throws IOException {
    File parent = file.getParentFile();

    if (parent != null && !parent.exists() && !parent.mkdirs())
      throw new IOException("Unable to create parent directory: " + parent.getPath());
  }
}
