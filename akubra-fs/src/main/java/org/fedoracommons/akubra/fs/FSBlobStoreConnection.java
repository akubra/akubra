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
package org.fedoracommons.akubra.fs;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;

import java.util.Iterator;
import java.util.Map;

import org.fedoracommons.akubra.Blob;
import org.fedoracommons.akubra.BlobStore;
import org.fedoracommons.akubra.BlobStoreConnection;
import org.fedoracommons.akubra.DuplicateBlobException;

/**
 * Filesystem-backed BlobStoreConnection implementation.
 *
 * @author Chris Wilper
 */
class FSBlobStoreConnection implements BlobStoreConnection {
  private final BlobStore blobStore;
  private final File baseDir;
  private final PathAllocator pAlloc;
  private final String blobIdPrefix;

  FSBlobStoreConnection(BlobStore blobStore, File baseDir, PathAllocator pAlloc) {
    this.blobStore = blobStore;
    this.baseDir = baseDir;
    this.pAlloc = pAlloc;
    this.blobIdPrefix = getBlobIdPrefix(baseDir);
  }

  /**
   * {@inheritDoc}
   */
  public BlobStore getBlobStore() {
    return blobStore;
  }

  /**
   * {@inheritDoc}
   */
  public Blob createBlob(URI blobId, Map<String, String> hints) throws DuplicateBlobException, IOException {
    File file = getFile(blobId);
    if (file == null) {
       // create
      String path = pAlloc.allocate(blobId, hints);
      file = new File(baseDir, path);
      try {
        return new FSBlob(this, new URI(blobIdPrefix + path), file);
      } catch (URISyntaxException wontHappen) {
        throw new RuntimeException(wontHappen);
      }
    } else {
      throw new DuplicateBlobException(blobId);
    }
  }

  /**
   * {@inheritDoc}
   */
  public Blob getBlob(URI blobId, Map<String, String> hints) throws IOException {
    final File file = getFile(blobId);
    if (file == null) {
      return null;
    }
    return new FSBlob(this, blobId, file);
  }

  /**
   * {@inheritDoc}
   */
  public URI removeBlob(URI blobId, Map<String, String> hints) throws IOException {
    File file = getFile(blobId);
    if (file == null) {
      return null;
    }
    if (!file.delete()) {
      throw new RuntimeException("Unable to delete file: " + file.getPath());
    }
    return blobId;
  }

  /**
   * {@inheritDoc}
   */
  public Iterator<URI> listBlobIds(String filterPrefix) {
    return new FSBlobIdIterator(baseDir, filterPrefix);
  }

  /**
   * {@inheritDoc}
   */
  public void close() {
    // nothing to do
  }

  protected static String getBlobIdPrefix(File dir) {
    return "file:///" + getEncodedPath(dir);
  }

  // gets a path like usr/local/some%20dir%20with%20space/
  private static String getEncodedPath(File dir) {
    if (dir.getName().length() == 0) {
      return "";
    }
    String current = encode(dir.getName()) + "/";
    File parent = dir.getParentFile();
    if (parent != null) {
      return getEncodedPath(parent) + current;
    } else {
      return current;
    }
  }

  // gets the file with the given id if exists in store, else null
  private File getFile(URI blobId) {
    if (blobId == null) {
      return null;
    }
    if (blobId.toString().startsWith(blobIdPrefix)) {
      File file = new File(blobId.getRawPath());
      if (file.exists())
        return file;
    }
    return null;
  }

  private static String encode(String in) {
    try {
      return URLEncoder.encode(in, "UTF-8");
    } catch (UnsupportedEncodingException wontHappen) {
      throw new RuntimeException(wontHappen);
    }
  }
}
