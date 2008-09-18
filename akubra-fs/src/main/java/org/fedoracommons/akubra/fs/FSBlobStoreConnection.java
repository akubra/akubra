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
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;

import java.util.Iterator;
import java.util.Map;

import org.fedoracommons.akubra.Blob;
import org.fedoracommons.akubra.BlobStoreConnection;

/**
 * Filesystem-backed BlobStoreConnection implementation.

 *
 * @author Chris Wilper
 */
class FSBlobStoreConnection implements BlobStoreConnection {
  private final File baseDir;
  private final PathAllocator pAlloc;
  private final String blobIdPrefix;

  public FSBlobStoreConnection(File baseDir, PathAllocator pAlloc) {
    this.baseDir = baseDir;
    this.pAlloc = pAlloc;
    this.blobIdPrefix = getBlobIdPrefix(baseDir);
  }

  /**
   * {@inheritDoc}
   */
  public Blob getBlob(URI blobId, Map<String, String> hints) throws IOException {
    final File file = getFile(blobId);
    if (file == null) {
      return null;
    }
    return new Blob() {
      public InputStream getInputStream() throws IOException {
        try {
          return new FileInputStream(file);
        } catch (FileNotFoundException e) {
          throw new RuntimeException("File has been deleted: " + file.getPath(), e);
        }
      }
      public OutputStream getOutputStream() throws IOException {
        throw new IOException("Operation not supported.");
      }
      public long getSize() {
        return file.length();
      }
    };
  }

  /**
   * {@inheritDoc}
   */
  public URI putBlob(URI blobId, Blob blob, Map<String, String> hints) throws IOException {
    File file = getFile(blobId);
    if (file == null) {
      // create
      String path = pAlloc.allocate(blobId, hints);
      file = new File(baseDir, path);
      writeFile(blob.getInputStream(), file);
      try {
        return new URI(blobIdPrefix + path);
      } catch (URISyntaxException wontHappen) {
        throw new RuntimeException(wontHappen);
      }
    } else {
      // update
      writeFile(blob.getInputStream(), file);
      return blobId;
    }
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
  public URI getBlobLocator(URI blobId, Map<String, String> hints) {
    // just make sure it exists within baseDir
    File file = getFile(blobId);
    if (file == null) {
      return null;
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

  private static void writeFile(InputStream in, File file) {
    makeParentDirs(file);
    try {
      writeStream(in, new FileOutputStream(file));
    } catch (IOException e) {
      throw new RuntimeException("Error writing file", e);
    }
  }

  private static void writeStream(InputStream in, OutputStream out)
      throws IOException {
    try {
      byte[] buf = new byte[4096];
      int len;
      while ((len = in.read(buf)) > 0) {
        out.write(buf, 0, len);
      }
    } finally {
      in.close();
      out.close();
    }
  }

  private static void makeParentDirs(File file) {
    File parent = file.getParentFile();
    if (parent != null && !parent.exists()) {
      if (!parent.mkdirs()) {
        throw new RuntimeException("Unable to create dir(s): " + parent.getPath());
      }
    }
  }

  private static String encode(String in) {
    try {
      return URLEncoder.encode(in, "UTF-8");
    } catch (UnsupportedEncodingException wontHappen) {
      throw new RuntimeException(wontHappen);
    }
  }
}
