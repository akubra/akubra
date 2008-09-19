/* $HeadURL$
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
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import java.net.URI;

import org.fedoracommons.akubra.Blob;
import org.fedoracommons.akubra.BlobStoreConnection;

/**
 * File-backed Blob implementation.
 *
 * @author Chris Wilper
 */
class FSBlob implements Blob {
  private final BlobStoreConnection connection;
  private final URI blobId;
  private final File file;

  FSBlob(BlobStoreConnection connection, URI blobId, File file) {
    this.connection = connection;
    this.blobId = blobId;
    this.file = file;
  }

  public BlobStoreConnection getConnection() {
    return connection;
  }

  public URI getId() {
    return blobId;
  }

  public URI getLocatorId() {
    return blobId;
  }

  public InputStream openInputStream() throws IOException {
    return new FileInputStream(file);
  }

  public OutputStream openOutputStream() throws IOException {
    if (!file.exists()) {
      makeParentDirs(file);
    }
    return new FileOutputStream(file);
  }

  public long getSize() {
    return file.length();
  }

  private static void makeParentDirs(File file) throws IOException {
    File parent = file.getParentFile();
    if (parent != null && !parent.exists()) {
      if (!parent.mkdirs()) {
        throw new IOException("Unable to create directory: " + parent.getPath());
      }
    }
  }
}
