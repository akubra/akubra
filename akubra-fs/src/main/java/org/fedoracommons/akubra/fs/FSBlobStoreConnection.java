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

import java.net.URI;

import java.util.Map;

import org.fedoracommons.akubra.Blob;
import org.fedoracommons.akubra.BlobStoreConnection;

/**
 * Filesystem-backed BlobStoreConnection implementation.
 * 
 * @author Chris Wilper
 */
public class FSBlobStoreConnection implements BlobStoreConnection {

  private final File baseDir;

  private final PathAllocator pAlloc;

  public FSBlobStoreConnection(File baseDir, PathAllocator pAlloc) {
    this.baseDir = baseDir;
    this.pAlloc = pAlloc;
  }

  /**
   * {@inheritDoc}
   */
  public Blob getBlob(URI blobId, Map<String, String> hints) {
    return null;
  }

  /**
   * {@inheritDoc}
   */
  public URI putBlob(URI blobId, Blob blob, Map<String, String> hints) {
    return null;
  }

  /**
   * {@inheritDoc}
   */
  public URI removeBlob(URI blobId, Map<String, String> hints) {
    return null;
  }


  /**
   * {@inheritDoc}
   */
  public URI getBlobLocator(URI blobId, Map<String, String> hints) {
    return null;
  }

  /**
   * {@inheritDoc}
   */
  public void close() {
  }
}
