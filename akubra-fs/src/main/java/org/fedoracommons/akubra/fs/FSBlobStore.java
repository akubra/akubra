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

import javax.transaction.Transaction;

import org.fedoracommons.akubra.AbstractBlobStore;
import org.fedoracommons.akubra.BlobStoreConnection;

/**
 * Filesystem-backed BlobStore implementation.
 *
 * @author Chris Wilper
 */
public class FSBlobStore extends AbstractBlobStore {

  private final File baseDir;

  private final PathAllocator pAlloc;

  private URI id;

  /**
   * Creates an instance with the given id and base storage directory,
   * using the DefaultPathAllocator and the DefaultFilenameAllocator.
   * 
   * @param id the unique identifier of this blobstore.
   * @param baseDir the base storage directory.
   */
  public FSBlobStore(URI id, File baseDir) {
    this.id = id;
    this.baseDir = baseDir;
    pAlloc = new DefaultPathAllocator();
  }

  /**
   * Creates an instance with the given id, base storage directory,
   * and path allocator.
   * 
   * @param id the unique identifier of this blobstore.
   * @param baseDir the base storage directory.
   * @param pAlloc the PathAllocator to use.
   */
  public FSBlobStore(URI id, File baseDir, PathAllocator pAlloc) {
    this.id = id;
    this.baseDir = baseDir;
    this.pAlloc = pAlloc;
  }

  /**
   * {@inheritDoc}
   */
  public URI getId() {
    return id;
  }

  /**
   * {@inheritDoc}
   */
  public BlobStoreConnection openConnection(Transaction tx) {
     return new FSBlobStoreConnection(baseDir, pAlloc);
  }

}
