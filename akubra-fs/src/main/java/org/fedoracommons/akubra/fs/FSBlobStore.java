/* $HeadURL:: http://gandalf.topazproject.org/svn/head/topaz/core/src/main/java/org/topa#$
 * $Id: ClassMetadata.java 5434 2008-04-12 11:41:51Z ronald $
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

import java.util.ArrayList;
import java.util.List;

import javax.transaction.Transaction;

import org.fedoracommons.akubra.BlobStore;
import org.fedoracommons.akubra.BlobStoreConnection;
import org.fedoracommons.akubra.Capability;

/**
 * Filesystem-backed BlobStore implementation.
 *
 * @author Chris Wilper
 */
public class FSBlobStore implements BlobStore {

  private final File m_baseDir;

  private final PathAllocator m_pAlloc;

  private URI m_id;

  /**
   * Creates an instance with the given id and base storage directory,
   * using the DefaultPathAllocator and the DefaultFilenameAllocator.
   * 
   * @param baseDir the base storage directory.
   */
  public FSBlobStore(File baseDir) {
    m_baseDir = baseDir;
    m_pAlloc = new DefaultPathAllocator();
  }

  /**
   * Creates an instance with the given id, base storage directory,
   * and path allocator.
   * 
   * @param id the unique identifier of this blobstore.
   * @param baseDir the base storage directory.
   * @param pAlloc the PathAllocator to use.
   */
  public FSBlobStore(File baseDir, PathAllocator pAlloc) {
    m_baseDir = baseDir;
    m_pAlloc = pAlloc;
  }

  /**
   * {@inheritDoc}
   */
  public URI getId() {
    // Q: what's this for?
    return m_id;
  }

  /**
   * {@inheritDoc}
   */
  public void setId(URI id) {
    // Q: are implementations supposed to persist this?
    m_id = id;
  }

  /**
   * {@inheritDoc}
   */
  public List<BlobStore> getBackingStores() {
    // Q: if none, should implementations return a 0-size list or null?
    return new ArrayList<BlobStore>(0);
  }

  /**
   * {@inheritDoc}
   */
  public void setBackingStores(List<BlobStore> stores) {
    // Q: what if my impl doesn't support this? throw unsupported, as below?
    throw new UnsupportedOperationException();
  }

  /**
   * {@inheritDoc}
   */
  public BlobStoreConnection openConnection(Transaction tx) {
     return new FSBlobStoreConnection(m_baseDir, m_pAlloc);
  }

  /**
   * {@inheritDoc}
   */
  public Capability[] getDeclaredCapabilities() {
    // Q: if none, should implementations return a 0-size array or null?
    return new Capability[0];
  }

  /**
   * {@inheritDoc}
   */
  public Capability[] getCapabilities() {
    // Q: ditto
    return new Capability[0];
  }
}
