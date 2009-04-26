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
package org.fedoracommons.akubra.map;

import java.io.IOException;

import java.net.URI;

import javax.transaction.Transaction;

import org.fedoracommons.akubra.BlobStore;
import org.fedoracommons.akubra.BlobStoreConnection;
import org.fedoracommons.akubra.impl.AbstractBlobStore;

/**
 * Wraps an existing {@link BlobStore} to provide a blob id mapping layer.
 *
 * @author Chris Wilper
 */
public class IdMappingBlobStore extends AbstractBlobStore {
  private final BlobStore store;
  private final IdMapper mapper;

  /**
   * Creates an instance.
   *
   * @param id the id associated with this store.
   * @param store the store to wrap.
   * @param mapper the mapper to use.
   */
  public IdMappingBlobStore(URI id, BlobStore store, IdMapper mapper) {
    super(id);
    this.store = store;
    this.mapper = mapper;
  }

  //@Override
  public BlobStoreConnection openConnection(Transaction tx) throws IOException {
    BlobStoreConnection connection = store.openConnection(tx);
    return new IdMappingBlobStoreConnection(this, connection, mapper);
  }

  // TODO: remove after setQuiescent is removed from BlobStore interface
  public boolean setQuiescent(boolean quiescent) throws IOException {
    return store.setQuiescent(quiescent);
  }

}
