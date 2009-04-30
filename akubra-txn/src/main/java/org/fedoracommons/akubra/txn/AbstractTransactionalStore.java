/* $HeadURL::                                                                            $
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

package org.fedoracommons.akubra.txn;

import java.io.IOException;
import java.net.URI;

import org.fedoracommons.akubra.BlobStore;
import org.fedoracommons.akubra.impl.AbstractBlobStore;

/**
 * A basic superclass for transactional stores.
 *
 * <p>Subclasses must implement {@link BlobStore#openConnection openConnection}.
 *
 * @author Ronald Tschalär
 */
public abstract class AbstractTransactionalStore extends AbstractBlobStore {
  /** the underlying blob-store used for the actual storage */
  protected final BlobStore wrappedStore;

  /**
   * Create a new transactional store.
   *
   * @param id      the id of this store
   * @param wrappedStore the wrapped non-transactional store
   */
  protected AbstractTransactionalStore(URI id, BlobStore wrappedStore) throws IOException {
    super(id);
    this.wrappedStore = wrappedStore;
  }
}
