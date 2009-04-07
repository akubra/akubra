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
package org.fedoracommons.akubra.impl;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.fedoracommons.akubra.BlobStore;

/**
 * Abstract BlobStore implementation with no backing stores or capabilities.
 *
 * @author Chris Wilper
 */
public abstract class AbstractBlobStore implements BlobStore {
  /** This store's id */
  protected final URI id;
  /** This store's declared capabilities */
  protected Set<URI> decCaps;

  /**
   * Create a new blob store.
   *
   * @param id the store's id
   */
  protected AbstractBlobStore(URI id) {
    this.id = id;
    decCaps = Collections.emptySet();
  }

  /**
   * Create a new blob store.
   *
   * @param id the store's id
   * @param decCaps declared capabilities of this store
   */
  protected AbstractBlobStore(URI id, URI ... decCaps) {
    this.id = id;
    this.decCaps = Collections.unmodifiableSet(new HashSet<URI>(Arrays.asList(decCaps)));
  }

  //@Override
  public URI getId() {
    return id;
  }

  /**
   * This implementation returns an empty list.
   * Subclasses that support backing stores should override this.
   */
  //@Override
  public List<? extends BlobStore> getBackingStores() {
    return new ArrayList<BlobStore>(0);
  }

  /**
   * This implementation throws {@link UnsupportedOperationException}.
   * Subclasses that support backing stores should override this.
   */
  //@Override
  public void setBackingStores(List<? extends BlobStore> stores) {
    throw new UnsupportedOperationException();
  }

  /**
   * This implementation returns an empty array.
   * Subclasses that support declared capabilities should override this.
   */
  //@Override
  public Set<URI> getDeclaredCapabilities() {
    return decCaps;
  }

  /**
   * This implementation returns the union of the declared capabilities of this store and
   * of the capabilities of all backing stores.
   */
  //@Override
  public Set<URI> getCapabilities() {
    Set<URI> caps = new HashSet<URI>(getDeclaredCapabilities());

    for (BlobStore bs : getBackingStores())
      caps.addAll(bs.getCapabilities());

    return caps;
  }
}
