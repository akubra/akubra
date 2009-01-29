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
package org.fedoracommons.akubra;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

/**
 * Abstract BlobStore implementation with no backing stores or capabilities.
 *
 * @author Chris Wilper
 */
public abstract class AbstractBlobStore implements BlobStore {
  /** This store's id */
  protected final URI id;

  /**
   * Create a new blob store.
   *
   * @param id the store's id
   */
  protected AbstractBlobStore(URI id) {
    this.id = id;
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
  public List<BlobStore> getBackingStores() {
    return new ArrayList<BlobStore>(0);
  }

  /**
   * This implementation throws {@link UnsupportedOperationException}.
   * Subclasses that support backing stores should override this.
   */
  //@Override
  public void setBackingStores(List<BlobStore> stores) {
    throw new UnsupportedOperationException();
  }

  /**
   * This implementation returns an empty array.
   * Subclasses that support declared capabilities should override this.
   */
  //@Override
  public Capability[] getDeclaredCapabilities() {
    return new Capability[0];
  }

  /**
   * This implementation returns an empty array.
   * Subclasses that support capabilties should override this.
   */
  //@Override
  public Capability[] getCapabilities() {
    return new Capability[0];
  }

}
