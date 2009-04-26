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

import java.net.URI;

import java.util.Iterator;

/**
 * Wraps the internal blob id iterator to provide id mapping where appropriate.
 *
 * @author Chris Wilper
 */
class IdMappingBlobIdIterator implements Iterator<URI> {
  private final Iterator<URI> iterator;
  private final IdMapper mapper;

  /**
   * Creates an instance.
   *
   * @param iterator the iterator to wrap.
   * @param mapper the mapper to use.
   */
  public IdMappingBlobIdIterator(Iterator<URI> iterator, IdMapper mapper) {
    this.iterator = iterator;
    this.mapper = mapper;
  }

  //@Override
  public boolean hasNext() {
    return iterator.hasNext();
  }

  //@Override
  public URI next() {
    return mapper.getExternalId(iterator.next());
  }

  //@Override
  public void remove() {
    throw new UnsupportedOperationException();
  }

}