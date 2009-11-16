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
package org.akubraproject.rmi.client;

import java.io.Serializable;

import java.rmi.RemoteException;

import java.util.Iterator;
import java.util.List;

import org.akubraproject.rmi.remote.RemoteIterator;

/**
 * An iterator that wraps a RemoteIterator.
 *
 * @author Pradeep Krishnan
  *
 * @param <T> the type of iterator
 */
class ClientIterator<T extends Serializable> implements Iterator<T> {
  private final RemoteIterator<T> ri;
  private final int               batchSize;
  private List<T>                 list = null;
  private Iterator<T>             it   = null;

  /**
   * Creates a new ClientIterator object.
   *
   * @param ri the remote iterator
   * @param batchSize the batch size to optimize calls to the server
   */
  public ClientIterator(RemoteIterator<T> ri, int batchSize) {
    this.ri          = ri;
    this.batchSize   = batchSize;
  }

  public boolean hasNext() {
    return load().hasNext();
  }

  public T next() {
    return load().next();
  }

  public void remove() {
    throw new UnsupportedOperationException("remove() is unsupported");
  }

  private Iterator<T> load() {
    if ((list != null) && (list.isEmpty() || it.hasNext()))
      return it;

    try {
      list = ri.next(batchSize);
    } catch (RemoteException re) {
      throw new RuntimeException("Failed to load next items from remote", re);
    }

    it = list.iterator();

    return it;
  }
}
