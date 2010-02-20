/* $HeadURL::                                                                            $
 * $Id$
 *
 * Copyright (c) 2009-2010 DuraSpace
 * http://duraspace.org
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
package org.akubraproject.rmi.server;

import java.io.Serializable;

import java.rmi.RemoteException;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.akubraproject.rmi.remote.RemoteIterator;

/**
 * Server side implementation of RemoteIterator.
 *
 * @author Pradeep Krishnan
 *
 * @param <T> the type of the Iterator
 */
public class ServerIterator<T extends Serializable>
     extends UnicastExportable implements RemoteIterator<T> {
  private static final long serialVersionUID = 1L;
  private final Iterator<T> it;

  /**
   * Creates a new ServerIterator object.
   *
   * @param it the real iterator object
   * @param exporter the exporter to use
   *
   * @throws RemoteException on an error in export
   */
  public ServerIterator(Iterator<T> it, Exporter exporter) throws RemoteException {
    super(exporter);
    this.it = it;
  }

  public List<T> next(int count) throws RemoteException {
    if (count <= 0)
      throw new IllegalArgumentException("Invalid batch-size: " + count);

    List<T> items = new ArrayList<T>(count);

    while ((count-- > 0) && it.hasNext())
      items.add(it.next());

    if (items.isEmpty())
      unExport(false);

    return items;
  }

  // for testing
  Iterator<T> getIterator() {
    return it;
  }
}
