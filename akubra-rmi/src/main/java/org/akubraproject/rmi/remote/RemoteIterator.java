/* $HeadURL::                                                                            $
 * $Id$
 *
 * Copyright (c) 2009 DuraSpace
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
package org.akubraproject.rmi.remote;

import java.io.Serializable;

import java.rmi.Remote;
import java.rmi.RemoteException;

import java.util.List;

/**
 * The interface to support an {@link java.util.Iterator Iterator} backed by
 * items on a remote server. {@link java.util.Iterator#remove remove()} method
 * is not supported for now since the goal here is to have bulk/batch loading.
 *
 * @author Pradeep Krishnan
 */
public interface RemoteIterator<T extends Serializable> extends Remote {
  /**
   * Gets the next few items from the server.
   *
   * @param count max number of items to get
   *
   * @return the items. The length indicates the actual items returned.
   *
   * @throws RemoteException on an error in rmi transport
   */
  List<T> next(int count) throws RemoteException;
}
