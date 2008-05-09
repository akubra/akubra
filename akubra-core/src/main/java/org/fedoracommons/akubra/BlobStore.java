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
package org.fedoracommons.akubra;

import java.util.List;

import javax.transaction.TransactionManager;

/**
 * Interface to abstract the idea of a general transaction based blob store
 *
 * @author Chris Wilper
 * @author Pradeep Krishnan
 * @author Ronald Tschalär
 */
public interface BlobStore {
  /**
   * Return the list of stores underlying this store
   *
   * @return list of underlying blob stores
   */
  List<BlobStore> getBackingStores();

  /**
   * Set the list of back store
   *
   * @param stores the list of backing store
   */
  void setBackingStores(List<BlobStore> stores)
    throws UnsupportedOperationException, IllegalStateException;

  /**
   * Open connection to the blob store
   *
   * @param txmgr the transaction manager that will manage the transactions
   *
   * @return the connection to the blob store
   */
  BlobStoreConnection openConnection(TransactionManager txmgr);
}
