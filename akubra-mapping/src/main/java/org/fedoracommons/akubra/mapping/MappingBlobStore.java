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
package org.fedoracommons.akubra.mapping;

import java.net.URI;

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;
import javax.transaction.Transaction;

import org.fedoracommons.akubra.AbstractBlobStore;
import org.fedoracommons.akubra.BlobStore;
import org.fedoracommons.akubra.BlobStoreConnection;

/**
 * A non-transactional wrapper that provides arbitrary, caller-specified blob
 * ids over an existing (non-transactional) BlobStore that only supports
 * store-generated ids.
 *
 * @author Chris Wilper
 */
public class MappingBlobStore extends AbstractBlobStore {
  private final URI id;
  private final BlobStore wrappedStore;
  private final DataSource dataSource;
  private final String mapTable;

  public MappingBlobStore(URI id, BlobStore wrappedStore, DataSource dataSource,
                          String mapTable) {
    this.id = id;
    this.wrappedStore = wrappedStore;
    this.dataSource = dataSource;
    this.mapTable = mapTable;
  }

  //@Override
  public URI getId() {
    return id;
  }

  //@Override
  public BlobStoreConnection openConnection(Transaction tx) {
    BlobStoreConnection bsConn = wrappedStore.openConnection(tx);
    try {
      Connection dbConn = dataSource.getConnection();
      return new MappingBlobStoreConnection(this, bsConn, dbConn, mapTable);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  //@Override
  public boolean setQuiescent(boolean quiescent) {
    return wrappedStore.setQuiescent(quiescent);
  }
}
