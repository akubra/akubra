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

import java.io.IOException;

import java.net.URI;

import java.sql.Connection;

import java.util.Iterator;
import java.util.Map;

import org.fedoracommons.akubra.Blob;
import org.fedoracommons.akubra.BlobStore;
import org.fedoracommons.akubra.BlobStoreConnection;
import org.fedoracommons.akubra.DuplicateBlobException;
import org.fedoracommons.akubra.MissingBlobException;

/**
 *
 * @author Chris Wilper
 */
class MappingBlobStoreConnection implements BlobStoreConnection {
  private final BlobStore blobStore;
  private final BlobStoreConnection bsConn;
  private final Connection dbConn;
  private final String tableName;

  public MappingBlobStoreConnection(BlobStore blobStore,
      BlobStoreConnection bsConn, Connection dbConn, String tableName) {
    this.blobStore = blobStore;
    this.bsConn = bsConn;
    this.dbConn = dbConn;
    this.tableName = tableName;
  }

  //@Override
  public BlobStore getBlobStore() {
    return blobStore;
  }

  //@Override
  public Blob createBlob(URI blobId, Map<String, String> hints) throws DuplicateBlobException,
      IOException {
    return null;
  }

  //@Override
  public Blob getBlob(URI blobId, Map<String, String> hints) throws IOException {
    return null;
  }

  //@Override
  public void renameBlob(URI oldBlobId, URI newBlobId, Map<String, String> hints)
      throws DuplicateBlobException, IOException, MissingBlobException {
  }

  //@Override
  public URI removeBlob(URI blobId, Map<String, String> hints) throws IOException {
    return null;
  }

  //@Override
  public Iterator<URI> listBlobIds(String filterPrefix) {
    return null;
  }

  //@Override
  public void close() {
  }

}
