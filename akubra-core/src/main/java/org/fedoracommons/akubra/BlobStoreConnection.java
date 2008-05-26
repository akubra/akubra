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

import java.net.URI;

/**
 * Interface to abstract the idea of a connection to a transaction based blob store
 *
 * @author Chris Wilper
 * @author Pradeep Krishnan
 * @author Ronald Tschalär
 */
public interface BlobStoreConnection {
  /**
   * Return the blob associated with the blob-id
   *
   * @param blobId URI identifying the blob
   *
   * @return the blob or null in case of no blob with the blob-id
   */
  Blob getBlob(URI blobId);

  /**
   * Stores a blob in the store.
   * 
   * If the blob does not have a blob-id, then the store will generate and attach one associated
   * with the blob. If a blob already exists with the blob-id, then the blob will be overwritten.
   * Compliant stores will expect applications to not modify the passed blob till the transaction is
   * completed (commit or rollback).
   * 
   * @param blob      the blob to store.
   *
   * @return the blob locator-id.
   */
  URI putBlob(Blob blob);

  /**
   * Remove the blob from the store
   *
   * @param blobId URI identifying the blob
   *
   * @return URI locatator-id of the deleted blob or null in case of no blob found
   */
  URI removeBlob(URI blobId);

  /**
   * Get the locator-id associated with the blob
   *
   * @param blobId URI identifying the blob
   *
   * @return URI identifying the location or null in case of no such blob
   */
  URI getBlobLocator(URI blobId);

  /**
   * Close the connection to the blob store
   */
  void close();
}
