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

public interface BlobStoreConnection {
  /**
   * Return the blob associated with the id
   *
   * @param blobId URI identifying the blob
   *
   * @return the content
   */
  Content getBlob(URI blobId);

  /**
   * Stores a blob with the given id.
   * 
   * If a blob id is specified, but such a blob already exists, its content will be replaced.
   * 
   * @param blobId the blob id, or null.
   * @param content the content to store.
   *
   * @return the blob locator-id.
   */
  URI putBlob(URI blobId, Content content);

  /**
   * Remove the blob from the store
   *
   * @param blobId URI identifying the blob
   *
   * @return URI identifying the deleted blob
   */
  URI removeBlob(URI blobId);

  /**
   * Get the locator associated with the id
   *
   * @param blobId URI identifying the blob
   *
   * @return URI identifying the location
   */
  URI getBlobLocator(URI blobId);

  /**
   * Close the connection to the blob store
   */
  void close();
}
