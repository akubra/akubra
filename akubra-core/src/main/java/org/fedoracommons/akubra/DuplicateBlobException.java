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
package org.fedoracommons.akubra;

import java.net.URI;

/**
 * This exception is thrown by a blob store in case in of missing blob in store.
 *
 * @author Chris Wilper
 * @author Pradeep Krishnan
 * @author Ronald Tschalär
 */
public class DuplicateBlobException extends AkubraException {
  public static final long serialVersionUID = 1L;
  private final URI blobId;

  /**
   * Exception indicating there is duplicate blob in the store associated with the blob-id.
   *
   * @param blobId the blob-id
   */
  public DuplicateBlobException(URI blobId) {
    this(blobId, "", null);
  }

  /**
   * Exception indicating there is duplicate blob in the store associated with the blob-id.
   *
   * @param blobId the blob-id
   * @param msg the details about the exception
   */
  public DuplicateBlobException(URI blobId, String msg) {
    this(blobId, msg, null);
  }

  /**
   * Exception indicating there is duplicate blob in the store associated with the blob-id.
   *
   * @param blobId the blob-id
   * @param msg    the details about the exception
   * @param cause  the underlying exception that caused this exception
   */
  public DuplicateBlobException(URI blobId, String msg, Throwable cause) {
    super("(Duplicate blob with id = '" + blobId + "')" + msg, cause);
    this.blobId = blobId;
  }

  /**
   * Return the duplicate blob-id
   *
   * @return the duplicate blob-id
   */
  public URI getBlobId() {
    return blobId;
  }
}
