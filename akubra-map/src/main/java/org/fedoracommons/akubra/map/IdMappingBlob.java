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

import java.io.IOException;

import java.net.URI;

import java.util.Map;

import org.fedoracommons.akubra.Blob;
import org.fedoracommons.akubra.BlobStoreConnection;
import org.fedoracommons.akubra.DuplicateBlobException;
import org.fedoracommons.akubra.MissingBlobException;
import org.fedoracommons.akubra.impl.BlobWrapper;

/**
 * Wraps an existing {@Blob} to provide id mapping where appropriate.
 *
 * @author Chris Wilper
 */
class IdMappingBlob extends BlobWrapper {
  private final BlobStoreConnection connection;
  private final Blob blob;
  private final IdMapper mapper;

  public IdMappingBlob(BlobStoreConnection connection, Blob blob, IdMapper mapper) {
    super(blob, connection);
    this.connection = connection;
    this.blob = blob;
    this.mapper = mapper;
  }

  @Override
  public URI getCanonicalId() throws IOException {
    URI internalId = blob.getCanonicalId();
    if (internalId == null)
      return null;
    return mapper.getExternalId(internalId);
  }

  @Override
  public URI getId() {
    return mapper.getExternalId(blob.getId());
  }

  @Override
  public Blob moveTo(URI blobId, Map<String, String> hints) throws DuplicateBlobException,
      IOException, MissingBlobException, NullPointerException, IllegalArgumentException {
    Blob internalBlob;
    if (blobId == null)
      internalBlob = blob.moveTo(null, hints);
    else
      internalBlob = blob.moveTo(mapper.getInternalId(blobId), hints);
    return new IdMappingBlob(connection, internalBlob, mapper);
  }

}
