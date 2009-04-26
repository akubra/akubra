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
import java.io.InputStream;
import java.io.OutputStream;

import java.net.URI;

import java.util.Map;

import org.fedoracommons.akubra.Blob;
import org.fedoracommons.akubra.BlobStoreConnection;
import org.fedoracommons.akubra.DuplicateBlobException;
import org.fedoracommons.akubra.MissingBlobException;

/**
 * Wraps an existing {@Blob} to provide id mapping where appropriate.
 *
 * @author Chris Wilper
 */
class IdMappingBlob implements Blob {
  private final BlobStoreConnection connection;
  private final Blob blob;
  private final IdMapper mapper;

  public IdMappingBlob(BlobStoreConnection connection, Blob blob, IdMapper mapper) {
    this.connection = connection;
    this.blob = blob;
    this.mapper = mapper;
  }

  //@Override
  public void delete() throws IOException {
    blob.delete();
  }

  //@Override
  public boolean exists() throws IOException {
    return blob.exists();
  }

  //@Override
  public URI getCanonicalId() throws IOException {
    URI internalId = blob.getCanonicalId();
    if (internalId == null)
      return null;
    return mapper.getExternalId(internalId);
  }

  //@Override
  public BlobStoreConnection getConnection() {
    return connection;
  }

  //@Override
  public URI getId() {
    return mapper.getExternalId(blob.getId());
  }

  //@Override
  public long getSize() throws IOException, MissingBlobException {
    return blob.getSize();
  }

  //@Override
  public Blob moveTo(URI blobId, Map<String, String> hints) throws DuplicateBlobException,
      IOException, MissingBlobException, NullPointerException, IllegalArgumentException {
    Blob internalBlob;
    if (blobId == null)
      internalBlob = blob.moveTo(null, hints);
    else
      internalBlob = blob.moveTo(mapper.getInternalId(blobId), hints);
    return new IdMappingBlob(connection, internalBlob, mapper);
  }

  //@Override
  public InputStream openInputStream() throws IOException, MissingBlobException {
    return blob.openInputStream();
  }

  //@Override
  public OutputStream openOutputStream(long estimatedSize, boolean overwrite) throws IOException,
      DuplicateBlobException {
    return blob.openOutputStream(estimatedSize, overwrite);
  }

}
