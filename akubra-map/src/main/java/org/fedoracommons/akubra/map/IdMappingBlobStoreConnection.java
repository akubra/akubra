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

import java.net.URI;

import java.util.Iterator;
import java.util.Map;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;

import org.fedoracommons.akubra.Blob;
import org.fedoracommons.akubra.BlobStore;
import org.fedoracommons.akubra.BlobStoreConnection;
import org.fedoracommons.akubra.UnsupportedIdException;
import org.fedoracommons.akubra.impl.AbstractBlobStoreConnection;

/**
 * Wraps the internal {@link BlobStoreConnection} to provide id mapping where
 * appropriate.
 *
 * @author Chris Wilper
 */
class IdMappingBlobStoreConnection extends AbstractBlobStoreConnection {
  private final BlobStoreConnection connection;
  private final IdMapper mapper;

  /**
   * Creates an instance.
   *
   * @param store the store from which this connection originated.
   * @param connection the wrapped connection.
   * @param mapper the mapper to use.
   */
  public IdMappingBlobStoreConnection(BlobStore store,
                                      BlobStoreConnection connection,
                                      IdMapper mapper) {
    super(store);
    this.connection = connection;
    this.mapper = mapper;
  }

  //@Override
  public Blob getBlob(URI blobId, Map<String, String> hints)
      throws IOException, UnsupportedIdException, UnsupportedOperationException {
    Blob internalBlob;
    if (blobId == null)
      internalBlob = connection.getBlob(null, hints);
    else
      internalBlob = connection.getBlob(mapper.getInternalId(blobId), hints);
    return new IdMappingBlob(this, internalBlob, mapper);
  }

  //@Override
  public Blob getBlob(InputStream content, long estimatedSize, Map<String, String> hints)
      throws IOException, UnsupportedOperationException {
    Blob internalBlob = connection.getBlob(content, estimatedSize, hints);
    return new IdMappingBlob(this, internalBlob, mapper);
  }

  //@Override
  public Iterator<URI> listBlobIds(String filterPrefix) throws IOException {
    Iterator<URI> iterator = connection.listBlobIds(filterPrefix);
    return Iterators.transform(iterator, new Function<URI, URI>() {
      public URI apply(URI uri) {
        return mapper.getExternalId(uri);
      }
    });
  }

  //@Override
  public void sync() throws IOException {
    connection.sync();
  }

  //@Override
  public void close() {
    super.close();
    connection.close();
  }
}
