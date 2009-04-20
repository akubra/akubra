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
package org.fedoracommons.akubra.fs;

import java.io.File;
import java.io.IOException;

import java.net.URI;
import java.net.URISyntaxException;

import java.util.Iterator;
import java.util.Map;

import org.fedoracommons.akubra.Blob;
import org.fedoracommons.akubra.BlobStore;
import org.fedoracommons.akubra.impl.AbstractBlobStoreConnection;
import org.fedoracommons.akubra.impl.StreamManager;
import org.fedoracommons.akubra.util.PathAllocator;

/**
 * Filesystem-backed BlobStoreConnection implementation.
 *
 * @author Chris Wilper
 */
class FSBlobStoreConnection extends AbstractBlobStoreConnection {
  private final File baseDir;
  private final PathAllocator pAlloc;

  FSBlobStoreConnection(BlobStore blobStore, File baseDir, PathAllocator pAlloc,
                        StreamManager manager) {
    super(blobStore, manager);
    this.baseDir = baseDir;
    this.pAlloc = pAlloc;
  }


  //@Override
  public Blob getBlob(URI blobId, Map<String, String> hints) throws IOException {
    if (isClosed())
      throw new IllegalStateException("Connection closed.");

    if (blobId == null) {
      String path = pAlloc.allocate(null, null);
      try {
        blobId = new URI(FSBlob.scheme + ":" + path);
      } catch (URISyntaxException wontHappen) {
        throw new Error(wontHappen);
      }
    }

    return new FSBlob(this, baseDir, blobId, streamManager);
  }

  //@Override
  public Iterator<URI> listBlobIds(String filterPrefix) {
    if (isClosed())
      throw new IllegalStateException("Connection closed.");

    return new FSBlobIdIterator(baseDir, filterPrefix);
  }

}
