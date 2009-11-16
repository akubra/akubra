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

package org.akubraproject.qsc;

import java.io.OutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.Map;

import org.akubraproject.Blob;
import org.akubraproject.BlobStoreConnection;
import org.akubraproject.impl.BlobWrapper;
import org.akubraproject.impl.StreamManager;

/**
 * Wraps an existing {@link Blob} to be able to track open output-streams.
 *
 * @author Ronald Tschalär
 */
class QuiescingBlob extends BlobWrapper {
  private final StreamManager streamManager;

  /**
   * Create a new quiescing blob.
   *
   * @param connection    the connection this blob belongs to
   * @param blob          the underlying blob this is wrapping
   * @param streamManager the stream manager.
   */
  public QuiescingBlob(BlobStoreConnection connection, Blob blob, StreamManager streamManager) {
    super(blob, connection);
    this.streamManager = streamManager;
  }

  @Override
  public OutputStream openOutputStream(long estimSize, boolean overwrite) throws IOException {
    ((QuiescingBlobStoreConnection) owner).waitUnquiescedAndMarkModified();
    return streamManager.manageOutputStream(owner, super.openOutputStream(estimSize, overwrite));
  }

  @Override
  public void delete() throws IOException {
    ((QuiescingBlobStoreConnection) owner).waitUnquiescedAndMarkModified();
    super.delete();
  }

  @Override
  public Blob moveTo(URI blobId, Map<String, String> hints) throws IOException {
    ((QuiescingBlobStoreConnection) owner).waitUnquiescedAndMarkModified();
    return super.moveTo(blobId, hints);
  }
}
