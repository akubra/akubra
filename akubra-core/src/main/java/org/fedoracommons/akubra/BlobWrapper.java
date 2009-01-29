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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import java.net.URI;

/**
 * Simple wrapper implementation that delegates all calls to the wrapped blob.
 * Subclass and override to customize.
 *
 * @author Ronald Tschalär
 */
public class BlobWrapper implements Blob {
  /** The wrapped blob to which all calls are delegated. */
  protected final Blob delegate;

  /**
   * Create a new BlobWrapper.
   *
   * @param delegate the blob to delegate the calls to.
   */
  protected BlobWrapper(Blob delegate) {
    this.delegate = delegate;
  }

  public BlobStoreConnection getConnection() {
    return delegate.getConnection();
  }

  public URI getId() {
    return delegate.getId();
  }

  public InputStream openInputStream() throws IOException {
    return delegate.openInputStream();
  }

  public OutputStream openOutputStream(long estimatedSize) throws IOException {
    return delegate.openOutputStream(estimatedSize);
  }

  public long getSize() {
    return delegate.getSize();
  }
}
