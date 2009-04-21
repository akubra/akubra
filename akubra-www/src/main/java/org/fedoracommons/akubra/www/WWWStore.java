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
package org.fedoracommons.akubra.www;

import java.io.IOException;

import java.net.URI;
import java.net.URLStreamHandler;

import java.util.Map;

import javax.transaction.Transaction;

import org.fedoracommons.akubra.BlobStoreConnection;
import org.fedoracommons.akubra.impl.AbstractBlobStore;

/**
 * A store that represents the World Wide Web. Blob ids are URLs. As expected, the store is
 * non-transactional. Also the iterator or deletion or rename are all meaningless operations.
 * Neither can you make the store quiescent for backups.
 *
 * @author Pradeep Krishnan
 */
public class WWWStore extends AbstractBlobStore {
  private final Map<String, URLStreamHandler> handlers;

  /**
   * Creates a new WWWStore object.
   *
   * @param id an identifier for this store
   */
  public WWWStore(URI id) {
    this(id, null);
  }

  /**
   * Creates a new WWWStore object.
   *
   * @param id       an identifier for this store
   * @param handlers the url stream-handlers (keyed by uri scheme) to use; if a handler is not
   *                 found then the java default one is used. May be null.
   */
  public WWWStore(URI id, Map<String, URLStreamHandler> handlers) {
    super(id);
    this.handlers = handlers;
  }

  public BlobStoreConnection openConnection(Transaction tx)
                                     throws UnsupportedOperationException, IOException {
    if (tx != null)
      throw new UnsupportedOperationException("Transactions not supported");

    return new WWWConnection(this, handlers);
  }

  public boolean setQuiescent(boolean quiescent) throws IOException {
    return false;
  }
}
