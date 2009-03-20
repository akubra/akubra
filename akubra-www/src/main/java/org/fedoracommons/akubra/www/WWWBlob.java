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
import java.io.InputStream;
import java.io.OutputStream;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;

import org.fedoracommons.akubra.Blob;
import org.fedoracommons.akubra.BlobStoreConnection;

/**
 * A WWW resource as a Blob.
 *
 * @author Pradeep Krishnan
 */
class WWWBlob implements Blob {
  private URL                 url;
  private BlobStoreConnection conn;
  private Long                size;
  private URLConnection       urlc;

  /**
   * Creates a new WWWBlob object.
   *
   * @param url the www url
   * @param conn the connection object
   */
  public WWWBlob(URL url, BlobStoreConnection conn) {
    this.url    = url;
    this.conn   = conn;
  }

  public BlobStoreConnection getConnection() {
    return conn;
  }

  /**
   * Notification that the BlobStoreConnection is closed.
   */
  void closed() {
    conn   = null;
    urlc   = null;
  }

  public URI getId() {
    try {
      return url.toURI();
    } catch (URISyntaxException e) {
      throw new Error("unexpected exception", e);
    }
  }

  private URLConnection connect(boolean input, boolean cache)
                         throws IOException {
    if (conn == null)
      throw new IOException("Connection closed.");

    URLConnection con;

    if ((urlc != null) && input)
      con = urlc;
    else {
      con = url.openConnection();
      con.setAllowUserInteraction(false);

      if (input)
        con.setDoInput(true);
      else
        con.setDoOutput(true);
    }

    if (input) {
      size   = (long) con.getContentLength();

      /*
       * close() on the InputStream will disconnect.
       * So the connection should not be cached in that case.
       * For getSize(), the caching the connection is a valid option.
       */
      urlc   = cache ? con : null;
    }

    return con;
  }

  public long getSize() throws IOException {
    if (size == null)
      connect(true, true);

    return size;
  }

  public InputStream openInputStream() throws IOException {
    URLConnection con = connect(true, false);

    return con.getInputStream();
  }

  public OutputStream openOutputStream(long estimatedSize)
                                throws IOException {
    URLConnection con = connect(false, false);

    return con.getOutputStream();
  }

  public boolean exists() throws IOException {
    return true;
  }

  public void create() throws IOException {
    throw new UnsupportedOperationException();
  }

  public void delete() throws IOException {
    throw new UnsupportedOperationException();
  }

  public void moveTo(Blob blob) throws IOException {
    throw new UnsupportedOperationException();
  }
}
