/* $HeadURL::                                                                            $
 * $Id$
 *
 * Copyright (c) 2009 DuraSpace
 * http://duraspace.org
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
package org.akubraproject.www;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.akubraproject.Blob;
import org.akubraproject.BlobStoreConnection;
import org.akubraproject.DuplicateBlobException;
import org.akubraproject.MissingBlobException;
import org.akubraproject.impl.AbstractBlob;
import org.akubraproject.impl.StreamManager;

/**
 * A WWW resource as a Blob.
 *
 * @author Pradeep Krishnan
 */
class WWWBlob extends AbstractBlob {
  private static final Log logger = LogFactory.getLog(WWWBlob.class);

  private final URL           url;
  private final StreamManager streamManager;
  private Long                size;
  private Boolean             exists;
  private URLConnection       urlc;
  private InputStream         content;

  /**
   * Creates a new WWWBlob object.
   *
   * @param url the www url
   * @param conn the connection object
   * @param streamManager the stream-manager
   */
  public WWWBlob(URL url, BlobStoreConnection conn, StreamManager streamManager) {
    super(conn, toURI(url));
    this.url = url;
    this.streamManager = streamManager;
  }

  private static URI toURI(URL url) {
    try {
      return url.toURI();
    } catch (URISyntaxException e) {
      throw new Error("unexpected exception", e);
    }
  }

  /**
   * Notification that the BlobStoreConnection is closed.
   */
  void closed() {
    urlc   = null;
    exists = null;
    size   = null;

    if (content != null) {
      try {
        content.close();
      } catch (IOException ioe) {
        logger.warn("Error closing input-stream for '" + id + "'", ioe);
      }
    }
  }

  private URLConnection connect(boolean input, boolean cache) throws IOException {
    ensureOpen();

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
      try {
        content = streamManager.manageInputStream(owner, con.getInputStream());
        exists = true;
      } catch (FileNotFoundException fnfe) {
        logger.debug("blob doesn't exist for '" + id + "'", fnfe);
        exists = false;
        size   = null;
        urlc   = null;
        throw new MissingBlobException(id);
      }

      size = (long) con.getContentLength();

      /*
       * close() on the InputStream will disconnect.
       * So the connection should not be cached in that case.
       * For getSize(), the caching the connection is a valid option.
       */
      urlc = cache ? con : null;
    }

    return con;
  }

  @Override
  public long getSize() throws IOException {
    if (exists != null && !exists)
      throw new MissingBlobException(id);
    if (size == null)
      connect(true, true);

    return size;
  }

  @Override
  public InputStream openInputStream() throws IOException {
    connect(true, false);

    return content;
  }

  @Override
  public OutputStream openOutputStream(long estimatedSize, boolean overwrite) throws IOException {
    if (!overwrite && exists())
      throw new DuplicateBlobException(id);

    URLConnection con = connect(false, false);

    OutputStream os = streamManager.manageOutputStream(owner, con.getOutputStream());
    exists = true;
    return os;
  }

  @Override
  public boolean exists() throws IOException {
    if (exists == null) {
      try {
        connect(true, true);
      } catch (MissingBlobException mbe) {
        logger.trace("blob doesn't exist for '" + id + "'", mbe);
        return false;
      }
    }
    return exists;
  }

  @Override
  public void delete() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Blob moveTo(URI blobId, Map<String, String> hints) throws IOException {
    throw new UnsupportedOperationException();
  }
}
