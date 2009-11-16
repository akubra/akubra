/* $HeadURL$
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

import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.FileUtils;

import org.testng.annotations.BeforeSuite;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.akubraproject.Blob;
import org.akubraproject.BlobStoreConnection;
import org.akubraproject.tck.TCKTestSuite;

/**
 * TCK test suite for {@link WWWStore}.
 *
 * @author Ronald Tschalär
 */
public class WWWTCKTest extends TCKTestSuite {
  private File baseDir;

  public WWWTCKTest() throws Exception {
    super(createStore(), getStoreId(), false, false, false, true, false, false, false);
  }

  private static URI getStoreId() throws Exception {
    return new URI("urn:www:tck-test");
  }

  private static WWWStore createStore() throws Exception {
    Map<String, URLStreamHandler> handlers = new HashMap<String, URLStreamHandler>();
    handlers.put("file", new FileURLStreamHandler());

    return new WWWStore(getStoreId(), handlers);
  }

  // Overridden tests

  @BeforeSuite
  public void init() throws IOException {
    File base = new File(System.getProperty("basedir"), "target");
    baseDir = new File(base, "tck-scratch");

    FileUtils.deleteDirectory(baseDir);
    baseDir.mkdirs();
  }

  // Overridden helpers

  /** @return file:///absolute/path/to/target/tck-scratch/name */
  @Override
  protected URI createId(String name) {
    return new File(baseDir, name).toURI();
  }

  /** @return file:///absolute/path/to/target/tck-scratch/name */
  @Override
  protected String getPrefixFor(String name) {
    return new File(baseDir, name).toURI().toString();
  }

  /** @return urn:foo */
  @Override
  protected URI getInvalidId() {
    return URI.create("urn:foo");
  }

  /** we can't determine aliases */
  protected URI[] getAliases(URI uri) {
    return null;
  }

  /** Blob.delete is not supported, so delete the file "manually" */
  @Override
  protected void deleteBlob(BlobStoreConnection con, Blob b) throws Exception {
    File f = new File(b.getId());
    if (!f.delete())
      throw new IOException("failed to delete '" + f + "'");

    ((WWWBlob) b).closed();     // hack to "clear the cache"

    assertFalse(b.exists());
    assertFalse(con.getBlob(b.getId(), null).exists());
  }

  /** con.listBlobIds is not supported, so list the files directly */
  @Override
  protected void assertNoBlobs(String prefix) throws Exception {
    assertEquals(listFiles(prefix).length, 0);
  }

  /** con.listBlobIds is not supported, so list the files directly */
  @Override
  protected void listBlobs(BlobStoreConnection con, String prefix, URI[] expected)
      throws Exception {
    Set<URI> exp = new HashSet<URI>(Arrays.asList(expected));
    URI id;

    for (String file : listFiles(prefix))
      assertTrue(exp.remove(id = createId(file)), "unexpected blob '" + id + "' found;");

    assertTrue(exp.isEmpty(), "expected blobs not found for prefix '" + prefix + "': " + exp + ";");
  }

  private String[] listFiles(String prefix) throws Exception {
    if (prefix == null || prefix.equals(""))
      return new String[0];

    final File f = new File(new URI(prefix));
    return f.getParentFile().list(new FilenameFilter() {
      public boolean accept(File dir, String name) {
        return name.startsWith(f.getName());
      }
    });
  }


  /**
   * This is to provide our own URLConnection for files which supports writing files (i.e.
   * URLConnection.getOutputStream()).
   */
  private static class FileURLStreamHandler extends URLStreamHandler {
    @Override
    protected URLConnection openConnection(URL url) throws IOException {
      return new FileURLConnection(url);
    }

    private static class FileURLConnection extends URLConnection {
      private final File file;

      FileURLConnection(URL url) throws IOException {
        super(url);
        try {
          file = new File(url.toURI());
        } catch (URISyntaxException use) {
          throw (IOException) new IOException("Failed to parse '" + url + "'").initCause(use);
        }
      }

      @Override
      public void connect() {
      }

      @Override
      public InputStream getInputStream() throws IOException {
        return new FileInputStream(file);
      }

      @Override
      public OutputStream getOutputStream() throws IOException {
        return new FileOutputStream(file);
      }
    }
  }
}
