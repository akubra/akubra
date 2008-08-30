/* $HeadURL::                                                                            $
 * $Id$
 *
 * Copyright (c) 2007-2008 by Fedora Commons Inc.
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

import java.io.UnsupportedEncodingException;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.net.URLEncoder;

/**
 * Allocates unique filenames based on a modern-filesystem-safe encoding of
 * the given blobId (if provided) and a rolling 31-bit counter.
 * 
 * A typical filename looks like _42_encoded-blob-id
 * 
 * Each character in a blobId URI is UTF-8 percent-encoded ("URI escaped")
 * except for the following: <code>a-z A-Z 0-9 = ( ) [ ] -</code>
 * In addition, <code>.</code> (period) is escaped as <code>%</code> when
 * it occurs as the last character of the URI.
 * 
 * @author Chris Wilper
 */
public class DefaultFilenameAllocator implements FilenameAllocator {

  private int m_fileNumber;

  public DefaultFilenameAllocator() {
  }

  /**
   * {@inheritDoc}
   */
  public String allocate(URI blobId) {
      StringBuilder filename = new StringBuilder();
      filename.append('_');
      filename.append(getNextFileNumber());
      if (blobId != null) {
          filename.append('_');
          filename.append(encode(blobId.toString()));
      }
      return filename.toString();
  }

  /**
   * {@inheritDoc}
   */
  public URI getBlobId(String filename) {
    if (filename.startsWith("_")) {
      // must be of the form _nnn_newEncoding or _nnn
      int i = filename.lastIndexOf("_");
      if (i == 0) {
        return null;  // does not encode blobID
      } else {
        return toURI(decodeBlobId(filename.substring(i + 1)));
      }
    } else {
      // must be of the form oldEncoding
      return toURI(decodeLegacyBlobId(filename));
    }
  }

  private synchronized int getNextFileNumber() {
    try {
      return m_fileNumber;
    } finally {
      if (m_fileNumber == Integer.MAX_VALUE) {
        m_fileNumber = 0;
      } else {
        m_fileNumber++;
      }
    }
  }

  private static String encode(String uri) {
    // encode char-by-char because we only want to borrow
    // URLEncoder.encode's behavior for some characters
    StringBuilder out = new StringBuilder();
    for (int i = 0; i < uri.length(); i++) {
        char c = uri.charAt(i);
        if (c >= 'a' && c <= 'z') {
            out.append(c);
        } else if (c >= '0' && c <= '9') {
            out.append(c);
        } else if (c >= 'A' && c <= 'Z') {
            out.append(c);
        } else if (c == '-' || c == '=' || c == '(' || c == ')'
                || c == '[' || c == ']' || c == ';' || c == '.') {
            out.append(c);
        } else if (c == ':') {
            out.append("%3A");
        } else if (c == ' ') {
            out.append("%20");
        } else if (c == '+') {
            out.append("%2B");
        } else if (c == '_') {
            out.append("%5F");
        } else if (c == '*') {
            out.append("%2A");
        } else {
            out.append(uriEncode(c));
        }
    }
    String encoded = out.toString();
    if (encoded.endsWith(".")) {
        encoded = encoded.substring(0, encoded.length() - 1) + "%";
    }
    return encoded;
  }

  private static String decodeBlobId(String encoded) {
    String toDecode = encoded;
    if (toDecode.endsWith("%")) {
      toDecode = toDecode.substring(0, toDecode.length() - 1) + ".";
    }
    return uriDecode(toDecode);
  }

  private static String decodeLegacyBlobId(String encoded) {
    // Compatible with Fedora 3.0 and below's LLStore, which did little encoding
    String decoded = encoded.replaceFirst("_", ":");
    if (decoded.endsWith("%")) {
      return decoded.substring(0, decoded.length() - 1) + ".";
    } else {
      return decoded;
    }
  }

  private static String uriEncode(char c) {
    try {
      return URLEncoder.encode("" + c, "UTF-8");
    } catch (UnsupportedEncodingException wontHappen) {
      throw new RuntimeException(wontHappen);
    }
  }

  private static String uriDecode(String s) {
    try {
      return URLDecoder.decode(s, "UTF-8");
    } catch (UnsupportedEncodingException wontHappen) {
      throw new RuntimeException(wontHappen);
    }
  }

  private static URI toURI(String s) {
    try {
      return new URI(s);
    } catch (URISyntaxException e) {
      return null;
    }
  }
}
