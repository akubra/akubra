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
package org.fedoracommons.akubra.util;

import java.io.UnsupportedEncodingException;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.net.URLEncoder;

import java.util.Map;

/**
 * Allocates unique filenames based on a modern-filesystem-safe encoding of
 * the given blobId (if provided) and a rolling 31-bit counter.
 * <p>
 * A typical filename looks like _42_encoded-blob-id
 * <p>
 * Each character in a blobId URI is UTF-8 percent-encoded ("URI escaped")
 * except for the following: <code>a-z A-Z 0-9 = ( ) [ ] -</code>
 * In addition, <code>.</code> (period) is escaped as <code>%</code> when
 * it occurs as the last character of the URI.
 * <p>
 * Note: This implementation does not make use of hints.
 *
 * @author Chris Wilper
 */
public class DefaultFilenameAllocator implements FilenameAllocator {
  private int fileNumber;

  public DefaultFilenameAllocator() {
  }

  //@Override
  public String allocate(URI blobId, Map<String, String> hints) {
      StringBuilder filename = new StringBuilder();
      filename.append('_');
      filename.append(getNextFileNumber());
      if (blobId != null) {
          filename.append('_');
          filename.append(encode(blobId.toString()));
      }
      return filename.toString();
  }

  //@Override
  public URI getBlobId(String filename) {
    if (filename.startsWith("_")) {
      // must be of the form _nnn_encodedBlobId or _nnn
      int i = filename.lastIndexOf("_");
      if (i == 0) {
        return null;  // does not encode blobID
      } else {
        return getAbsoluteURI(decodeBlobId(filename.substring(i + 1)));
      }
    }
    return null;
  }

  private synchronized int getNextFileNumber() {
    try {
      return fileNumber;
    } finally {
      if (fileNumber == Integer.MAX_VALUE) {
        fileNumber = 0;
      } else {
        fileNumber++;
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

  private static URI getAbsoluteURI(String s) {
    try {
      URI uri = new URI(s);
      if (uri.isAbsolute()) {
        return uri;
      }
    } catch (URISyntaxException e) {
    }
    return null;
  }
}
