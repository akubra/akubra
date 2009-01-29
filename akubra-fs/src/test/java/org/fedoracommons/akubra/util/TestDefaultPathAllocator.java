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
package org.fedoracommons.akubra.util;

import java.net.URI;
import java.net.URISyntaxException;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Unit tests for {@link DefaultPathAllocator}.
 *
 * @author Chris Wilper
 */
public class TestDefaultPathAllocator {
  private final DefaultPathAllocator allocator = new DefaultPathAllocator(new MockFilenameAllocator());

  /**
   * Allocations should have the form 2008/0304/1015/allocated-filename
   * and blobIds and hints should be passed down to the filename allocator.
   */
  @Test
  public void testAllocateWithHints() {
    Map<String, String> hints = new HashMap<String, String>();
    hints.put("extension", ".txt");
    URI blobId = uri("urn:example:foo");
    validate(allocator.allocate(blobId, hints), blobId.toString() + hints.get("extension"));
  }

  /**
   * Allocations without blobIds or hints should be supported.
   */
  @Test
  public void testAllocateWithoutBlobId() {
    validate(allocator.allocate(null, null), "nonenull");
  }

  /**
   * The path allocator should delegate to the filename allocator
   * when getting blob ids.
   */
  @Test
  public void testGetBlobId() {
    assertEquals(uri("urn:example:filename"),
                 allocator.getBlobId("2008/0304/1015/filename"));
  }

  private void validate(String path, String expectedFilename) {
    String[] parts = path.split("/");
    assertEquals(4, parts.length);
    for (int i = 0; i < 3; i++) {
      String part = parts[i];
      assertEquals(4, part.length());
      try {
        Integer.parseInt(part);
      } catch (NumberFormatException e) {
        fail("path part is non-numeric: " + part);
      }
    }
    assertEquals(expectedFilename, parts[3]);
  }

  private static URI uri(String uri) {
    try {
      return new URI(uri);
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  private class MockFilenameAllocator implements FilenameAllocator {
    public String allocate(URI blobId, Map<String, String> hints) {
      String id = "none";
      if (blobId != null) {
        id = blobId.toString();
      }
      String ext = "null";
      if (hints != null && hints.get("extension") != null)
        ext = hints.get("extension");
      return id + ext;
    }
    public URI getBlobId(String filename) {
      try {
        return new URI("urn:example:" + filename);
      } catch (URISyntaxException e) {
        throw new RuntimeException(e);
      }
    }
  }

}
