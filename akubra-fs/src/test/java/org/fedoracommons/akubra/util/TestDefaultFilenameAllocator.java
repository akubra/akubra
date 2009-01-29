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

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

/**
 * Unit tests for {@link DefaultFilenameAllocator}.
 *
 * @author Chris Wilper
 */
public class TestDefaultFilenameAllocator {
  private final DefaultFilenameAllocator allocator = new DefaultFilenameAllocator();

  /**
   * Allocations without blobId should start with _0 and increment.
   */
  @Test
  public void testAllocateWithoutBlobId() {
    assertEquals("_0", allocator.allocate(null, null));
    assertEquals("_1", allocator.allocate(null, null));
    assertEquals("_2", allocator.allocate(null, null));
  }

  /**
   * Encoding and decoding should be done in accordance with the
   * rules described in {@link DefaultFilenameAllocator}'s javadoc.
   */
  @Test
  public void testEncodingAndDecoding() {
    final String alphaLower = "abcdefghijklmnopqrstuvwxyz";
    final String alphaUpper = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    final String numbers = "0123456789";
    final String safeSymbols = "=()[]-";
    testEncDec(alphaLower, alphaLower);
    testEncDec(alphaUpper, alphaUpper);
    testEncDec(numbers, numbers);
    testEncDec(safeSymbols, safeSymbols);
    testEncDec(".a", ".a");
    testEncDec(".", "%");
    testEncDec("a.", "a%");
    testEncDec("foo_bar", "foo%5Fbar");
  }

  /**
   * Null should be returned when the given name doesn't encode a blobId.
   */
  @Test
  public void testGetBlobIdNotDecodable() {
    assertNull(allocator.getBlobId("a"));
    assertNull(allocator.getBlobId("_"));
    assertNull(allocator.getBlobId("_0"));
    assertNull(allocator.getBlobId("_0_"));
    assertNull(allocator.getBlobId("_0_a"));
  }

  private void testEncDec(String localName,
                          String encodedLocalName) {
    // test encoding
    String name = allocator.allocate(uri("urn:example:" + localName), null);
    int i = name.lastIndexOf("_");
    if (i == 0)
      fail("Expected two underscores in name: " + name);
    String encoded = "urn%3Aexample%3A" + encodedLocalName;
    String gotEncoded = name.substring(i + 1);
    assertEquals(encoded, gotEncoded);
    // test decoding
    URI uri = allocator.getBlobId("_0_" + encoded);
    assertNotNull(uri);
    assertEquals("urn:example:" + localName, uri.toString());
  }

  private static URI uri(String uri) {
    try {
      return new URI(uri);
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

}
