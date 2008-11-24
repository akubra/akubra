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

import java.net.URI;

import java.text.SimpleDateFormat;

import java.util.Date;
import java.util.Map;

/**
 * Allocates a shallow hierarchy of unique filesystem paths, based on
 * the current date and a constructor-provided {@link FilenameAllocator}.
 * <p>
 * A typical path looks like 2008/0304/1015/allocated-filename
 * <p>
 * Note: This implementation only uses hints for the filename part of the
 * path allocation, and only if the associated <code>FilenameAllocator</code>
 * uses them.
 *
 * @author Chris Wilper
 */
public class DefaultPathAllocator implements PathAllocator {
  private final FilenameAllocator fAlloc;

  /**
   * Creates an instance that uses the DefaultFilenameAllocator.
   */
  public DefaultPathAllocator() {
      this.fAlloc = new DefaultFilenameAllocator();
  }

  /**
   * Creates an instance that uses the given FilenameAllocator.
   *
   * @param fAlloc the filename allocator to use.
   */
  public DefaultPathAllocator(FilenameAllocator fAlloc) {
      this.fAlloc = fAlloc;
  }

  //@Override
  public String allocate(URI blobId, Map<String, String> hints) {
    return getDir() + fAlloc.allocate(blobId, hints);
  }

  //@Override
  public URI getBlobId(String path) {
    int i = path.lastIndexOf("/");
    if (i == -1 || i == path.length() - 1) {
        return null;
    }
    return fAlloc.getBlobId(path.substring(i + 1));
  }

  private static String getDir() {
    SimpleDateFormat format = new SimpleDateFormat("yyyy/MMdd/HHmm/");
    return format.format(new Date());
  }
}
