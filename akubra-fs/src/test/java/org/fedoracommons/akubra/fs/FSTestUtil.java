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
package org.fedoracommons.akubra.fs;

import java.io.File;

/**
 * Utilities for testing the fs implementation.
 *
 * @author Chris Wilper
 */
public abstract class FSTestUtil {

  // create new tmpDir in java.io.tmpdir
  public static File createTempDir() throws Exception {
    File tempFile = File.createTempFile("akubra-test", null);
    File tmpDir = new File(tempFile.getPath());
    tempFile.delete();
    tmpDir.mkdir();
    return tmpDir;
  }

  // create an empty file or dir with the given name in the given dir
  public static void add(File dir, String name) throws Exception {
    File file = new File(dir, name);
    if (name.endsWith("/")) {
      file.mkdir();
    } else {
      file.createNewFile();
    }
  }

  // rm -rf dir
  public static void rmdir(File dir) {
    for (File file : dir.listFiles()) {
      if (file.isDirectory()) {
        rmdir(file);
      } else {
        file.delete();
      }
    }
    dir.delete();
  }

}
