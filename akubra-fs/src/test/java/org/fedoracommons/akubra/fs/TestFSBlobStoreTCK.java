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
import java.net.URI;

import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;

import org.fedoracommons.akubra.tck.TCKTestSuite;

/**
 * TCK test suite for {@link FSBlobStore}.
 *
 * @author Ronald Tschalär
 */
public class TestFSBlobStoreTCK extends TCKTestSuite {
  private static File baseDir;

  public TestFSBlobStoreTCK() throws Exception {
    super(getStore(), getStoreId(), false, true);
  }

  private static URI getStoreId() throws Exception {
    return new URI("urn:example:store");
  }

  private static FSBlobStore getStore() throws Exception {
    baseDir = FSTestUtil.createTempDir();
    return new FSBlobStore(getStoreId(), baseDir);
  }

  @AfterSuite
  public void destroy() {
    FSTestUtil.rmdir(baseDir);
  }

  protected URI createId(String name) {
    return URI.create("file:" + name);
  }

  protected String getPrefixFor(String name) {
    return "file:" + name;
  }

  protected URI getInvalidId() {
    return URI.create("urn:foo");
  }
}
