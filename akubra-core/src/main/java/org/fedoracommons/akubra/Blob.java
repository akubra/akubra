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
package org.fedoracommons.akubra;

import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Interface to abstract the idea of a blob in the blob store.
 *
 * @author Chris Wilper
 * @author Pradeep Krishnan
 * @author Ronald Tschalär
 */
public interface Blob {
  /**
   * This method returns an InputStream representing the data and throws the appropriate exception
   * if it can not do so.
   *
   * @return the input stream.
   */
  InputStream getInputStream() throws IOException;

  /**
   * This method returns an OutputStream where the data can be written and throws the appropriate
   * exception if it can not do so.
   *
   * @return the output stream.
   */
  OutputStream getOutputStream() throws IOException;

  /**
   * Gets the size of the blob, in bytes.
   *
   * @return the size in bytes, or -1 if unknown.
   */
  long getSize();
}
