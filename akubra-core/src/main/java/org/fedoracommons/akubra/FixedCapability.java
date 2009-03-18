/* $HeadURL::                                                                            $
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

package org.fedoracommons.akubra;

import java.net.URI;

/**
 * A capability that cannot be switched off (disabled).
 *
 * @author Ronald Tschalär
 */
public class FixedCapability implements Capability {
  /** the id of this capability */
  protected final URI id;

  /**
   * Create a new capability with the given id.
   *
   * @param id the capability's id
   */
  public FixedCapability(URI id) {
    this.id = id;
  }

  public URI getId() {
    return id;
  }

  /**
   * @return false (always enabled)
   */
  public boolean isOptional() {
    return false;
  }

  /**
   * @return true (always enabled)
   */
  public boolean isEnabled() {
    return true;
  }

  /**
   * @throws UnsupportedOperationException if <var>enabled</var> is false
   */
  public void setEnabled(boolean enabled) throws UnsupportedOperationException {
    if (!enabled)
      throw new UnsupportedOperationException("Cannot disable capability '" + id + "'");
  }
}
