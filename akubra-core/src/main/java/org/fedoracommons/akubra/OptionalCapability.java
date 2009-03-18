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
 * A base class for capabilities that can be switched on/off (enabled/disabled).
 *
 * @author Ronald Tschalär
 */
public abstract class OptionalCapability implements Capability {
  /** the id of this capability */
  protected final URI id;

  /**
   * the enabled state of this capability - subclasses should set this appropriately
   * in their implementation of <code>setEnabled</code>.
   */
  protected boolean enabled;

  /**
   * Create a new optional capability with the given id and intial enabled state.
   *
   * @param id      the capability's id
   * @param enabled the initial enabled state
   */
  protected OptionalCapability(URI id, boolean enabled) {
    this.id      = id;
    this.enabled = enabled;
  }

  public URI getId() {
    return id;
  }

  /**
   * @return true
   */
  public boolean isOptional() {
    return true;
  }

  public boolean isEnabled() {
    return enabled;
  }
}
