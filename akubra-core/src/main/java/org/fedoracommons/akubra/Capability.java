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
 * Interface to abstract the idea of capabilities of a blob store
 *
 * @author Chris Wilper
 * @author Pradeep Krishnan
 * @author Ronald Tschalär
 */
public interface Capability {
  /**
   * Return the identifier associated with the capability.
   *
   * @return the URI uniquely identifying the capability
   */
  URI getId();

  /**
   * Returns 'true' if the capability can be switched on/off (enabled/disabled).
   *
   * Note: that an implementation can start out having this capability as optional, but once some
   * blobs are stored or based on other run time situations, it is entirely possible that this
   * capability effectively becomes non-optional (as reflected in the two exceptions declared in
   * the {@link #setEnabled setEnabled} call).
   *
   * @return boolean if the capability can be enabled and disabled
   */
  boolean isOptional();

  /**
   * Returns the current enabled status of the capability.
   *
   * @return boolean indicating whether the capability is currently enabled (true) or not (false)
   */
  boolean isEnabled();

  /**
   * Turn this capability on/off.
   *
   * @param enabled true to turn on, else off
   *
   * @exception IllegalStateException invalid state of store to try changing state
   * @exception UnsupportedOperationException capability does not allow enabling/disabling
   */
  void setEnabled(boolean enabled) throws IllegalStateException, UnsupportedOperationException;
}
