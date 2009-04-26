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
package org.fedoracommons.akubra.map;

import java.net.URI;

/**
 * Simple {@link IdMapper} implementation for testing. Internal ids are
 * the same as external ids, optionally prepended with a given
 * <code>uriPrefix</code>.
 *
 * @author Chris Wilper
 */
public class MockIdMapper implements IdMapper {
  private final String uriPrefix;

  public MockIdMapper() {
    this(null);
  }

  public MockIdMapper(String uriPrefix) {
    if (uriPrefix == null)
      this.uriPrefix = "";
    else
      this.uriPrefix = uriPrefix;
  }

  public URI getExternalId(URI internalId) throws NullPointerException {
    if (internalId == null)
      throw new NullPointerException();
    if (!internalId.toString().startsWith(uriPrefix))
      throw new IllegalArgumentException();
    return URI.create(internalId.toString().substring(uriPrefix.length()));
  }

  public URI getInternalId(URI externalId) throws NullPointerException {
    if (externalId == null)
      throw new NullPointerException();
    return URI.create(uriPrefix + externalId);
  }

}
