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
package org.fedoracommons.akubra.rmi.server;

import java.rmi.RemoteException;
import java.rmi.server.Unreferenced;

/**
 * A base class for all objects that are exported for use by a single client.
 *
 * @author Pradeep Krishnan
 */
public abstract class UnicastExportable extends Exportable implements Unreferenced {
  private static final long serialVersionUID = 1L;

  /**
   * Creates a new UnicastExportable object.
   *
   * @param exporter the exporter to use
   *
   * @throws RemoteException on an export error
   */
  protected UnicastExportable(Exporter exporter) throws RemoteException {
    super(exporter);
  }

  public void unreferenced() {
    unExport(false);
  }
}
