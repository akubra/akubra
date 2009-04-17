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
package org.fedoracommons.akubra.rmi.remote;

import java.io.IOException;

import java.rmi.RemoteException;

/**
 * An interface that lets clients write to an output stream on a remote server
 * to create a new blob.
 *
 * @author Pradeep Krishnan
 */
public interface RemoteBlobCreator extends RemoteOutputStream {
  /**
   * Gets the Blob that was created. Note that this will block till {@link
   * RemoteOutputStream#close()} has been called. Also note that the server side may un-export
   * this object at the end of this call.
   *
   * @return the remote blob handle
   *
   * @throws RemoteException on an error in rmi transport
   * @throws IOException error reported by the remote server
   */
  RemoteBlob getBlob() throws RemoteException, IOException;
}
