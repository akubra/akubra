/* $HeadURL::                                                                            $
 * $Id$
 *
 * Copyright (c) 2009 DuraSpace
 * http://duraspace.org
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
package org.akubraproject.rmi.remote;

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
   * Shuts down this blob-creator and gets the Blob that was created. Note that if this is not
   * an abortive shutdown, this call will block till {@link RemoteOutputStream#close()} has been
   * called. Also note that the server side will un-export this object at the end of this call.
   *
   * @param abort if set to true, all attempts are made to abort the blob creation
   *
   * @return the remote blob handle
   *
   * @throws RemoteException on an error in rmi transport
   * @throws IOException error reported by the remote server
   */
  RemoteBlob shutDown(boolean abort) throws RemoteException, IOException;
}
