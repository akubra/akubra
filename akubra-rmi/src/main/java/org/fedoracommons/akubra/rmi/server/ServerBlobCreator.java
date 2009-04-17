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
package org.fedoracommons.akubra.rmi.server;

import java.io.IOException;
import java.io.InputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;

import org.fedoracommons.akubra.BlobStoreConnection;
import org.fedoracommons.akubra.rmi.remote.RemoteBlob;
import org.fedoracommons.akubra.rmi.remote.RemoteBlobCreator;

/**
 * Server side implementation of RemoteBlobCreator.
 *
 * @author Pradeep Krishnan
 */
public class ServerBlobCreator extends ServerOutputStream implements RemoteBlobCreator {
  private static final long        serialVersionUID = 1L;
  private final Future<RemoteBlob> blob;

  /**
   * Creates a new ServerBlobCreator object.
   *
   * @param con the server side blob store connection
   * @param estimatedSize the size estimate on the new blob from client
   * @param hints the blob creation hints from client
   * @param exporter the exporter to use
   *
   * @throws IOException on an error in creation
   */
  public ServerBlobCreator(final BlobStoreConnection con, final long estimatedSize,
                           final Map<String, String> hints, Exporter exporter)
                    throws IOException {
    super(new PipedOutputStream(), exporter);

    final InputStream in = new PipedInputStream((PipedOutputStream) out, 8192);

    blob =
      Executors.newSingleThreadExecutor(new ThreadFactory() {
          public Thread newThread(Runnable r) {
            Thread t = new Thread(r, "akubra-rmi-blob-creator");
            t.setDaemon(true);

            return t;
          }
        }).submit(new Callable<RemoteBlob>() {
          public RemoteBlob call() throws Exception {
            return new ServerBlob(con.getBlob(in, estimatedSize, hints), getExporter());
          }
        });
  }

  public RemoteBlob getBlob() throws IOException {
    unExport(false);

    try {
      return blob.get();
    } catch (InterruptedException e) {
      throw (IOException) new IOException("Interrupted while waiting for blob").initCause(e);
    } catch (ExecutionException e) {
      Throwable t = e.getCause();

      if (t instanceof IOException)
        throw (IOException) t;

      if (t instanceof RuntimeException)
        throw (RuntimeException) t;

      throw (IOException) new IOException("Unexpected exception in create-blob").initCause(t);
    }
  }

  /**
   * Overrides from ServerOutputStream so that this is not unexported.
   *
   * @throws IOException on an error in stream close
   */
  @Override
  public void close() throws IOException {
    out.close();
  }

  /**
   * Overrides from ServerOutputStream so that the worker thread is aborted.
   */
  @Override
  public void unreferenced() {
    if (!blob.isDone())
      blob.cancel(true);

    unExport(true);
  }
}
