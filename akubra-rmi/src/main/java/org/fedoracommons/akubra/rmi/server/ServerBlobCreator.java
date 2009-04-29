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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;

import org.fedoracommons.akubra.BlobStoreConnection;
import org.fedoracommons.akubra.rmi.remote.PartialBuffer;
import org.fedoracommons.akubra.rmi.remote.RemoteBlob;
import org.fedoracommons.akubra.rmi.remote.RemoteBlobCreator;

/**
 * Server side implementation of RemoteBlobCreator.
 *
 * @author Pradeep Krishnan
 */
public class ServerBlobCreator extends UnicastExportable implements RemoteBlobCreator {
  private static final long        serialVersionUID = 1L;
  private final Future<RemoteBlob> reader;
  private final ExecutorService    writerService;
  private final PipedOutputStream  out;

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
    super(exporter);
    out = new PipedOutputStream();

    final InputStream in       = new PipedInputStream(out);
    ExecutorService   executor =
      Executors.newSingleThreadExecutor(new ThreadFactory() {
          public Thread newThread(Runnable r) {
            Thread t = new Thread(r, "akubra-rmi-blob-creator");
            t.setDaemon(true);

            return t;
          }
        });

    reader =
      executor.submit(new Callable<RemoteBlob>() {
          public RemoteBlob call() throws Exception {
            return new ServerBlob(con.getBlob(in, estimatedSize, hints), getExporter());
          }
        });

    writerService =
      Executors.newSingleThreadExecutor(new ThreadFactory() {
          public Thread newThread(Runnable r) {
            Thread t = new Thread(r, "akubra-rmi-blob-writer");
            t.setDaemon(true);

            return t;
          }
        });
  }

  public RemoteBlob getBlob() throws IOException {
    unExport(false);

    try {
      return reader.get();
    } catch (InterruptedException e) {
      throw (IOException) new IOException("Interrupted while waiting for reader").initCause(e);
    } catch (ExecutionException e) {
      Throwable t = e.getCause();

      if (t instanceof IOException)
        throw (IOException) t;

      if (t instanceof RuntimeException)
        throw (RuntimeException) t;

      throw (IOException) new IOException("Unexpected exception in reader").initCause(t);
    }
  }

  public void close() throws IOException {
    execute(new Callable<Void>() {
        public Void call() throws Exception {
          out.close();

          return null;
        }
      });
  }

  @Override
  public void unreferenced() {
    if (!reader.isDone())
      reader.cancel(true);

    writerService.shutdownNow();

    unExport(true);
  }

  public void flush() throws IOException {
    execute(new Callable<Void>() {
        public Void call() throws Exception {
          out.flush();

          return null;
        }
      });
  }

  public void write(final byte[] b) throws IOException {
    execute(new Callable<Void>() {
        public Void call() throws Exception {
          out.write(b);

          return null;
        }
      });
  }

  public void write(final int b) throws IOException {
    execute(new Callable<Void>() {
        public Void call() throws Exception {
          out.write(b);

          return null;
        }
      });
  }

  public void write(final PartialBuffer b) throws IOException {
    execute(new Callable<Void>() {
        public Void call() throws Exception {
          out.write(b.getBuffer(), b.getOffset(), b.getLength());

          return null;
        }
      });
  }

  private <T> T execute(Callable<T> action) throws IOException {
    try {
      return writerService.submit(action).get();
    } catch (InterruptedException e) {
      throw (IOException) new IOException("Interrupted while waiting for writer").initCause(e);
    } catch (ExecutionException e) {
      Throwable t = e.getCause();

      if (t instanceof IOException)
        throw (IOException) t;

      if (t instanceof RuntimeException)
        throw (RuntimeException) t;

      throw (IOException) new IOException("Unexpected exception in writer").initCause(t);
    }
  }
}
