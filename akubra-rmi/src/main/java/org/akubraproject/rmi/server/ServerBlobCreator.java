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
package org.akubraproject.rmi.server;

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
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.akubraproject.BlobStoreConnection;
import org.akubraproject.rmi.remote.PartialBuffer;
import org.akubraproject.rmi.remote.RemoteBlob;
import org.akubraproject.rmi.remote.RemoteBlobCreator;

/**
 * Server side implementation of RemoteBlobCreator.
 *
 * @author Pradeep Krishnan
 */
public class ServerBlobCreator extends UnicastExportable implements RemoteBlobCreator {
  private static final Log         log              = LogFactory.getLog(ServerBlobCreator.class);
  private static final long        serialVersionUID = 1L;
  private final ExecutorService    readerService;
  private final ExecutorService    writerService;
  private final Future<RemoteBlob> reader;
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

    final InputStream in = new PipedInputStream(out);
    readerService =
      Executors.newSingleThreadExecutor(new ThreadFactory() {
          public Thread newThread(Runnable r) {
            Thread t = new Thread(r, "akubra-rmi-blob-creator");
            t.setDaemon(true);

            return t;
          }
        });

    reader =
      readerService.submit(new Callable<RemoteBlob>() {
          public RemoteBlob call() throws Exception {
            if (log.isDebugEnabled())
              log.debug("Started blob creator");

            return new ServerBlob(con.getBlob(in, estimatedSize, hints), getExporter());
          }
        });

    /*
     * Note: there needs to be a single thread that writes to PipedOutputStream.
     * See PipedOutputStream and PipedInputStream source. So all rmi calls for the
     * stream are scheduled on this single-threaded execution service.
     */
    writerService =
      Executors.newSingleThreadExecutor(new ThreadFactory() {
          public Thread newThread(Runnable r) {
            Thread t = new Thread(r, "akubra-rmi-blob-writer");
            t.setDaemon(true);

            return t;
          }
        });

    if (log.isDebugEnabled())
      log.debug("Server blob creator is ready");
  }

  private RemoteBlob getBlob() throws IOException {
    try {
      if (log.isDebugEnabled())
        log.debug("Waiting for Server blob ...");

      return reader.get();
    } catch (InterruptedException e) {
      throw new IOException("Interrupted while waiting for reader", e);
    } catch (ExecutionException e) {
      Throwable t = e.getCause();

      if (t instanceof IOException)
        throw (IOException) t;

      if (t instanceof RuntimeException)
        throw (RuntimeException) t;

      throw new IOException("Unexpected exception in reader", t);
    }
  }

  public RemoteBlob shutDown(boolean abort) throws IOException {
    if (log.isDebugEnabled())
      log.debug(abort ? "Aborting server blob creator" : "Shuting down server blob creator");

    unExport(abort);

    if (abort)
      reader.cancel(abort);

    RemoteBlob rb = abort ? null : getBlob();

    readerService.shutdownNow();
    writerService.shutdownNow();

    if (abort) {
      /*
       * Wait for termination so that the 'cancel' above can do the
       * necessary cleanup.
       */
      try {
        if (!readerService.awaitTermination(5, TimeUnit.SECONDS))
          throw new IOException("Failed to terminate reader service");
      } catch (InterruptedException e) {
        throw new IOException("Interrupted while awaiting termination of reader", e);
      }
    }

    return rb;
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
    try {
      shutDown(true);
    } catch (IOException e) {
      log.warn("Error during abort", e);
    }
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
      throw new IOException("Interrupted while waiting for writer", e);
    } catch (ExecutionException e) {
      Throwable t = e.getCause();

      if (t instanceof IOException)
        throw (IOException) t;

      if (t instanceof RuntimeException)
        throw (RuntimeException) t;

      throw new IOException("Unexpected exception in writer", t);
    }
  }
}
