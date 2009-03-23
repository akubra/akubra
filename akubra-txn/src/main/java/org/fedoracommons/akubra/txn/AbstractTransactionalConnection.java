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

package org.fedoracommons.akubra.txn;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.transaction.Status;
import javax.transaction.Synchronization;
import javax.transaction.Transaction;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.fedoracommons.akubra.AbstractBlob;
import org.fedoracommons.akubra.AbstractBlobStoreConnection;
import org.fedoracommons.akubra.Blob;
import org.fedoracommons.akubra.BlobStore;
import org.fedoracommons.akubra.BlobStoreConnection;
import org.fedoracommons.akubra.BlobWrapper;
import org.fedoracommons.akubra.DuplicateBlobException;
import org.fedoracommons.akubra.MissingBlobException;

/**
 * A basic superclass for transactional store connections. This implements the common blob-handling
 * parts of a transactional connection, leaving subclasses to implement the transactional
 * management of the id-mappings.
 *
 * <p>Subclasses must implement {@link #getRealId getRealId}, {@link #remNameEntry remNameEntry},
 * {@link #addNameEntry addNameEntry}, {@link BlobStoreConnection#listBlobIds listBlobIds}, and
 * override {@link #close close}; in addition they may want to override {@link
 * #beforeCompletion beforeCompletion} and/or {@link #afterCompletion afterCompletion} for pre-
 * and post-commit/rollback processing.
 *
 * <p>The subclass is expected to implement id mapping, mapping upper-level blob-id's to underlying
 * blob-id's; these mappings are managed via the <code>remNameEntry</code> and
 * <code>addNameEntry</code> method, and <code>getRealId</code> is used to query the mapping.
 *
 * @author Ronald Tschalär
 */
public abstract class AbstractTransactionalConnection extends AbstractBlobStoreConnection
    implements Synchronization {
  private static final Log logger = LogFactory.getLog(AbstractTransactionalConnection.class);

  /** the underlying blob-store that actually stores the blobs */
  protected final BlobStoreConnection bStoreCon;
  /** the transaction this connection belongs to */
  protected final Transaction tx;
  /** the list of underlying id's of added blobs */
  protected final List<URI>   newBlobs = new ArrayList<URI>();
  /** the list of underlying id's of deleted blobs */
  protected final List<URI>   delBlobs = new ArrayList<URI>();

  /**
   * Create a new transactional connection.
   *
   * @param owner   the blob-store we belong to
   * @param bStore  the underlying blob-store to use
   * @param tx      the transaction we belong to
   * @throws IOException if an error occurs initializing this connection
   */
  protected AbstractTransactionalConnection(BlobStore owner, BlobStore bStore, Transaction tx)
      throws IOException {
    super(owner);
    this.bStoreCon = bStore.openConnection(null);
    this.tx        = tx;

    try {
      tx.registerSynchronization(this);
    } catch (Exception e) {
      throw (IOException) new IOException("Error registering txn synchronization").initCause(e);
    }

    if (logger.isDebugEnabled())
      logger.debug("opened connection " + this);
  }

  //@Override
  public Blob getBlob(URI blobId, final Map<String, String> hints) throws IOException {
    if (isClosed())
      throw new IllegalStateException("Connection closed.");

    if (blobId == null)
      blobId = createBlob(blobId, hints).getId();

    return new AbstractBlob(this, blobId) {

      public boolean exists() throws IOException {
        Blob blob = lookupBlob(getId(), hints);
        return (blob != null) && blob.exists();
      }

      public void create() throws IOException {
        createBlob(getId(), hints);
      }

      public void delete() throws IOException {
        removeBlob(getId(), hints);
      }

      public void moveTo(Blob blob) throws IOException {
        renameBlob(getId(), blob.getId(), hints);
      }

      public long getSize() throws IOException {
        Blob blob = lookupBlob(getId(), hints);
        if (blob == null)
          throw new MissingBlobException(getId());
        return blob.getSize();
      }

      public InputStream openInputStream() throws IOException {
        Blob blob = lookupBlob(getId(), hints);
        if (blob == null)
          throw new MissingBlobException(getId());
        return blob.openInputStream();
      }

      public OutputStream openOutputStream(long estimatedSize) throws IOException {
        Blob blob = lookupBlob(getId(), hints);
        if (blob == null)
          throw new MissingBlobException(getId());
        return blob.openOutputStream(estimatedSize);
      }
    };
  }

  public void close() {
    bStoreCon.close();
    super.close();
  }

  Blob createBlob(URI blobId, Map<String, String> hints)
      throws DuplicateBlobException, IOException {
    if (isClosed())
      throw new IllegalStateException("Connection closed.");

    if (logger.isTraceEnabled())
      logger.trace("creating blob '" + blobId + "' (" + this + ")");

    if (getRealId(blobId) != null)
      throw new DuplicateBlobException(blobId);

    Blob res = bStoreCon.getBlob(blobId, hints);
    if (!res.exists())
      res.create();

    if (blobId == null)
      blobId = res.getId();

    addNameEntry(blobId, res.getId());
    newBlobs.add(res.getId());

    if (logger.isTraceEnabled())
      logger.trace("created blob '" + blobId + "' with underlying id '" + res.getId() + "' (" +
                   this + ")");

    return res.getId().equals(blobId) ? res : new BlobWrapper(res, this, blobId);
  }

  Blob lookupBlob(URI blobId, Map<String, String> hints) throws IOException {
    if (isClosed())
      throw new IllegalStateException("Connection closed.");

    if (logger.isTraceEnabled())
      logger.trace("getting blob '" + blobId + "' (" + this + ")");

    URI realId = getRealId(blobId);
    if (realId == null)
      return null;

    Blob res = bStoreCon.getBlob(realId, hints);

    if (logger.isTraceEnabled())
      logger.trace("got blob '" + blobId + "' with underlying id '" +
                   (res != null ? res.getId() : null) + "' (" + this + ")");

    return (res != null) ? new BlobWrapper(res, this, blobId) : null;
  }

  void renameBlob(URI oldBlobId, URI newBlobId, Map<String, String> hints)
      throws DuplicateBlobException, IOException, MissingBlobException {
    if (isClosed())
      throw new IllegalStateException("Connection closed.");

    if (logger.isTraceEnabled())
      logger.trace("renaming blob '" + oldBlobId + "' to '" + newBlobId + "' (" + this + ")");

    URI id = getRealId(oldBlobId);
    if (id == null)
      throw new MissingBlobException(oldBlobId);

    remNameEntry(oldBlobId, id);
    addNameEntry(newBlobId, id);
  }

  URI removeBlob(URI blobId, Map<String, String> hints) throws IOException {
    if (isClosed())
      throw new IllegalStateException("Connection closed.");

    if (logger.isTraceEnabled())
      logger.trace("removing blob '" + blobId + "' (" + this + ")");

    URI id = getRealId(blobId);
    if (id == null)
      return null;

    remNameEntry(blobId, id);
    if (newBlobs.contains(id))
      newBlobs.remove(id);
    else
      delBlobs.add(id);

    if (logger.isTraceEnabled())
      logger.trace("removed blob '" + blobId + "' with underlying id '" + id + "' (" + this + ")");

    return id;
  }

  /**
   * Look up the underlying store's blob-id for the given upper-level blob-id.
   *
   * @param blobId  the upper level blob-id
   * @return the underlying blob-id that <var>blobId</var> maps to, or null if no such mapping
   *         exists (i.e. <var>blobId</var> is not a known upper-level blob-id)
   * @throws IOException if an error occurred looking up the id
   */
  protected abstract URI getRealId(URI blobId) throws IOException;

  /**
   * Remove an id mapping.
   *
   * @param ourId   the upper-level blob-id
   * @param storeId the underlying store's blob-id
   * @throws IOException if an error occurred removing the mapping or the mapping does not exist
   */
  protected abstract void remNameEntry(URI ourId, URI storeId) throws IOException;

  /**
   * Add an id mapping.
   *
   * @param ourId   the upper-level blob-id to map
   * @param storeId the underlying store's blob-id to map <var>ourId</var> to
   * @throws IOException if an error occurred adding the mapping or the mapping already exists
   */
  protected abstract void addNameEntry(URI ourId, URI storeId) throws IOException;

  /**
   * Invoked before the transaction is completed, i.e. before a rollback or commit is started.
   * Whether or not this is called on a rollback may vary.
   *
   * @see Synchronization#beforeCompletion
   */
  public void beforeCompletion() {
  }

  /**
   * Invoked after the transaction has completed, i.e. after a rollback or commit has finished.
   * This is always callled.
   *
   * <p>Subclasses that override this must make sure to invoke <code>super.afterCompletion</code>
   * so that the cleanup code in this implementation is run. This implementation cleans up deleted
   * or added blobs (depending on the outcome of the transaction).
   *
   * @see Synchronization#afterCompletion
   */
  public void afterCompletion(int status) {
    if (status == Status.STATUS_COMMITTED) {
      for (URI blobId : delBlobs) {
        try {
          bStoreCon.getBlob(blobId, null).delete();
        } catch (IOException ioe) {
          logger.error("Error deleting removed blob after commit: blobId = '" + blobId + "'",
                       ioe);
        }
      }
    } else {
      for (URI blobId : newBlobs) {
        try {
          bStoreCon.getBlob(blobId, null).delete();
        } catch (IOException ioe) {
          logger.error("Error deleting added blob after rollback: blobId = '" + blobId + "'",
                       ioe);
        }
      }
    }
  }
}
