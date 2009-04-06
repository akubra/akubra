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

import org.fedoracommons.akubra.Blob;
import org.fedoracommons.akubra.BlobStore;
import org.fedoracommons.akubra.BlobStoreConnection;
import org.fedoracommons.akubra.DuplicateBlobException;
import org.fedoracommons.akubra.MissingBlobException;
import org.fedoracommons.akubra.UnsupportedIdException;
import org.fedoracommons.akubra.impl.AbstractBlob;
import org.fedoracommons.akubra.impl.AbstractBlobStoreConnection;

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
  protected final Transaction         tx;
  /** Whether or not the current transaction has been completed yet */
  protected       boolean             isCompleted = false;
  /** the list of underlying id's of added blobs */
  protected final List<URI>           newBlobs = new ArrayList<URI>();
  /** the list of underlying id's of deleted blobs */
  protected final List<URI>           delBlobs = new ArrayList<URI>();

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

    if (blobId != null)
      validateId(blobId);
    else
      blobId = createBlob(null, hints);

    return new AbstractBlob(this, blobId) {
      private boolean isNew;

      public boolean exists() throws IOException {
        return (getRealId(getId()) != null);
      }

      public void create() throws IOException {
        createBlob(getId(), hints);
        isNew = true;
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
        if (!exists())
          throw new MissingBlobException(getId());

        if (!isNew) {
          removeBlob(getId(), hints);
          createBlob(getId(), hints);
        }

        return lookupBlob(getId(), hints).openOutputStream(estimatedSize);
      }
    };
  }

  private URI createBlob(URI blobId, Map<String, String> hints)
      throws DuplicateBlobException, IOException {
    if (isClosed())
      throw new IllegalStateException("Connection closed.");
    if (blobId == null)
      throw new UnsupportedIdException(null, "id-generation is not currently supported");

    if (logger.isDebugEnabled())
      logger.debug("creating blob '" + blobId + "' (" + this + ")");

    if (getRealId(blobId) != null)
      throw new DuplicateBlobException(blobId);

    boolean accAppId =
        bStoreCon.getBlobStore().getCapabilities().contains(BlobStore.ACCEPT_APP_ID_CAPABILITY);
    Blob res = bStoreCon.getBlob(accAppId ? blobId : null, hints);
    if (accAppId && res.exists()) {
      if (logger.isDebugEnabled())
        logger.debug("duplicate id - retrying with generated id");
      res = bStoreCon.getBlob(null, hints);
    }

    if (!res.exists())
      res.create();

    boolean added = false;
    try {
      addNameEntry(blobId, res.getId());
      addBlob(blobId, res.getId());
      added = true;
    } finally {
      if (!added) {
        try {
          res.delete();
        } catch (Throwable t) {
          logger.warn("Error removing created blob during exception handling: lower-blob-id = '" +
                      res.getId() + "'", t);
        }
      }
    }

    if (logger.isDebugEnabled())
      logger.debug("created blob '" + blobId + "' with underlying id '" + res.getId() + "' (" +
                   this + ")");

    return blobId;
  }

  private Blob lookupBlob(URI blobId, Map<String, String> hints) throws IOException {
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

    return res;
  }

  private void renameBlob(URI oldBlobId, URI newBlobId, Map<String, String> hints)
      throws DuplicateBlobException, IOException, MissingBlobException {
    if (isClosed())
      throw new IllegalStateException("Connection closed.");

    if (logger.isDebugEnabled())
      logger.debug("renaming blob '" + oldBlobId + "' to '" + newBlobId + "' (" + this + ")");

    URI id = getRealId(oldBlobId);
    if (id == null)
      throw new MissingBlobException(oldBlobId);
    if (getRealId(newBlobId) != null)
      throw new DuplicateBlobException(newBlobId);

    remNameEntry(oldBlobId, id);
    addNameEntry(newBlobId, id);
  }

  private void removeBlob(URI blobId, Map<String, String> hints) throws IOException {
    if (isClosed())
      throw new IllegalStateException("Connection closed.");

    if (logger.isDebugEnabled())
      logger.debug("removing blob '" + blobId + "' (" + this + ")");

    URI id = getRealId(blobId);
    if (id == null)
      return;

    remNameEntry(blobId, id);
    remBlob(blobId, id);

    if (logger.isDebugEnabled())
      logger.debug("removed blob '" + blobId + "' with underlying id '" + id + "' (" + this + ")");
  }

  /**
   * Check whether we can store this id.
   *
   * @param blobId  the upper level blob-id
   * @throws UnsupportedIdException if the id cannot be stored
   */
  protected void validateId(URI blobId) throws UnsupportedIdException {
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
   * Remove a blob from the underlying store. This implementation just updates the {@link newBlobs}
   * and {@link delBlobs} lists; actual blob deletion is deferred till commit.
   *
   * @param ourId   the upper-level blob-id
   * @param storeId the underlying store's blob-id
   * @throws IOException if an error occurred removing the blob or the blob does not exist
   */
  protected void remBlob(URI ourId, URI storeId) throws IOException {
    if (newBlobs.contains(storeId)) {
      newBlobs.remove(storeId);
      bStoreCon.getBlob(storeId, null).delete();
    } else {
      delBlobs.add(storeId);
    }
  }

  /**
   * Add a blob to the underlying store. This implementation just updates the {@link newBlobs}
   * list; actual blob writing is done via the blob itself..
   *
   * @param ourId   the upper-level blob-id
   * @param storeId the underlying store's blob-id
   * @throws IOException if an error occurred removing the blob or the blob does not exist
   */
  protected void addBlob(URI ourId, URI storeId) throws IOException {
    newBlobs.add(storeId);
  }

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
    if (isCompleted)    // I've seen BTM call us twice here after a timeout and rollback.
      return;
    isCompleted = true;

    try {
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
            logger.error("Error deleting added blob after rollback: blobId = '" + blobId + "'", ioe);
          }
        }
      }
    } finally {
      bStoreCon.close();
    }
  }
}
