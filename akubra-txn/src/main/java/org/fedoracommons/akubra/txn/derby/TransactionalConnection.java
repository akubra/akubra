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

package org.fedoracommons.akubra.txn.derby;

import java.io.IOException;
import java.net.URI;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import javax.transaction.Status;
import javax.transaction.Synchronization;
import javax.transaction.Transaction;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.fedoracommons.akubra.Blob;
import org.fedoracommons.akubra.BlobStore;
import org.fedoracommons.akubra.BlobStoreConnection;
import org.fedoracommons.akubra.BlobWrapper;
import org.fedoracommons.akubra.DuplicateBlobException;
import org.fedoracommons.akubra.MissingBlobException;

/**
 * A connection for the transactional store.
 *
 * @author Ronald Tschalär
 */
public class TransactionalConnection implements BlobStoreConnection {
  private static final Log logger = LogFactory.getLog(TransactionalConnection.class);

  private final BlobStore   owner;
  private final BlobStoreConnection bStoreCon;
  private final Connection  con;
  private final Transaction tx;
  private final List<URI>   newBlobs = new ArrayList<URI>();
  private final List<URI>   delBlobs = new ArrayList<URI>();

  /**
   * Create a new transactional connection.
   *
   * @param owner   the blob-store we belong to
   * @param bStore  the underlying blob-store to use
   * @param con     the db connection to use
   * @param tx      the transaction we belong to
   * @throws IOException if an error occurs initializing this connection
   */
  TransactionalConnection(BlobStore owner, BlobStore bStore, Connection con, Transaction tx)
      throws IOException {
    this.owner     = owner;
    this.bStoreCon = bStore.openConnection(null);
    this.con       = con;
    this.tx        = tx;

    try {
      tx.registerSynchronization(new TCSynch());
    } catch (Exception e) {
      throw (IOException) new IOException("Error registering txn synchronization").initCause(e);
    }

    if (logger.isDebugEnabled())
      logger.debug("opened connection " + this);
  }

  //@Override
  public BlobStore getBlobStore() {
    return owner;
  }

  //@Override
  public Blob createBlob(URI blobId, Map<String, String> hints)
      throws DuplicateBlobException, IOException {
    if (logger.isTraceEnabled())
      logger.trace("creating blob '" + blobId + "' (" + this + ")");

    if (getRealId(blobId) != null)
      throw new DuplicateBlobException(blobId);

    Blob res;
    try {
      res = bStoreCon.createBlob(blobId, hints);
    } catch (DuplicateBlobException dbe) {
      logger.debug("duplicate id - retrying with generated id");
      res = bStoreCon.createBlob(null, hints);
    }

    if (blobId == null)
      blobId = res.getId();

    addNameEntry(blobId, res.getId());
    newBlobs.add(res.getId());

    if (logger.isTraceEnabled())
      logger.trace("created blob '" + blobId + "' with underlying id '" + res.getId() + "' (" +
                   this + ")");

    return res.getId().equals(blobId) ? res : new IdBlobWrapper(res, blobId);
  }

  //@Override
  public Blob getBlob(URI blobId, Map<String, String> hints) throws IOException {
    if (logger.isTraceEnabled())
      logger.trace("getting blob '" + blobId + "' (" + this + ")");

    URI realId = getRealId(blobId);
    if (realId == null)
      return null;

    Blob res = bStoreCon.getBlob(realId, hints);

    if (logger.isTraceEnabled())
      logger.trace("got blob '" + blobId + "' with underlying id '" +
                   (res != null ? res.getId() : null) + "' (" + this + ")");

    return (res != null) ? new IdBlobWrapper(res, blobId) : null;
  }

  //@Override
  public void renameBlob(URI oldBlobId, URI newBlobId, Map<String, String> hints)
      throws DuplicateBlobException, IOException, MissingBlobException {
    if (logger.isTraceEnabled())
      logger.trace("renaming blob '" + oldBlobId + "' to '" + newBlobId + "' (" + this + ")");

    URI id = getRealId(oldBlobId);
    if (id == null)
      throw new MissingBlobException(oldBlobId);

    remNameEntry(oldBlobId, id);
    addNameEntry(newBlobId, id);
  }

  //@Override
  public URI removeBlob(URI blobId, Map<String, String> hints) throws IOException {
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

  //@Override
  public Iterator<URI> listBlobIds(String filterPrefix) throws IOException {
    if (logger.isTraceEnabled())
      logger.trace("listing blob-ids with prefix '" + filterPrefix + "' (" + this + ")");

    String query = (filterPrefix != null && filterPrefix.length() > 0) ?
      "SELECT appId FROM " + TransactionalStore.NAME_TABLE + " WHERE appId LIKE '" +
                             escLike(filterPrefix) + "%' ESCAPE '!'" :
      "SELECT appId FROM " + TransactionalStore.NAME_TABLE;

    try {
      final Statement stmt = con.createStatement();
      final ResultSet rs   = stmt.executeQuery(query);

      return new Iterator<URI>() {
        private boolean hasNext = rs.next();

        //@Override
        public boolean hasNext() {
          return hasNext;
        }

        //@Override
        public URI next() throws NoSuchElementException {
          if (!hasNext)
            throw new NoSuchElementException();

          try {
            URI res = URI.create(rs.getString(1));
            hasNext = rs.next();

            return res;
          } catch (SQLException sqle) {
            throw new RuntimeException("error reading db results", sqle);
          }
        }

        //@Override
        public void remove() throws UnsupportedOperationException {
          throw new UnsupportedOperationException();
        }

        @Override
        protected void finalize() {
          try {
            rs.close();
          } catch (SQLException sqle2) {
            logger.error("Error closing result-set", sqle2);
          }

          try {
            stmt.close();
          } catch (SQLException sqle2) {
            logger.error("Error closing statement", sqle2);
          }
        }
      };
    } catch (SQLException sqle) {
      throw (IOException) new IOException("Error querying db").initCause(sqle);
    }
  }

  //@Override
  public void close() {
    if (logger.isDebugEnabled())
      logger.debug("closing connection " + this);

    try {
      con.close();
    } catch (SQLException sqle) {
      logger.error("Error closing db connection", sqle);
    }
  }

  private URI getRealId(URI blobId) throws IOException {
    try {
      Statement stmt = con.createStatement();
      try {
        ResultSet rs = stmt.executeQuery("SELECT storeId FROM " + TransactionalStore.NAME_TABLE +
                                         " WHERE appId = '" + escLit(blobId) + "'");
        try {
          return rs.next() ? URI.create(rs.getString(1)) : null;
        } finally {
          rs.close();
        }
      } finally {
        stmt.close();
      }
    } catch (SQLException sqle) {
      throw (IOException) new IOException("Error querying db").initCause(sqle);
    }
  }

  private void remNameEntry(URI ourId, URI storeId) throws IOException {
    try {
      Statement stmt = con.createStatement();
      try {
        stmt.executeUpdate("DELETE FROM " + TransactionalStore.NAME_TABLE + " WHERE appId = '" +
                           escLit(ourId) + "'");
      } finally {
        stmt.close();
      }
    } catch (SQLException sqle) {
      throw (IOException) new IOException("Error updating db").initCause(sqle);
    }
  }

  private void addNameEntry(URI ourId, URI storeId) throws IOException {
    try {
      Statement stmt = con.createStatement();
      try {
        stmt.executeUpdate("INSERT INTO " + TransactionalStore.NAME_TABLE + " VALUES ('" +
                           escLit(ourId) + "', '" + escLit(storeId) + "')");
      } finally {
        stmt.close();
      }
    } catch (SQLException sqle) {
      throw (IOException) new IOException("Error updating db").initCause(sqle);
    }
  }

  private static String escLit(URI uri) {
    return escLit(uri.toString());
  }

  private static String escLit(String str) {
    return str.replace("'", "''");
  }

  private static String escLike(String str) {
    return escLit(str).replace("!", "!!").replace("_", "!_").replace("%", "!%");
  }

  private class IdBlobWrapper extends BlobWrapper {
    private final URI id;

    IdBlobWrapper(Blob delegate, URI id) {
      super(delegate);
      this.id = id;
    }

    @Override
    public BlobStoreConnection getConnection() {
      return TransactionalConnection.this;
    }

    @Override
    public URI getId() {
      return id;
    }
  }

  private class TCSynch implements Synchronization {
    //@Override
    public void beforeCompletion() {
      // FIXME bStoreCon.flush();
    }

    //@Override
    public void afterCompletion(int status) {
      if (status == Status.STATUS_COMMITTED) {
        for (URI blobId : delBlobs) {
          try {
            bStoreCon.removeBlob(blobId, null);
          } catch (IOException ioe) {
            logger.error("Error deleting removed blob after commit: blobId = '" + blobId + "'",
                         ioe);
          }
        }
      } else {
        for (URI blobId : newBlobs) {
          try {
            bStoreCon.removeBlob(blobId, null);
          } catch (IOException ioe) {
            logger.error("Error deleting added blob after rollback: blobId = '" + blobId + "'",
                         ioe);
          }
        }
      }
    }
  }
}
