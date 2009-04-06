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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;

import javax.sql.XAConnection;
import javax.transaction.Status;
import javax.transaction.Transaction;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.derby.iapi.services.monitor.Monitor;

import org.fedoracommons.akubra.BlobStore;
import org.fedoracommons.akubra.UnsupportedIdException;
import org.fedoracommons.akubra.txn.ConcurrentBlobUpdateException;
import org.fedoracommons.akubra.txn.SQLTransactionalConnection;

/**
 * A connection for the transactional store.
 *
 * @author Ronald Tschalär
 */
public class TransactionalConnection extends SQLTransactionalConnection {
  private static final Log logger = LogFactory.getLog(TransactionalConnection.class);

  private final long              version;
  private final PreparedStatement nam_get;
  private final PreparedStatement nam_ins;
  private final PreparedStatement nam_upd;
  private final PreparedStatement del_ins;
  private final PreparedStatement del_upd;
  private final PreparedStatement nam_cfl;

  private int numMods = 0;

  /**
   * Create a new transactional connection.
   *
   * @param owner   the blob-store we belong to
   * @param bStore  the underlying blob-store to use
   * @param xaCon   the xa connection to use
   * @param con     the db connection to use
   * @param tx      the transaction we belong to
   * @param version the read version to use
   * @throws IOException if an error occurs initializing this connection
   */
  TransactionalConnection(BlobStore owner, BlobStore bStore, XAConnection xaCon, Connection con,
                          Transaction tx, long version)
      throws IOException {
    super(owner, bStore, xaCon, con, tx);
    this.version = version;

    try {
      String sql = "SELECT storeId, deleted FROM " + TransactionalStore.NAME_TABLE +
                   " WHERE appId = ? AND (version < " + version + " AND committed <> 0 OR " +
                   " version = " + version + ") ORDER BY version DESC";
      nam_get = con.prepareStatement(sql);
      nam_get.setMaxRows(1);

      sql = "INSERT INTO " + TransactionalStore.NAME_TABLE +
            " VALUES (?, ?, " + version + ", ?, ?)";
      nam_ins = con.prepareStatement(sql);

      sql = "UPDATE " + TransactionalStore.NAME_TABLE + " SET storeId = ?, deleted = ? " +
            " WHERE appId = ? AND version = " + version;
      nam_upd = con.prepareStatement(sql);

      sql = "INSERT INTO " + TransactionalStore.DEL_TABLE + " VALUES (?, ?, " + version + ")";
      del_ins = con.prepareStatement(sql);

      sql = "UPDATE " + TransactionalStore.DEL_TABLE + " SET storeId = ? " +
            " WHERE appId = ? AND version = " + version;
      del_upd = con.prepareStatement(sql);

      sql = "SELECT version, committed, deleted FROM " + TransactionalStore.NAME_TABLE +
            " WHERE appId = ? ORDER BY version DESC";
      nam_cfl = con.prepareStatement(sql);
      nam_cfl.setMaxRows(1);
    } catch (SQLException sqle) {
      throw (IOException) new IOException("Error querying db").initCause(sqle);
    }
  }

  //@Override
  public Iterator<URI> listBlobIds(String filterPrefix) throws IOException {
    if (logger.isDebugEnabled())
      logger.debug("listing blob-ids with prefix '" + filterPrefix + "' (" + this + ")");

    String query =
      "SELECT appId, version, deleted FROM " + TransactionalStore.NAME_TABLE +
      " WHERE (version < " + version + " and committed <> 0 or version = " + version + ")";
    if (filterPrefix != null && filterPrefix.length() > 0)
      query += " and appId LIKE '" + escLike(filterPrefix) + "%' ESCAPE '!'";
    query += " ORDER BY appId";

    try {
      ResultSet rs = con.createStatement().executeQuery(query);
      return new RSBlobIdIterator(rs, true) {
        private String  lastId;
        private boolean afterFirst;
        private boolean notFinished;

        @Override
        protected boolean nextRow() throws SQLException {
          if (!afterFirst) {
            afterFirst  = true;
            notFinished = rs.next();
          }

          boolean isDel = false;
          do {
            if (!notFinished)
              return false;

            long maxVers = 0;

            lastId = rs.getString(1);
            do {
              long v = rs.getLong(2);
              if (v > maxVers) {
                maxVers = v;
                isDel   = rs.getBoolean(3);
              }
            } while ((notFinished = rs.next()) && rs.getString(1).equals(lastId));
          } while (isDel);

          return true;
        }

        @Override
        protected String getId() throws SQLException {
          return lastId;
        }
      };
    } catch (SQLException sqle) {
      throw (IOException) new IOException("Error querying db").initCause(sqle);
    }
  }

  @Override
  protected void validateId(URI blobId) throws UnsupportedIdException {
    if (blobId.toString().length() > 1000)
      throw new UnsupportedIdException(blobId, "Blob id must be less than 1000 characters long");
  }

  @Override
  protected URI getRealId(URI blobId) throws IOException {
    try {
      //System.out.println(dumpResults(con.createStatement().executeQuery(
      //    "SELECT * FROM " + TransactionalStore.NAME_TABLE)));

      nam_get.setString(1, blobId.toString());

      ResultSet rs = nam_get.executeQuery();
      try {
        return !rs.next() ? null : rs.getBoolean(2) ? null : URI.create(rs.getString(1));
      } finally {
        rs.close();
      }
    } catch (SQLException sqle) {
      throw (IOException) new IOException("Error querying db").initCause(sqle);
    }
  }

  static String dumpResults(ResultSet rs) throws SQLException {
    StringBuilder res = new StringBuilder(500);
    res.append("table dump (").append(rs.getMetaData().getTableName(1)).append(":\n");

    int numCols = rs.getMetaData().getColumnCount();
    res.append("  ");
    for (int idx = 1; idx <= numCols; idx++)
      res.append(rs.getMetaData().getColumnLabel(idx)).append(idx < numCols ? ", " : "");
    res.append("\n");

    while (rs.next()) {
      res.append("  ");
      for (int idx = 1; idx <= numCols; idx++)
        res.append(rs.getString(idx)).append(idx < numCols ? ", " : "");
      res.append("\n");
    }

    rs.close();

    return res.toString();
  }

  @Override
  protected void remNameEntry(URI ourId, URI storeId) throws IOException {
    if (logger.isDebugEnabled())
      logger.debug("Removing name-entry '" + ourId + "' -> '" + storeId + "', version=" + version);

    updNameEntry(ourId, storeId, true);
  }

  @Override
  protected void addNameEntry(URI ourId, URI storeId) throws IOException {
    if (logger.isDebugEnabled())
      logger.debug("Adding name-entry '" + ourId + "' -> '" + storeId + "', version=" + version);

    updNameEntry(ourId, storeId, false);
  }

  private void updNameEntry(URI ourId, URI storeId, boolean delete) throws IOException {
    try {
      boolean useUpdate = false;

      nam_cfl.setString(1, ourId.toString());
      ResultSet rs = nam_cfl.executeQuery();
      try {
        if (rs.next()) {
          long v = rs.getLong(1);
          if (v > version ||
              v < version && !rs.getBoolean(2) ||
              rs.getBoolean(3) == delete)
            throw new ConcurrentBlobUpdateException(ourId, "Conflict detected: '" + ourId +
                                  "' is already being modified in another transaction");

          if (v == version)
            useUpdate = true;
        }
      } finally {
        rs.close();
      }

      numMods++;

      if (useUpdate) {
        if (logger.isTraceEnabled())
          logger.trace("Updating existing name-entry");

        nam_upd.setString(1, storeId.toString());
        nam_upd.setBoolean(2, delete);
        nam_upd.setString(3, ourId.toString());
        nam_upd.executeUpdate();
      } else {
        if (logger.isTraceEnabled())
          logger.trace("Inserting new name-entry");

        nam_ins.setString(1, ourId.toString());
        nam_ins.setString(2, storeId.toString());
        nam_ins.setBoolean(3, delete);
        nam_ins.setBoolean(4, false);
        nam_ins.executeUpdate();
      }

      if (delete) {
        del_ins.setString(1, ourId.toString());
        del_ins.setString(2, null);
        del_ins.executeUpdate();
      }
    } catch (SQLException sqle) {
      throw (IOException) new IOException("Error updating db").initCause(sqle);
    }
  }

  @Override
  protected void remBlob(URI ourId, URI storeId) throws IOException {
    try {
      if (newBlobs.contains(storeId)) {
        newBlobs.remove(storeId);
        bStoreCon.getBlob(storeId, null).delete();
      } else {
        del_upd.setString(1, storeId.toString());
        del_upd.setString(2, ourId.toString());
        del_upd.executeUpdate();
      }
    } catch (SQLException sqle) {
      throw (IOException) new IOException("Error updating delete-blobs table").initCause(sqle);
    }
  }

  private static String escLike(String str) {
    return str.replace("'", "''").replace("!", "!!").replace("_", "!_").replace("%", "!%");
  }

  @Override
  public void beforeCompletion() {
    if (numMods > 0) {
      try {
        long writeVers = ((TransactionalStore) owner).txnPrepare(numMods, version);

        if (logger.isTraceEnabled())
          logger.trace("updating name-table for commit");
        con.createStatement().execute(
            "UPDATE " + TransactionalStore.NAME_TABLE + " SET version = " + writeVers +
            ", committed = 1 WHERE version = " + version);

        if (logger.isTraceEnabled())
          logger.trace("updating delete-table for commit");
        con.createStatement().execute(
            "UPDATE " + TransactionalStore.DEL_TABLE + " SET version = " + writeVers +
            " WHERE version = " + version);
      } catch (InterruptedException ie) {
        throw new RuntimeException("Error waiting for write lock", ie);
      } catch (SQLException sqle) {
        throw new RuntimeException("Error updating db", sqle);
      }
    }

    super.beforeCompletion();
  }

  @Override
  public void afterCompletion(int status) {
    if (isCompleted)
      return;

    try {
      ((TransactionalStore) owner).txnComplete(status == Status.STATUS_COMMITTED, version);
    } finally {
      super.afterCompletion(status);
    }

    ((TransactionalStore) owner).purgeOldVersions(version);

    /* java.util.Timer does not actually remove a cancelled task until purge() is invoked. This
     * leads to connections not being cleaned up, and eventually an OOM exception. Hence we
     * invoke purge() here. Derby should be doing this itself, though - see
     * https://issues.apache.org/jira/browse/DERBY-4137
     */
    Monitor.getMonitor().getTimerFactory().getCancellationTimer().purge();
  }
}
