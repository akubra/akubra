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
import java.sql.Statement;
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
  private final PreparedStatement nam_cmt;
  private final PreparedStatement del_cmt;
  private final PreparedStatement nam_lst_all;
  private final PreparedStatement nam_lst_pfx;

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
      /* Note: it's important that these all be constant strings (i.e. always the same on each
       * invocation) so that jdbc prepared-statement caching can kick in.
       */

      // get store-id
      String sql = "SELECT storeId, deleted FROM " + TransactionalStore.NAME_TABLE +
                   " WHERE appId = ? AND (version < ? AND committed <> 0 OR " +
                   " version = ?) ORDER BY version DESC";
      nam_get = con.prepareStatement(sql);
      nam_get.setMaxRows(1);

      // update name-table on blob insert/delete/modify
      sql = "INSERT INTO " + TransactionalStore.NAME_TABLE + " VALUES (?, ?, ?, ?, ?)";
      nam_ins = con.prepareStatement(sql);

      sql = "SELECT storeId, deleted FROM " + TransactionalStore.NAME_TABLE +
            " -- DERBY-PROPERTIES index=NAME_MAP_AIIDX \n WHERE appId = ? AND version = ?";
      nam_upd = con.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);

      // update delete-list on blob delete
      sql = "INSERT INTO " + TransactionalStore.DEL_TABLE + " VALUES (?, ?, ?)";
      del_ins = con.prepareStatement(sql);

      sql = "SELECT storeId FROM " + TransactionalStore.DEL_TABLE +
            " -- DERBY-PROPERTIES index=DELETED_LIST_VIDX \n WHERE appId = ? AND version = ?";
      del_upd = con.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);

      // detect update conflicts
      sql = "SELECT version, committed, deleted FROM " + TransactionalStore.NAME_TABLE +
            " WHERE appId = ? ORDER BY version DESC";
      nam_cfl = con.prepareStatement(sql);
      nam_cfl.setMaxRows(1);

      // update name-table and delete-list on commit
      sql = "SELECT version, committed FROM " + TransactionalStore.NAME_TABLE +
            " -- DERBY-PROPERTIES index=NAME_MAP_VIDX \n WHERE version = ?";
      nam_cmt = con.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);

      sql = "SELECT version FROM " + TransactionalStore.DEL_TABLE +
            " -- DERBY-PROPERTIES index=DELETED_LIST_VIDX \n WHERE version = ?";
      del_cmt = con.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);

      // list blob-ids
      sql = "SELECT appId, version, deleted FROM " + TransactionalStore.NAME_TABLE +
            " WHERE (version < ? AND committed <> 0 OR version = ?) ORDER BY appId";
      nam_lst_all = con.prepareStatement(sql);

      sql = "SELECT appId, version, deleted FROM " + TransactionalStore.NAME_TABLE +
            " WHERE (version < ? AND committed <> 0 OR version = ?)" +
            " AND appId LIKE ? ESCAPE '!' ORDER BY appId";
      nam_lst_pfx = con.prepareStatement(sql);
    } catch (SQLException sqle) {
      throw (IOException) new IOException("Error querying db").initCause(sqle);
    }
  }

  //@Override
  public Iterator<URI> listBlobIds(String filterPrefix) throws IOException {
    if (logger.isDebugEnabled())
      logger.debug("listing blob-ids with prefix '" + filterPrefix + "' (" + this + ")");

    try {
      PreparedStatement query;
      if (filterPrefix != null && filterPrefix.trim().length() > 0) {
        query = nam_lst_pfx;
        query.setLong(1, version);
        query.setLong(2, version);
        query.setString(3, escLike(filterPrefix.trim()) + '%');
      } else {
        query = nam_lst_all;
        query.setLong(1, version);
        query.setLong(2, version);
      }

      ResultSet rs = query.executeQuery();
      return new RSBlobIdIterator(rs, false) {
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
      nam_get.setLong(2, version);
      nam_get.setLong(3, version);

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

      // check for conflicts
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

      // hack to serialize writers if desired (because of Derby locking issues)
      if (numMods == 0 && ((TransactionalStore) owner).singleWriter()) {
        try {
          ((TransactionalStore) owner).acquireWriteLock(version);
        } catch (InterruptedException ie) {
          throw (IOException) new IOException("Interrupted waiting for write lock").initCause(ie);
        }
      }

      numMods++;

      // add-to/update the name-map and deleted-list
      if (useUpdate) {
        if (logger.isTraceEnabled())
          logger.trace("Updating existing name-entry");

        nam_upd.setString(1, ourId.toString());
        nam_upd.setLong(2, version);
        doUpdate(nam_upd, storeId.toString(), delete);
      } else {
        if (logger.isTraceEnabled())
          logger.trace("Inserting new name-entry");

        nam_ins.setString(1, ourId.toString());
        nam_ins.setString(2, storeId.toString());
        nam_ins.setLong(3, version);
        nam_ins.setBoolean(4, delete);
        nam_ins.setBoolean(5, false);
        nam_ins.executeUpdate();
      }

      if (delete) {
        del_ins.setString(1, ourId.toString());
        del_ins.setString(2, null);
        del_ins.setLong(3, version);
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
        del_upd.setString(1, ourId.toString());
        del_upd.setLong(2, version);
        doUpdate(del_upd, storeId.toString());
      }
    } catch (SQLException sqle) {
      throw (IOException) new IOException("Error updating delete-blobs table").initCause(sqle);
    }
  }

  private static String escLike(String str) {
    return str.replace("!", "!!").replace("_", "!_").replace("%", "!%");
  }

  @Override
  public void beforeCompletion() {
    if (numMods > 0) {
      try {
        long writeVers = ((TransactionalStore) owner).txnPrepare(numMods, version);

        if (logger.isTraceEnabled())
          logger.trace("updating name-table for commit (version=" + version + ", write-version=" +
                       writeVers + ")");

        nam_cmt.setLong(1, version);
        doUpdate(nam_cmt, writeVers, true);

        if (logger.isTraceEnabled())
          logger.trace("updating delete-table for commit (version=" + version + ", write-version=" +
                       writeVers + ")");

        del_cmt.setLong(1, version);
        doUpdate(del_cmt, writeVers);
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
      closeStatements();
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

  private void closeStatements() {
    for (Statement stmt : new Statement[] { nam_get, nam_ins, nam_upd, del_ins, del_upd, nam_cfl,
                                            nam_cmt, del_cmt, nam_lst_all, nam_lst_pfx }) {
      try {
        stmt.close();
      } catch (SQLException sqle) {
        logger.warn("Error closing prepared statement", sqle);
      }
    }
  }

  private static void doUpdate(PreparedStatement query, Object... newVals) throws SQLException {
    ResultSet rs = query.executeQuery();
    try {
      while (rs.next()) {
        int idx = 1;
        for (Object v : newVals) {
          if (v instanceof String)
            rs.updateString(idx++, (String) v);
          else if (v instanceof Boolean)
            rs.updateBoolean(idx++, (Boolean) v);
          else if (v instanceof Long)
            rs.updateLong(idx++, (Long) v);
          else
            throw new Error("Unknown value type " + v.getClass() + " (" + v + ")");
        }
        rs.updateRow();
      }
    } finally {
      rs.close();
    }
  }
}
