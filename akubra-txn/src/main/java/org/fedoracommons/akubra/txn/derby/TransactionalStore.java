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
import java.sql.PreparedStatement;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.sql.XAConnection;
import javax.transaction.Transaction;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.derby.jdbc.EmbeddedXADataSource;

import org.fedoracommons.akubra.BlobStore;
import org.fedoracommons.akubra.BlobStoreConnection;
import org.fedoracommons.akubra.txn.AbstractTransactionalStore;

/**
 * A simple transactional store using Derby db for the transaction logging and id mappings. It
 * provides snapshot isolation with fail-fast semantics, meaning it will immediately throw a
 * {@link org.fedoracommons.akubra.txn.ConcurrentBlobUpdateException ConcurrentBlobUpdateException}
 * if a transaction tries to modify (insert, delete, or overwrite) a blob which was modified by
 * another transaction since the start of the first transaction (even if the change by the other
 * transaction hasn't beem committed yet). The assumption is that rollbacks are rare and that it is
 * better to be notified of a conflict immediately rather than wasting time uploading large amounts
 * of data that will just have to be deleted again.
 *
 * <p>In general a transaction must be considered failed and should be rolled back after any
 * exception occurred.
 *
 * <p>This store must be configured with exactly one underlying blob-store. It supports arbitrary
 * application-ids and maps them to the underlying blob-store's id's; it currently requires that
 * the underlying blob-store to be capable of generating ids.
 *
 * <p>Snapshot isolation is implemented using a MVCC design as follows. A name-map holds a list of
 * versioned id mappings which maps application-ids to underlying store-ids; in addition, each
 * mapping has two flags indicating whether the mapping has been deleted and whether it has been
 * committed. When a transaction starts it is given a read version number (these increase
 * monotonically); only committed map entries with a version less than this read version or
 * uncommitted entries with a version the same as the read version will be read; if there are
 * multiple such entries for a given app-id, then the one with the highest version is used. If the
 * transaction makes a change (adding, removing, replacing, etc), a new entry in recorded in the
 * map with the version set to the read-version and with the committed flag set to false. On commit
 * the transaction is assigned a write version number (which is higher than any previously issued
 * read version numbers) and which it then sets on all entries written as part of this transaction;
 * it also sets the committed flag to true on these entries.
 *
 * <p>Old entries (and the underlying blobs) are cleaned out as they become unreferenced, i.e. when
 * no active transaction could refer to them anymore. In order to speed up the discovery of such
 * entries, a separate deleted-list is kept into which an entry is made each time an entry in the
 * main map is marked as deleted and each time a blob is marked as deleted. This list is processed
 * at the end of every transaction and upon startup (on startup the list is completely cleared as
 * there are no active transactions).
 *
 * @author Ronald Tschalär
 */
public class TransactionalStore extends AbstractTransactionalStore {
  /** The SQL table used by this store to hold the name mappings */
  public static final String NAME_TABLE = "NAME_MAP";
  /** The SQL table used by this store to hold the list of deleted blobs */
  public static final String DEL_TABLE  = "DELETED_LIST";

  private static final Log logger = LogFactory.getLog(TransactionalStore.class);

  private final EmbeddedXADataSource dataSource;
  private final Set<Long>            activeTxns = new HashSet<Long>();
  private       long                 nextVersion;
  private       long                 writeVersion = -1;
  private       long                 writeLockHolder = -1;

  /**
   * Create a new transactional store. Exactly one backing store must be set before this can
   * be used.
   *
   * @param id    the id of this store
   * @param dbDir the directory to use to store the transaction information
   */
  public TransactionalStore(URI id, String dbDir) throws IOException {
    super(id, TXN_CAPABILITY, ACCEPT_APP_ID_CAPABILITY);

    //TODO: redirect logging to logger
    //System.setProperty("derby.stream.error.logSeverityLevel", "50000");
    //System.setProperty("derby.stream.error.file", new File(base, "derby.log").toString());
    //System.setProperty("derby.language.logStatementText", "true");
    //System.setProperty("derby.stream.error.method", "java.sql.DriverManager.getLogStream");

    dataSource = new EmbeddedXADataSource();
    dataSource.setDatabaseName(dbDir);
    dataSource.setCreateDatabase("create");

    Runtime.getRuntime().addShutdownHook(new Thread() {
      public void run() {
        try {
          dataSource.setShutdownDatabase("shutdown");
          dataSource.getXAConnection().getConnection();
        } catch (Exception e) {
          logger.warn("Error shutting down derby", e);
        }
      }
    });

    createTables();
    nextVersion = findYoungestVersion() + 1;
  }

  private void createTables() throws IOException {
    runInCon(new Action<Void>() {
      public Void run(Connection con) throws SQLException {
        // test if table exists
        ResultSet rs = con.getMetaData().getTables(null, null, NAME_TABLE, null);
        try {
          if (rs.next())
            return null;
        } finally {
          rs.close();
        }

        // nope, so create it
        logger.info("Creating tables and indexes for name-map");

        Statement stmt = con.createStatement();
        try {
          stmt.execute("CREATE TABLE " + NAME_TABLE +
                       " (appId VARCHAR(1000), storeId VARCHAR(1000), version BIGINT, deleted SMALLINT, committed SMALLINT)");
          stmt.execute("CREATE INDEX " + NAME_TABLE + "_AIIDX ON " + NAME_TABLE + "(appId)");
          stmt.execute("CREATE INDEX " + NAME_TABLE + "_VIDX ON " + NAME_TABLE + "(version)");

          stmt.execute("CREATE TABLE " + DEL_TABLE + " (appId VARCHAR(1000), storeId VARCHAR(1000), version BIGINT)");
          stmt.execute("CREATE INDEX " + DEL_TABLE + "_VIDX ON " + DEL_TABLE + "(version)");
        } finally {
          stmt.close();
        }

        return null;
      }
    }, "Failed to create tables");
  }

  private long findYoungestVersion() throws IOException {
    return runInCon(new Action<Long>() {
      public Long run(Connection con) throws SQLException {
        Statement stmt = con.createStatement();
        stmt.setMaxRows(1);
        try {
          ResultSet rs =
              stmt.executeQuery("SELECT version FROM " + NAME_TABLE + " ORDER BY version DESC");
          return rs.next() ? rs.getLong(1) : -1L;
        } finally {
          stmt.close();
        }
      }
    }, "Failed to find youngest version");
  }

  @Override
  public void setBackingStores(List<BlobStore> stores)
      throws IllegalStateException, IllegalArgumentException {
    super.setBackingStores(stores);
    if (!wrappedStore.getCapabilities().contains(GENERATE_ID_CAPABILITY))
      throw new IllegalArgumentException("underlying store must support id-generation");
  }

  /**
   * @throws IllegalStateException if no backing store has been set yet
   */
  //@Override
  public BlobStoreConnection openConnection(Transaction tx)
      throws IllegalStateException, IOException {
    long version;
    synchronized (this) {
      if (wrappedStore == null)
        throw new IllegalStateException("no backing store has been set yet");

      if (!started) {
        started = true;
        purgeOldVersions(0);
      }

      while (writeVersion >= 0 && nextVersion == writeVersion) {
        if (logger.isDebugEnabled())
          logger.debug("Out of available versions - waiting for write-lock to be released");

        try {
          wait();
        } catch (InterruptedException ie) {
          throw (IOException) new IOException("wait for write-lock interrupted").initCause(ie);
        }
      }

      version = nextVersion++;

      boolean isNew = activeTxns.add(version);
      assert isNew : "duplicate version " + version;
    }

    boolean ok = false;
    try {
      XAConnection xaCon;
      Connection   con;
      synchronized (dataSource) {
        xaCon = dataSource.getXAConnection();
        con   = xaCon.getConnection();
      }

      con.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);

      tx.enlistResource(xaCon.getXAResource());

      BlobStoreConnection bsc =
          new TransactionalConnection(this, wrappedStore, xaCon, con, tx, version);

      if (logger.isDebugEnabled())
        logger.debug("Opened connection, read-version=" + version);

      ok = true;
      return bsc;
    } catch (IOException ioe) {
      throw ioe;
    } catch (Exception e) {
      throw (IOException) new IOException("Error connecting to db").initCause(e);
    } finally {
      if (!ok) {
        synchronized (this) {
          activeTxns.remove(version);
        }
      }
    }
  }

  /**
   * Acquire the write lock. This is a simple, re-entrant lock without a lock count. If the lock
   * is already held this will block until it is free.
   *
   * @param version the version acquiring the lock
   * @throws InterruptedException if waiting for the lock was interrupted
   */
  synchronized void acquireWriteLock(long version) throws InterruptedException {
    while (writeLockHolder >= 0 && writeLockHolder != version)
      wait();

    if (logger.isTraceEnabled())
      logger.trace("Transaction " + version + " acquired write lock");

    writeLockHolder = version;
  }

  /**
   * Release the write lock. This always completely releases lock no matter how often {@link
   * #acquireWriteLock} was invoked.
   *
   * @param version the version that acquired the lock
   * @throws IllegalStateException if the lock is not held by <var>version</var>
   */
  synchronized void releaseWriteLock(long version) {
    if (writeLockHolder != version)
      throw new IllegalStateException("Connection '" + version + "' is not the holder of the " +
                                      "write lock; '" + writeLockHolder + "' is");

    if (logger.isTraceEnabled())
      logger.trace("Transaction " + version + " released write lock");

    writeLockHolder = -1;
    notifyAll();
  }

  /**
   * Prepare the transaction. This acquires the write-lock and hence must always be followed by
   * {@link #txnComplete} to release it.
   *
   * @param numMods the number of modifications made during this transaction; this is used
   *                to estimate how long the commit might take
   * @param version the transaction's read-version - used for logging
   * @return the write version
   * @throws InterruptedException if interrupted while waiting for the write-lock
   */
  synchronized long txnPrepare(int numMods, long version) throws InterruptedException {
    if (logger.isDebugEnabled())
      logger.debug("Preparing transaction " + version);

    acquireWriteLock(version);

    /* Leave a little space in the version number sequence so other transactions may start while
     * this one completes. The constant '1/100' is pulled out of thin air, and represents a guess
     * on the upper bound on how many transactions are likely to be started during the time it 
     * takes this one to complete; if it is too large then we just have larger holes and the
     * transaction numbers jump more than necessary, which isn't tragic as long as the jumps are
     * not so large that we run into a real possibility of version number wrap-around; if it is too
     * small then that just means transactions may be needlessly held up waiting for this one to
     * complete.
     */
    writeVersion = nextVersion + numMods / 100;

    if (logger.isDebugEnabled())
      logger.debug("Prepared transaction " + version + ", write-version=" + writeVersion);

    return writeVersion;
  }

  /**
   * Signal that the transaction is complete. This must always be invoked.
   *
   * @param committed whether the transaction was committed or rolled back
   * @param version   the transaction's read-version
   */
  synchronized void txnComplete(boolean committed, long version) {
    if (logger.isDebugEnabled())
      logger.debug("Transaction " + version + " completed " +
                   (committed ? "(committed)" : "(rolled back)"));

    boolean wasActive = activeTxns.remove(version);
    assert wasActive : "completed unknown transaction " + version +
                       (committed ? "(committed)" : "(rolled back)");

    if (writeLockHolder != version)
      return;           // never prepared (e.g. r/o txn, or rollback)

    if (committed && writeVersion >= 0)
      nextVersion = writeVersion + 1;
    writeVersion = -1;

    releaseWriteLock(version);
  }

  /**
   * Purge all old versions that are not being used anymore.
   *
   * @param lastCompletedVersion the version of the recently completed transaction; if there are
   *                             other, older transactions still active then the purge can be
   *                             avoided, i.e. this is just for optimization.
   */
  void purgeOldVersions(long lastCompletedVersion) {
    final long minVers;
    synchronized (this) {
      minVers = activeTxns.isEmpty() ? nextVersion : Collections.min(activeTxns);
    }

    if (minVers < lastCompletedVersion)
      return;           // we didn't release anything

    try {
      runInCon(new Action<Void>() {
        public Void run(Connection con) throws SQLException {
          if (logger.isDebugEnabled())
            logger.debug("Purging deleted blobs older than revision " + minVers);

          Statement stmt = con.createStatement();
          try {
            // clean out stale mapping entries
            ResultSet rs = stmt.executeQuery("SELECT appId, version FROM " + DEL_TABLE +
                                             " WHERE version < " + minVers);
            if (!rs.next())
              return null;

            PreparedStatement purge = con.prepareStatement(
                "DELETE FROM " + NAME_TABLE +
                " WHERE appId = ? AND (version < ? OR version = ? AND deleted <> 0)");
            try {
              do {
                purge.setString(1, rs.getString(1));
                purge.setLong(2, rs.getLong(2));
                purge.setLong(3, rs.getLong(2));
                purge.executeUpdate();
              } while (rs.next());
            } finally {
              purge.close();
            }

            // remove unreferenced blobs
            rs = stmt.executeQuery("SELECT storeId FROM " + DEL_TABLE +
                                   " WHERE version < " + minVers + " AND storeId IS NOT NULL");
            try {
              BlobStoreConnection bsc = wrappedStore.openConnection(null);
              try {
                while (rs.next()) {
                  String storeId = rs.getString(1);
                  if (logger.isTraceEnabled())
                    logger.trace("Purging deleted blob '" + storeId + "'");

                  try {
                    bsc.getBlob(URI.create(storeId), null).delete();
                  } catch (IOException ioe) {
                    logger.warn("Error purging blob '" + storeId + "'", ioe);
                  }
                }
              } finally {
                bsc.close();
              }
            } catch (IOException ioe) {
              logger.warn("Error opening connection to underlying store to purge old versions",
                          ioe);
            }

            // purge processed entries fromm the delete table
            stmt.executeUpdate("DELETE FROM " + DEL_TABLE + " WHERE version < " + minVers);

            return null;
          } finally {
            stmt.close();
          }
        }
      }, "Error purging old versions");
    } catch (Exception e) {
      logger.warn("Error purging old versions", e);
    }
  }

  private <T> T runInCon(Action<T> action, String errMsg) throws IOException {
    try {
      XAConnection xaCon;
      Connection   con;
      synchronized (dataSource) {
        xaCon = dataSource.getXAConnection();
        con   = xaCon.getConnection();
      }

      con.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);

      try {
        return action.run(con);
      } finally {
        xaCon.close();
      }
    } catch (SQLException sqle) {
      throw (IOException) new IOException(errMsg).initCause(sqle);
    }
  }

  private static interface Action<T> {
    public T run(Connection con) throws SQLException;
  }
}
