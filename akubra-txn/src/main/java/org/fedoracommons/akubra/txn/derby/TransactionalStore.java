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
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import javax.sql.XAConnection;
import javax.transaction.Transaction;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.derby.jdbc.EmbeddedXADataSource;

import org.fedoracommons.akubra.BlobStoreConnection;
import org.fedoracommons.akubra.txn.AbstractTransactionalStore;

/**
 * A simple transactional store using Derby db for the transaction logging and id mappings.
 *
 * <p>This sets derby to use isolation-level serializable.
 *
 * @author Ronald Tschalär
 */
public class TransactionalStore extends AbstractTransactionalStore {
  /** The SQL table used by this store */
  public static final String NAME_TABLE = "NameMap";

  private static final Log logger = LogFactory.getLog(TransactionalStore.class);

  private final EmbeddedXADataSource dataSource;

  /**
   * Create a new transactional store. Exactly one backing store must be set before this can
   * be used.
   *
   * @param id    the id of this store
   * @param dbDir the directory to use to store the transaction information
   */
  public TransactionalStore(URI id, String dbDir) throws IOException {
    super(id);

    dataSource = new EmbeddedXADataSource();
    dataSource.setDatabaseName(dbDir);
    dataSource.setCreateDatabase("create");

    Runtime.getRuntime().addShutdownHook(new Thread() {
      public void run() {
        try {
          DriverManager.getConnection("jdbc:derby:;shutdown=true");
        } catch (Exception e) {
          logger.warn("Error shutting down derby", e);
        }
      }
    });

    createTables();
  }

  private void createTables() throws IOException {
    try {
      Connection con = dataSource.getXAConnection().getConnection();
      try {
        // test if table exists
        ResultSet rs = con.getMetaData().getTables(null, null, NAME_TABLE, null);
        try {
          if (rs.next())
            return;
        } finally {
          rs.close();
        }

        // nope, so create it
        Statement stmt = con.createStatement();
        try {
          stmt.execute("CREATE TABLE " + NAME_TABLE +
                       " (appId VARCHAR(1000), storeId VARCHAR(1000))");
        } finally {
          stmt.close();
        }
      } finally {
        con.close();
      }
    } catch (SQLException sqle) {
      throw (IOException) new IOException("table " + NAME_TABLE + " setup failed").initCause(sqle);
    }
  }

  /**
   * @throws IllegalStateException if no backing store has been set yet
   */
  //@Override
  public BlobStoreConnection openConnection(Transaction tx)
      throws IllegalStateException, IOException {
    if (wrappedStore == null)
      throw new IllegalStateException("no backing store has been set yet");
    started = true;

    try {
      XAConnection xaCon = dataSource.getXAConnection();
      Connection con = xaCon.getConnection();
      con.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);

      tx.enlistResource(xaCon.getXAResource());

      return new TransactionalConnection(this, wrappedStore, con, tx);
    } catch (IOException ioe) {
      throw ioe;
    } catch (Exception e) {
      throw (IOException) new IOException("Error connecting to db").initCause(e);
    }
  }
}
