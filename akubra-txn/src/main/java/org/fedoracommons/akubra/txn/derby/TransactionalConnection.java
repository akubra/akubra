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
import java.util.Iterator;

import javax.transaction.Transaction;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.fedoracommons.akubra.BlobStore;
import org.fedoracommons.akubra.txn.SQLTransactionalConnection;

/**
 * A connection for the transactional store.
 *
 * @author Ronald Tschalär
 */
public class TransactionalConnection extends SQLTransactionalConnection {
  private static final Log logger = LogFactory.getLog(TransactionalConnection.class);

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
    super(owner, bStore, con, tx);
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
      ResultSet rs = con.createStatement().executeQuery(query);
      return new RSBlobIdIterator(rs, true);
    } catch (SQLException sqle) {
      throw (IOException) new IOException("Error querying db").initCause(sqle);
    }
  }

  protected URI getRealId(URI blobId) throws IOException {
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

  protected void remNameEntry(URI ourId, URI storeId) throws IOException {
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

  protected void addNameEntry(URI ourId, URI storeId) throws IOException {
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
}
