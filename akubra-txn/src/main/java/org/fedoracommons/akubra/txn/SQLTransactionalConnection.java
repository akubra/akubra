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

import java.io.IOException;
import java.net.URI;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import javax.transaction.Transaction;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.fedoracommons.akubra.BlobStore;
import org.fedoracommons.akubra.BlobStoreConnection;

/**
 * A basic superclass for sql-based transactional store connections. This just closes the sql
 * connection at the end and provides an Iterator implementation for implementing
 * {@link BlobStoreConnection#listBlobIds listBlobIds}.
 *
 * @author Ronald Tschalär
 */
public abstract class SQLTransactionalConnection extends AbstractTransactionalConnection {
  private static final Log logger = LogFactory.getLog(SQLTransactionalConnection.class);

  /** The db connection being used */
  protected final Connection con;

  /**
   * Create a new sql-based transactional connection.
   *
   * @param owner   the blob-store we belong to
   * @param bStore  the underlying blob-store to use
   * @param con     the db connection to use
   * @param tx      the transaction we belong to
   * @throws IOException if an error occurs initializing this connection
   */
  protected SQLTransactionalConnection(BlobStore owner, BlobStore bStore, Connection con,
                                       Transaction tx) throws IOException {
    super(owner, bStore, tx);
    this.con = con;
  }

  //@Override
  public void close() {
    if (logger.isDebugEnabled())
      logger.debug("closing connection " + this);

    super.close();

    try {
      con.close();
    } catch (SQLException sqle) {
      logger.error("Error closing db connection", sqle);
    }
  }

  /**
   * A ResultSet based Iterator of blob-id's. Useful for {@link BlobStoreConnection#listBlobIds
   * listBlobIds}.
   *
   * @author Ronald Tschalär
   */
  protected static class RSBlobIdIterator implements Iterator<URI> {
    private final ResultSet rs;
    private final boolean   closeStmt;
    private       boolean   hasNext;

    /**
     * Create a new iterator.
     *
     * @param rs        the underlying result-set to use; the result-set must have one column such
     *                  that <code>rs.getString(1)</code> returns a URI.
     * @param closeStmt whether to close the associated statement when closing the result-set at
     *                  the end of the iteration.
     * @throws SQLException if thrown while attempting to advance to the first row
     */
    public RSBlobIdIterator(ResultSet rs, boolean closeStmt) throws SQLException {
      this.rs        = rs;
      this.closeStmt = closeStmt;
      this.hasNext   = rs.next();
    }

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

        if (!hasNext)
          close();

        return res;
      } catch (SQLException sqle) {
        throw new RuntimeException("error reading db results", sqle);
      }
    }

    //@Override
    public void remove() throws UnsupportedOperationException {
      throw new UnsupportedOperationException();
    }

    /**
     * Close the underlying result-set and statement (if <var>closeStmt</var> is true).
     */
    protected void close() {
      try {
        rs.close();
      } catch (SQLException sqle2) {
        logger.error("Error closing result-set", sqle2);
      }

      try {
        if (closeStmt)
          rs.getStatement().close();
      } catch (SQLException sqle2) {
        logger.error("Error closing statement", sqle2);
      }
    }
  }
}
