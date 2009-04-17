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
package org.fedoracommons.akubra.rmi.remote;

/**
 * A listener for calls made on a Transaction object.
 *
 * @author Pradeep Krishnan
 */
public interface RemoteTransactionListener extends RemoteCallListener {

  /**
   * Abstract base class for operations involving an XAResource.
   *
   * @param <T> the return value type
   */
  public abstract static class XAResourceOp<T> implements Operation<T> {
    private static final long serialVersionUID = 1L;
    private final RemoteXAResource xa;

    /**
     * Creates an instance of XAResourceOp.
     *
     * @param xa the server side XAResource
     */
    protected XAResourceOp(RemoteXAResource xa) {
      this.xa = xa;
    }

    /**
     * Gets the server side XA resource.
     *
     * @return the server side XA resource
     */
    public RemoteXAResource getXAResource() {
      return xa;
    }
  }

  /**
   * Encapsulates an {@link javax.transaction.Transaction#enlistResource(javax.transaction.xa.XAResource)}
   * operation.
   */
  public static class EnlistXAResource extends XAResourceOp<Boolean> {
    private static final long serialVersionUID = 1L;

    /**
     * Creates an EnlistXAResource instance.
     *
     * @param xa a server side XAResource
     */
    public EnlistXAResource(RemoteXAResource xa) {
      super(xa);
    }
  }

  /**
   * Encapsulates a {@link javax.transaction.Transaction#delistResource(javax.transaction.xa.XAResource, int)}
   * operation.
   */
  public static class DelistXAResource extends XAResourceOp<Boolean> {
    private static final long serialVersionUID = 1L;
    private final int flags;

    /**
     * Creates an DelistXAResource instance.
     *
     * @param xa a server side XAResource
     * @param flags flags for the delistResource call
     */
    public DelistXAResource(RemoteXAResource xa, int flags) {
      super(xa);
      this.flags = flags;
    }

    /**
     * Gets the flags for the delistResource call.
     *
     * @return the flags
     */
    public int getFlags() {
      return flags;
    }
  }

  /**
   * Encapsulates a {@link javax.transaction.Transaction#getStatus()}
   * operation.
   */
  public static class GetStatus implements Operation<Integer> {
    private static final long serialVersionUID = 1L;
  }

  /**
   * Encapsulates a {@link javax.transaction.Transaction#rollback()}
   * operation.
   */
  public static class Rollback implements Operation<Void> {
    private static final long serialVersionUID = 1L;
  }

  /**
   * Encapsulates a {@link javax.transaction.Transaction#registerSynchronization(javax.transaction.Synchronization)}
   * operation.
   */
  public static class RegisterSynchronization implements Operation<Void> {
    private static final long serialVersionUID = 1L;
    private final RemoteSynchronization rs;

    /**
     * Creates a RegisterSynchronization instance.
     *
     * @param rs a server side Synchronization object
     */
    public RegisterSynchronization(RemoteSynchronization rs) {
      this.rs = rs;
    }

    /**
     * Gets the server side Synchronization object.
     *
     * @return the Synchronization object
     */
    public RemoteSynchronization getSynchronization() {
      return rs;
    }
  }

  public static class TakeConnection implements Operation<Void> {
    private static final long serialVersionUID = 1L;
    private final RemoteConnection con;

    /**
     * Creates a TakeConnection instance.
     *
     * @param con a server connection
     */
    public TakeConnection(RemoteConnection con) {
      this.con = con;
    }

    /**
     * Gets the server side Connection object.
     *
     * @return the server connection object
     */
    public RemoteConnection getConnection() {
      return con;
    }
  }
}
