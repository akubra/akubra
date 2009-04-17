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

import java.io.Serializable;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.concurrent.ExecutionException;


/**
 * A listener for calls that the server side wishes to make on the client side.
 * This is mainly to reverse the call direction and avoid rmi-call-backs to the
 * client from the server side. This reversal is needed to make akubra-servers
 * work with clients that are behind firewalls or are on a NAT network.
 *
 * @author Pradeep Krishnan
 */
public interface RemoteCallListener extends Remote {

  /**
   * Waits for and gets the next operation to be executed on the client side.
   *
   * @return the next operation
   *
   * @throws InterruptedException if the wait is interrupted
   *
   * @throws RemoteException on an rmi transport error
   */
  public Operation<?> getNextOperation() throws InterruptedException, RemoteException;

  /**
   * Posts the result of a previous operation and waits for the caller to pick it up.
   *
   * @param result the result to be posted
   *
   * @throws InterruptedException if the wait is interrupted
   *
   * @throws RemoteException on an rmi transport error
   */
  public void postResult(Result<?> result) throws InterruptedException, RemoteException;

  /**
   * An operation to be performed on the client side.
   *
   * @param <T> the result type of the operation
   */
  public static interface Operation<T> extends Serializable {
  }

  /**
   * Result of an operation.
   *
   * @param <T> the return type of the result
   */
  public static class Result<T> implements Operation<T> {
    private static final long serialVersionUID = 1L;
    private final Throwable error;
    private final T result;

    /**
     * Creates an instance of an error Result.
     *
     * @param error the error
     */
    public Result(Throwable error) {
      this.error = error;
      this.result = null;
    }

    /**
     * Creates an instance of Result.
     *
     * @param result the result value
     */
    public Result(T result) {
      this.error = null;
      this.result = result;
    }

    /**
     * Creates an instance of void Result.
     */
    public Result() {
      this.error = null;
      this.result = null;
    }

    /**
     * Gets the result.
     *
     * @return the result
     *
     * @throws ExecutionException on any error posted on execution
     */
    public T get() throws ExecutionException {
      if (error != null)
        throw new ExecutionException(error);
      return result;
    }
  }
}
