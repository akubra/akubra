/* $HeadURL$
 * $Id$
 *
 * Copyright (c) 2009-2010 DuraSpace
 * http://duraspace.org
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
package org.akubraproject.rmi.remote;

import java.io.Serializable;

import javax.transaction.xa.Xid;

/**
 * A serializable copy for non serializable xids so that they can be send across RMI.
 *
 * @author Pradeep Krishnan
 */
public class SerializedXid implements Xid, Serializable {
  private static final long serialVersionUID = 1L;
  private final byte[]      bq;
  private final byte[]      gtid;
  private final int         fmtId;
  private transient String  string;

  /**
   * Creates a new SerializedXid object.
   *
   * @param xid the xid to copy from (usually non-serializable)
   */
  public SerializedXid(Xid xid) {
    bq      = xid.getBranchQualifier();
    gtid    = xid.getGlobalTransactionId();
    fmtId   = xid.getFormatId();
  }

  public byte[] getBranchQualifier() {
    return bq;
  }

  public int getFormatId() {
    return fmtId;
  }

  public byte[] getGlobalTransactionId() {
    return gtid;
  }

  @Override
  public boolean equals(Object other) {
    return (other instanceof SerializedXid) && toString().equals(other.toString());
  }

  @Override
  public String toString() {
    if (string == null)
      string = "[fmtId=" + fmtId + ", bq=" + toString(bq) + ", gtid=" + toString(gtid) + "]";

    return string;
  }

  private static String toString(byte[] ba) {
    if (ba == null)
      return null;

    if (ba.length == 0)
      return "[]";

    StringBuilder builder = new StringBuilder();
    builder.append("[");

    for (byte b : ba)
      builder.append(b).append(",");

    builder.setLength(builder.length() - 1);

    return builder.append("]").toString();
  }

  @Override
  public int hashCode() {
    return toString().hashCode();
  }
}
