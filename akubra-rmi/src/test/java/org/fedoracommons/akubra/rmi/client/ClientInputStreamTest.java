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
package org.fedoracommons.akubra.rmi.client;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import java.rmi.NoSuchObjectException;

import org.fedoracommons.akubra.rmi.remote.RemoteInputStream;
import org.fedoracommons.akubra.rmi.server.Exporter;
import org.fedoracommons.akubra.rmi.server.ServerInputStream;

import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

/**
 * Unit tests for ClientInputStream.
 *
 * @author Pradeep Krishnan
  */
public class ClientInputStreamTest {
  private Exporter                   exporter;
  private ServerInputStream          si;
  private ClientInputStream          ci;
  private final byte[]               buf = new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
  private final ByteArrayInputStream bi  = new ByteArrayInputStream(buf);

  @BeforeSuite
  public void setUp() throws Exception {
    exporter = new Exporter(0);
    si       = new ServerInputStream(bi, exporter);
    ci       = new ClientInputStream((RemoteInputStream) si.getExported());
  }

  @AfterSuite
  public void tearDown() throws Exception {
    ci.close();
  }

  @Test
  public void testClientInputStream() throws IOException {
    assertFalse(ci.markSupported());
  }

  @Test
  public void testRead() throws IOException {
    bi.reset();

    for (byte b : buf)
      assertEquals(b, ci.read());

    assertEquals(-1, ci.read());
  }

  @Test
  public void testReadByteArray() throws IOException {
    bi.reset();

    byte[] r = new byte[100];
    assertEquals(buf.length, ci.read(r));

    for (int i = 0; i < buf.length; i++)
      assertEquals(buf[i], r[i]);

    assertEquals(-1, ci.read());

    bi.reset();
    r = new byte[0];
    assertEquals(0, ci.read(r));
  }

  @Test
  public void testReadByteArrayIntInt() throws IOException {
    bi.reset();

    byte[] r = new byte[100];

    bogusIndex(r, -1, 10);
    bogusIndex(r, 10, -1);
    bogusIndex(r, -1, -1);
    bogusIndex(r, 50, 51);
    bogusIndex(r, 101, 1);
    bogusIndex(r, 0, 101);

    int off = r.length - buf.length;
    assertEquals(buf.length, ci.read(r, off, buf.length));

    for (int i = 0; i < buf.length; i++)
      assertEquals(buf[i], r[off + i]);

    assertEquals(-1, ci.read());
  }

  private void bogusIndex(byte[] b, int off, int len) {
    try {
      ci.read(b, off, len);
      fail("Failed to get expected exception for bogus values (" + b.length + ", " + off + ", "
           + len + ")");
    } catch (Exception e) {
    }
  }

  @Test
  public void testSkip() throws IOException {
    bi.reset();

    int skip = buf.length - 1;
    int n    = (int) ci.skip(skip);
    assertEquals(skip, n);

    for (int i = n; i < buf.length; i++)
      assertEquals(buf[i], ci.read());

    assertEquals(-1, ci.read());
  }

  @Test
  public void testClose() throws IOException {
    ServerInputStream si = new ServerInputStream(bi, exporter);
    ClientInputStream ci = new ClientInputStream((RemoteInputStream) si.getExported());
    bi.reset();
    assertEquals(buf[0], ci.read());
    ci.close();

    try {
      for (int i = 0; i < 10; i++) {
        try {
          Thread.sleep(Exporter.RETRY_DELAY * i);
        } catch (InterruptedException e) {
        }

        ci.read();
      }

      fail("Failed to get expected exception");
    } catch (NoSuchObjectException e) {
    }
  }
}
