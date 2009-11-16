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
package org.akubraproject.rmi.client;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import java.rmi.NoSuchObjectException;

import org.akubraproject.rmi.remote.RemoteOutputStream;
import org.akubraproject.rmi.server.Exporter;
import org.akubraproject.rmi.server.ServerOutputStream;

import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

/**
 * Unit tests for ClientOutputStream.
 *
 * @author Pradeep Krishnan
  */
public class ClientOutputStreamTest {
  private Exporter                    exporter;
  private ServerOutputStream          so;
  private ClientOutputStream          co;
  private final ByteArrayOutputStream bo = new ByteArrayOutputStream();

  @BeforeSuite
  public void setUp() throws Exception {
    exporter = new Exporter(0);
    so       = new ServerOutputStream(bo, exporter);
    co       = new ClientOutputStream((RemoteOutputStream) so.getExported());
  }

  @AfterSuite
  public void tearDown() throws Exception {
    co.close();
  }

  @Test
  public void testClientOutputStream() throws IOException {
  }

  @Test
  public void testWriteInt() throws IOException {
    bo.reset();
    co.write('a');

    byte[] b = bo.toByteArray();
    assertNotNull(b);
    assertEquals(1, b.length);
    assertEquals('a', b[0]);
  }

  @Test
  public void testWriteByteArray() throws IOException {
    bo.reset();

    byte[] in = "Hello World".getBytes();
    co.write(in);

    byte[] b = bo.toByteArray();
    assertNotNull(b);
    assertEquals(in, b);

    bo.reset();
    in = new byte[0];
    co.write(in);
    b = bo.toByteArray();
    assertNotNull(b);
    assertEquals(in, b);
  }

  @Test
  public void testWriteByteArrayIntInt() throws IOException {
    bo.reset();

    byte[] in = "Hello World".getBytes();
    byte[] pb = new byte[in.length * 3];
    System.arraycopy(in, 0, pb, in.length, in.length);
    co.write(pb, in.length, in.length);

    byte[] b = bo.toByteArray();
    assertNotNull(b);
    assertEquals(in, b);

    byte[] r = new byte[100];
    bogusIndex(r, -1, 10);
    bogusIndex(r, 10, -1);
    bogusIndex(r, -1, -1);
    bogusIndex(r, 50, 51);
    bogusIndex(r, 101, 1);
    bogusIndex(r, 0, 101);

    co.write(pb, in.length, 0);
    b = bo.toByteArray();
    assertNotNull(b);
    assertEquals(in, b);
  }

  private void bogusIndex(byte[] b, int off, int len) {
    try {
      co.write(b, off, len);
      fail("Failed to get expected exception for bogus values (" + b.length + ", " + off + ", "
           + len + ")");
    } catch (Exception e) {
    }
  }

  @Test
  public void testFlush() throws IOException {
    final boolean[]       flushed = new boolean[] { false };
    ByteArrayOutputStream bo      =
      new ByteArrayOutputStream() {
        @Override
        public void flush() {
          flushed[0] = true;
        }
      };

    ServerOutputStream so = new ServerOutputStream(bo, exporter);
    ClientOutputStream co = new ClientOutputStream((RemoteOutputStream) so.getExported());
    assertFalse(flushed[0]);
    co.flush();
    assertTrue(flushed[0]);
    co.close();
  }

  @Test
  public void testClose() throws IOException {
    ServerOutputStream so = new ServerOutputStream(bo, exporter);
    ClientOutputStream co = new ClientOutputStream((RemoteOutputStream) so.getExported());
    co.close();

    try {
      for (int i = 0; i < 10; i++) {
        try {
          Thread.sleep(Exporter.RETRY_DELAY * i);
        } catch (InterruptedException e) {
        }

        co.write('a');
      }

      fail("Failed to get expected exception");
    } catch (NoSuchObjectException e) {
    }
  }
}
