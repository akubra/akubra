/* $HeadURL::                                                                            $
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
package org.akubraproject.rmi.server;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.akubraproject.rmi.remote.PartialBuffer;
import org.akubraproject.rmi.remote.RemoteOutputStream;

import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

/**
 * Unit tests for ServerOutputStream.
 *
 * @author Pradeep Krishnan
 */
public class ServerOutputStreamTest {
  private Exporter                    exporter;
  private ServerOutputStream          so;
  private final ByteArrayOutputStream bo = new ByteArrayOutputStream();

  @BeforeSuite
  public void setUp() throws Exception {
    exporter   = new Exporter(0);
    so         = new ServerOutputStream(bo, exporter);
  }

  @AfterSuite
  public void tearDown() throws Exception {
    so.close();
    so         = null;
    exporter   = null;
  }

  @Test
  public void testServerOutputStream() throws IOException {
    assertTrue(so.getExported() instanceof RemoteOutputStream);
  }

  @Test
  public void testWriteInt() throws IOException {
    bo.reset();
    so.write('a');

    byte[] b = bo.toByteArray();
    assertNotNull(b);
    assertEquals(1, b.length);
    assertEquals('a', b[0]);
  }

  @Test
  public void testWriteByteArray() throws IOException {
    bo.reset();

    byte[] in = "Hello World".getBytes();
    so.write(in);

    byte[] b = bo.toByteArray();
    assertNotNull(b);
    assertEquals(in, b);

    bo.reset();
    in = new byte[0];
    so.write(in);
    b = bo.toByteArray();
    assertNotNull(b);
    assertEquals(in, b);
  }

  @Test
  public void testWritePartialBuffer() throws IOException {
    bo.reset();

    byte[]        in = "Hello World".getBytes();
    PartialBuffer pb = new PartialBuffer(new byte[in.length * 3], in.length, in.length);
    System.arraycopy(in, 0, pb.getBuffer(), pb.getOffset(), pb.getLength());
    so.write(pb);

    byte[] b = bo.toByteArray();
    assertNotNull(b);
    assertEquals(in, b);
  }

  @Test
  public void testFlush() throws IOException {
    final boolean[]    flushed = new boolean[] { false };
    OutputStream       out     =
      new OutputStream() {
        @Override
        public void flush() {
          flushed[0] = true;
        }

        @Override
        public void write(int b) throws IOException {
        }
      };

    ServerOutputStream so = new ServerOutputStream(out, exporter);
    assertFalse(flushed[0]);
    so.flush();
    assertTrue(flushed[0]);
  }

  @Test
  public void testClose() throws IOException {
    final boolean[]    closed = new boolean[] { false, false };
    OutputStream       out    =
      new OutputStream() {
        @Override
        public void close() throws IOException {
          closed[0] = true;
        }

        @Override
        public void write(int b) throws IOException {
        }
      };

    ServerOutputStream so =
      new ServerOutputStream(out, exporter) {
        private static final long serialVersionUID = 1L;

        @Override
        public void unExport(boolean force) {
          closed[1] = true;
          super.unExport(force);
        }
      };

    assertFalse(closed[0]);
    assertFalse(closed[1]);
    so.close();
    assertTrue(closed[0]);
    assertTrue(closed[1]);
  }

  @Test
  public void testUnreferenced() throws IOException {
    final boolean[]    closed = new boolean[] { false, false };
    OutputStream       out    =
      new OutputStream() {
        @Override
        public void close() throws IOException {
          closed[0] = true;
        }

        @Override
        public void write(int b) throws IOException {
        }
      };

    ServerOutputStream so =
      new ServerOutputStream(out, exporter) {
        private static final long serialVersionUID = 1L;

        @Override
        public void unExport(boolean force) {
          closed[1] = true;
          super.unExport(force);
        }
      };

    assertFalse(closed[0]);
    assertFalse(closed[1]);
    so.unreferenced();
    assertTrue(closed[0]);
    assertTrue(closed[1]);
  }
}
