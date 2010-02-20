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
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import org.akubraproject.rmi.remote.PartialBuffer;
import org.akubraproject.rmi.remote.RemoteInputStream;

import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

/**
 * Unit tests for ServerInputStream.
 *
 * @author Pradeep Krishnan
 */
public class ServerInputStreamTest {
  private Exporter                   exporter;
  private ServerInputStream          si;
  private final byte[]               buf = new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
  private final ByteArrayInputStream bi  = new ByteArrayInputStream(buf);

  @BeforeSuite
  public void setUp() throws Exception {
    exporter   = new Exporter(0);
    si         = new ServerInputStream(bi, exporter);
  }

  @AfterSuite
  public void tearDown() throws Exception {
    si.close();
    si         = null;
    exporter   = null;
  }

  @Test
  public void testServerInputStream() throws IOException {
    ServerInputStream si = new ServerInputStream(bi, exporter);
    assertTrue(si.getExported() instanceof RemoteInputStream);
    si.close();
    si = null;
  }

  @Test
  public void testRead() throws IOException {
    bi.reset();

    try {
      si.read(-1);
      fail("Failed to throw exception for read with negative length");
    } catch (Exception e) {
    }

    PartialBuffer pb = si.read(0);
    assertNotNull(pb);
    assertEquals(0, pb.getLength());
    assertEquals(0, pb.getOffset());
    pb = si.read(1);
    assertNotNull(pb);
    assertEquals(1, pb.getLength());
    assertEquals(0, pb.getOffset());
    assertEquals(buf[0], pb.getBuffer()[0]);
    pb = si.read(10);
    assertNotNull(pb);
    assertEquals(10, pb.getLength());
    assertEquals(0, pb.getOffset());

    for (int i = 0; i < 10; i++)
      assertEquals(buf[i + 1], pb.getBuffer()[i]);

    pb = si.read(10);
    assertNull(pb);
  }

  @Test
  public void testReadInt() throws IOException {
    bi.reset();

    for (byte b : buf)
      assertEquals(b, si.read());

    assertEquals(-1, si.read());
  }

  @Test
  public void testSkip() throws IOException {
    bi.reset();
    assertEquals(buf.length - 1, si.skip(buf.length - 1));
    assertEquals(buf[buf.length - 1], si.read());
    assertEquals(-1, si.read());
  }

  @Test
  public void testClose() throws IOException {
    final boolean[]      closed = new boolean[] { false, false };
    ByteArrayInputStream bi     =
      new ByteArrayInputStream(buf) {
        @Override
        public void close() throws IOException {
          closed[0] = true;
          super.close();
        }
      };

    ServerInputStream si =
      new ServerInputStream(bi, exporter) {
        private static final long serialVersionUID = 1L;

        @Override
        public void unExport(boolean force) {
          closed[1] = true;
          super.unExport(force);
        }
      };

    si.read(100);
    assertFalse(closed[0]);
    assertFalse(closed[1]);
    si.close();
    assertTrue(closed[0]);
    assertTrue(closed[1]);
  }

  @Test
  public void testUnreferenced() throws IOException {
    final boolean[]      closed = new boolean[] { false, false };
    ByteArrayInputStream bi     =
      new ByteArrayInputStream(buf) {
        @Override
        public void close() throws IOException {
          closed[0] = true;
          super.close();
        }
      };

    ServerInputStream si =
      new ServerInputStream(bi, exporter) {
        private static final long serialVersionUID = 1L;

        @Override
        public void unExport(boolean force) {
          closed[1] = true;
          super.unExport(force);
        }
      };

    si.read(100);
    assertFalse(closed[0]);
    assertFalse(closed[1]);
    si.unreferenced();
    assertTrue(closed[0]);
    assertTrue(closed[1]);
  }
}
