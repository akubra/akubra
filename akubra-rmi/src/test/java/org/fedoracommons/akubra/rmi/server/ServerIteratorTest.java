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
package org.fedoracommons.akubra.rmi.server;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.net.URI;

import java.rmi.RemoteException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.fedoracommons.akubra.rmi.remote.RemoteIterator;

import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

/**
 * Unit tests for ServerIterator.
 *
 * @author Pradeep Krishnan
  */
public class ServerIteratorTest {
  private Exporter exporter;

  @BeforeSuite
  public void setUp() throws Exception {
    exporter = new Exporter(0);
  }

  @AfterSuite
  public void tearDown() throws Exception {
    exporter = null;
  }

  @Test
  public void testServerIterator() throws RemoteException {
    ServerIterator<URI> it = new ServerIterator<URI>(new ArrayList<URI>().iterator(), exporter);
    assertTrue(it.getExported() instanceof RemoteIterator);
  }

  @Test
  public void testNext() throws RemoteException {
    List<String>           l  = Arrays.asList("quick", "brown", "fox");
    ServerIterator<String> it = new ServerIterator<String>(l.iterator(), exporter);

    for (int v : new int[] { -1, 0 }) {
      try {
        it.next(-1);
        fail("Failed to recieve expected exception for batch-size: " + v);
      } catch (Exception e) {
      }
    }

    List<String> n = it.next(10);
    assertEquals(l, n);
    n = it.next(10);
    assertTrue(n.isEmpty());
  }

  @Test
  public void testUnExport() throws RemoteException {
    List<String>           l      = Arrays.asList("quick", "brown", "fox");
    final boolean[]        closed = new boolean[] { false };
    ServerIterator<String> it     =
      new ServerIterator<String>(l.iterator(), exporter) {
        private static final long serialVersionUID = 1L;

        @Override
        public void unExport(boolean force) {
          closed[0] = true;
          super.unExport(force);
        }
      };

    assertFalse(closed[0]);

    for (String v : l) {
      assertEquals(v, it.next(1).get(0));
      assertFalse(closed[0]);
    }

    assertTrue(it.next(1).isEmpty());
    assertTrue(closed[0]);
  }
}
