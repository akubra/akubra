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
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;

import org.fedoracommons.akubra.rmi.remote.RemoteIterator;
import org.fedoracommons.akubra.rmi.server.Exporter;
import org.fedoracommons.akubra.rmi.server.ServerIterator;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

/**
 * Unit tests for ClientIterator.
 *
 * @author Pradeep Krishnan
  */
public class ClientIteratorTest {
  private Exporter               exporter;
  private ServerIterator<String> si;
  private ClientIterator<String> ci;
  private List<String>           source;

  @BeforeSuite
  public void setUpSuite() throws Exception {
    exporter   = new Exporter(0);
    source     = Arrays.asList("The quick brown fox jumped over a lazy dog.".split(" "));
  }

  @AfterSuite
  public void tearDownSuite() throws Exception {
    exporter = null;
  }

  @SuppressWarnings("unchecked")
  @BeforeMethod
  public void setUpTest() throws Exception {
    si   = new ServerIterator<String>(source.iterator(), exporter);
    ci   = new ClientIterator<String>((RemoteIterator<String>) si.getExported(), 8);
  }

  @AfterMethod
  public void tearDownTest() throws Exception {
    si.unExport(false);
    si   = null;
    ci   = null;
  }

  @Test
  public void testHasNext() {
    assertTrue(ci.hasNext());
    assertTrue(ci.hasNext());

    for (String v : source) {
      assertTrue(ci.hasNext());
      assertEquals(v, ci.next());
    }

    assertFalse(ci.hasNext());
    assertFalse(ci.hasNext());
  }

  @Test
  public void testNext() {
    for (String v : source) {
      assertEquals(v, ci.next());
    }

    try {
      ci.next();
      fail("Failed to get NoSuchElementException");
    } catch (NoSuchElementException e) {
    } catch (Exception e) {
      fail("Failed to get NoSuchElementException");
    }
  }

  @Test
  public void testRemove() {
    for (String v : source) {
      assertEquals(v, ci.next());

      try {
        ci.remove();
        fail("Failed to get  UnsupportedOperationException");
      } catch (UnsupportedOperationException e) {
      } catch (Exception e) {
        fail("Failed to get  UnsupportedOperationException");
      }
    }
  }
}
