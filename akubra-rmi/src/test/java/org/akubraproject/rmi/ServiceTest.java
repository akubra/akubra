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
package org.akubraproject.rmi;

import java.io.IOException;

import java.net.ServerSocket;
import java.net.URI;
import java.net.URISyntaxException;

import java.rmi.NotBoundException;
import java.rmi.registry.Registry;

import org.akubraproject.BlobStore;
import org.akubraproject.mem.MemBlobStore;

import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

/**
 * Tests export, locate and un-export of akubra-rmi server and client.
 *
 * @author Pradeep Krishnan
 */
public class ServiceTest {
  private MemBlobStore mem;
  private int          reg;

  /**
   * Setup before all tests.
   *
   */
  @BeforeSuite
  public void setUp() throws Exception {
    mem   = new MemBlobStore();
    reg   = freePort();
  }

  /**
   * Tear down after all tests.
   *
   */
  @AfterSuite
  public void tearDown() throws Exception {
  }

  /**
   * Tests export, access and unexport with defaults.
   *
   */
  @Test
  public void testDefault() throws NotBoundException, IOException {
    if (!isFree(Registry.REGISTRY_PORT))
      return;  // skip this test

    AkubraRMIServer server = new AkubraRMIServer(mem);
    try {
      BlobStore store = AkubraRMIClient.create();
      store.openConnection(null, null).close();
    } finally {
      server.shutDown(false);
    }
  }

  /**
   * Tests export, access and unexport with a specific registry port.
   *
   */
  @Test
  public void testWithPort() throws NotBoundException, IOException {
    AkubraRMIServer server = new AkubraRMIServer(mem, reg);

    try {
      BlobStore store = AkubraRMIClient.create(reg);
      store.openConnection(null, null).close();
    } finally {
      server.shutDown(true);
    }
  }

  /**
   * Tests export, access and unexport with a specific registry port and a specific service
   * name.
   *
   */
  @Test
  public void testWithNameAndPort() throws NotBoundException, IOException, URISyntaxException {
    AkubraRMIServer server = new AkubraRMIServer(mem, "service-test-with-name-and-port", reg);

    try {
      BlobStore store = AkubraRMIClient.create("service-test-with-name-and-port", reg);
      store.openConnection(null, null).close();
    } finally {
      server.shutDown(true);
    }
  }

  /**
   * Tests export, access and unexport with a specific registry port and a specific service
   * name and with different ports for RMI registry and akubra-rmi-server.
   *
   */
  @Test
  public void testWithNameAndAlternatePorts()
                                     throws NotBoundException, IOException, URISyntaxException {
    AkubraRMIServer server = new AkubraRMIServer(mem, "test-with-name-and-alternate-ports", reg, freePort());

    try {
      BlobStore store = AkubraRMIClient.create("test-with-name-and-alternate-ports", reg);
      store.openConnection(null, null).close();
    } finally {
      server.shutDown(true);
    }
  }

  /**
   * Tests export, access and unexport with a specific registry port and a specific service
   * name. and with different ports for RMI registry and akubra-rmi-server.
   *
   */
  @Test
  public void testLookupByUri() throws NotBoundException, IOException, URISyntaxException {
    AkubraRMIServer server = new AkubraRMIServer(mem, "test-uri-lookup", reg, freePort());

    try {
      BlobStore store =
        AkubraRMIClient.create(URI.create("rmi://localhost:" + reg + "/test-uri-lookup"));
      store.openConnection(null, null).close();
    } finally {
      server.shutDown(false);
    }
  }

  public static int freePort() throws IOException {
    ServerSocket s    = new ServerSocket(0);
    int          port = s.getLocalPort();
    s.close();

    return port;
  }

  private boolean isFree(int port) {
    try {
      new ServerSocket(port).close();
      return true;
    } catch (Exception e) {
      return false;
    }
  }
}
