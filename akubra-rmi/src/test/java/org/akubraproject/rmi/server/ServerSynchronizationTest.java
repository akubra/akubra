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
package org.akubraproject.rmi.server;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.reset;
import static org.easymock.EasyMock.verify;

import static org.testng.Assert.assertTrue;

import java.rmi.RemoteException;

import javax.transaction.Synchronization;

import org.akubraproject.rmi.remote.RemoteSynchronization;

import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

/**
 * Unit tests for ServerSynchronization.
 *
 * @author Pradeep Krishnan
  */
public class ServerSynchronizationTest {
  private Exporter              exporter;
  private ServerSynchronization ss;
  private Synchronization       sync;

  @BeforeSuite
  public void setUp() throws Exception {
    exporter   = new Exporter(0);
    sync       = createMock(Synchronization.class);
    ss         = new ServerSynchronization(sync, exporter);
  }

  @AfterSuite
  public void tearDown() throws Exception {
    ss.unExport(false);
  }

  @Test
  public void testServerSynchronization() {
    assertTrue(ss.getExported() instanceof RemoteSynchronization);
  }

  @Test
  public void testAfterCompletion() throws RemoteException {
    reset(sync);
    sync.afterCompletion(42);
    replay(sync);

    ss.afterCompletion(42);
    verify(sync);
  }

  @Test
  public void testBeforeCompletion() throws RemoteException {
    reset(sync);
    sync.beforeCompletion();
    replay(sync);

    ss.beforeCompletion();
    verify(sync);
  }
}
