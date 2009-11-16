/* $HeadURL$
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

import static org.easymock.classextension.EasyMock.createMock;
import static org.easymock.classextension.EasyMock.replay;
import static org.easymock.classextension.EasyMock.reset;
import static org.easymock.classextension.EasyMock.verify;

import javax.transaction.Synchronization;

import org.akubraproject.rmi.remote.RemoteSynchronization;
import org.akubraproject.rmi.server.Exporter;
import org.akubraproject.rmi.server.ServerSynchronization;

import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

/**
 * Unit tests for ClientSynchronization.
 *
 * @author Pradeep Krishnan
  */
public class ClientSynchronizationTest {
  private Exporter              exporter;
  private ServerSynchronization ss;
  private ClientSynchronization cs;
  private Synchronization       sync;

  @BeforeSuite
  public void setUp() throws Exception {
    exporter   = new Exporter(0);
    sync       = createMock(Synchronization.class);
    ss         = new ServerSynchronization(sync, exporter);
    cs         = new ClientSynchronization((RemoteSynchronization) ss.getExported());
  }

  @AfterSuite
  public void tearDown() throws Exception {
    ss.unExport(false);
  }

  @Test
  public void testAfterCompletion() {
    reset(sync);
    sync.afterCompletion(42);
    replay(sync);

    cs.afterCompletion(42);
    verify(sync);
  }

  @Test
  public void testBeforeCompletion() {
    reset(sync);
    sync.beforeCompletion();
    replay(sync);

    cs.beforeCompletion();
    verify(sync);
  }
}
