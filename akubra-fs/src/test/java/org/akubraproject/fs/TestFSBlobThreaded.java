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
package org.akubraproject.fs;

import java.io.File;
import java.io.OutputStream;

import java.lang.reflect.Method;

import java.net.URI;

import com.google.testing.threadtester.Breakpoint;
import com.google.testing.threadtester.ClassInstrumentation;
import com.google.testing.threadtester.CodePosition;
import com.google.testing.threadtester.Instrumentation;
import com.google.testing.threadtester.ObjectInstrumentation;
import com.google.testing.threadtester.ThreadedAfter;
import com.google.testing.threadtester.ThreadedBefore;
import com.google.testing.threadtester.ThreadedTest;
import com.google.testing.threadtester.ThreadedTestRunner;

import org.akubraproject.impl.StreamManager;
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;

/**
 * Unit tests for threaded {@link FSBlob}.
 *
 * @author Scott Prater
 */
public class TestFSBlobThreaded {

  private static URI id;
  private static File baseDir;
  private static FSBlobStoreConnection conn;
  private static StreamManager mgr;
  private static FSBlob fsblob;
  private static boolean threadFailed = false;
  private final ThreadedTestRunner runner = new ThreadedTestRunner();

  @Test
  public void testThreading() {
    runner.runTests(getClass(), FSBlob.class);
  }

  @ThreadedBefore
  public void before() throws Exception {
    id = new URI("urn:example:store");
    baseDir = FSTestUtil.createTempDir();
    FSBlobStore store = new FSBlobStore(id, baseDir);
    mgr = new StreamManager();
    conn = new FSBlobStoreConnection(store, baseDir, mgr, true);
  }

  @ThreadedTest
  public void runThreadedTest() throws Exception {

    URI uri = new URI("file:foo/bar/baz/testblob");
    fsblob = new FSBlob(conn, baseDir, uri, mgr, null);

    CodePosition position = getDirExistsCodePosition();

    Runnable task = new Runnable() {
      public void run() {
        try {
          OutputStream out = fsblob.openOutputStream(-1, true);
          out.write(new String("fsblob test one").getBytes());
          out.close();
        } catch (Exception e) {
          e.printStackTrace();
          throw new RuntimeException("Test Thread 1 exception: " + e.getMessage());
        }
      }
    };

    Thread thread1 = new Thread(task);
    Thread.setDefaultUncaughtExceptionHandler(new ThreadExceptionHandler());

    ObjectInstrumentation<FSBlob> instrumented = Instrumentation.getObjectInstrumentation(fsblob);
    Breakpoint bp = instrumented.createBreakpoint(position, thread1);

    thread1.start();
    bp.await();

    OutputStream out = fsblob.openOutputStream(-1, true);
    out.write(new String("fsblob test two").getBytes());
    out.close();

    bp.resume();
    thread1.join();

    assertFalse(threadFailed);
  }

  @ThreadedAfter
  public static void destroy() {
    conn.close();
    FSTestUtil.rmdir(baseDir);
  }

  private static CodePosition getDirExistsCodePosition() throws Exception {
    ClassInstrumentation instr = Instrumentation.getClassInstrumentation(FSBlob.class);
    Method makeParentDirs = FSBlob.class.getDeclaredMethod("makeParentDirs", File.class);
    Method exists = File.class.getDeclaredMethod("exists");
    CodePosition position = instr.afterCall(makeParentDirs, exists);
    
    return position;
  }

  private static void setThreadFailed(boolean failed) {
    threadFailed = failed;
  }

  private class ThreadExceptionHandler implements Thread.UncaughtExceptionHandler {
    public void uncaughtException(Thread t, Throwable e) {
      e.printStackTrace();
      TestFSBlobThreaded.setThreadFailed(true);
    }
  }
}

