/*
 * Copyright 2018 StreamSets Inc.
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
package com.streamsets.datacollector.usagestats;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.streamsets.datacollector.bundles.SupportBundleManager;
import com.streamsets.datacollector.json.ObjectMapperFactory;
import com.streamsets.datacollector.main.BuildInfo;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.lib.security.http.RestClient;
import com.streamsets.pipeline.lib.executor.SafeScheduledExecutorService;
import org.apache.commons.io.IOUtils;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.Servlet;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class TestStatsCollectorTask {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final Map<String, Object> DEFAULT_SYS_INFO_MAP = ImmutableMap.of("cloudProvider", SysInfo.UNKNOWN);
  private static String POST_TELEMETRY_URL = "https://fake-url.com/post/telemetry/here";

  private Runnable runnable;
  private HttpURLConnection[] uploadConnectionHolder = new HttpURLConnection[1];

  private File createTestDir() {
    File dir = new File("target", UUID.randomUUID().toString());
    Assert.assertTrue(dir.mkdir());
    return dir.getAbsoluteFile();
  }

  @Test
  public void testGetters() {
    File testDir = createTestDir();
    BuildInfo buildInfo = Mockito.mock(BuildInfo.class);
    Mockito.when(buildInfo.getVersion()).thenReturn("v1");

    RuntimeInfo runtimeInfo = mockRuntimeInfo("id", testDir);

    Configuration config = new Configuration();
    config.set(StatsCollectorTask.ROLL_PERIOD_CONFIG, 1);

    SafeScheduledExecutorService scheduler = Mockito.mock(SafeScheduledExecutorService.class);

    StatsCollectorTask task = mockStatsCollectorTask(buildInfo, runtimeInfo, config, scheduler, true);

    Assert.assertEquals(buildInfo, task.getBuildInfo());

    Assert.assertEquals(runtimeInfo, task.getRuntimeInfo());

    Assert.assertEquals(TimeUnit.HOURS.toMillis(1), task.getRollFrequencyMillis());

    Assert.assertNull(task.getStatsInfo());

  }

  @Test
  public void testClusterSlave() {
    File testDir = createTestDir();
    BuildInfo buildInfo = Mockito.mock(BuildInfo.class);
    Mockito.when(buildInfo.getVersion()).thenReturn("v1");
    Mockito.when(buildInfo.getBuiltRepoSha()).thenReturn("sha1");

    RuntimeInfo runtimeInfo = mockRuntimeInfo("id", testDir);

    Configuration config = new Configuration();

    SafeScheduledExecutorService scheduler = Mockito.mock(SafeScheduledExecutorService.class);

    StatsCollectorTask task = mockStatsCollectorTask(buildInfo, runtimeInfo, config, scheduler, true);

    Mockito.when(runtimeInfo.isClusterSlave()).thenReturn(true);

    task.init();

    Assert.assertTrue(task.isOpted());
    Assert.assertFalse(task.isActive());
    Assert.assertNotNull(task.getStatsInfo());

    task.stop();
  }

  @Test
  public void testFirstRunAndCommonInitializationAndStopLogic() {
    File testDir = createTestDir();
    BuildInfo buildInfo = Mockito.mock(BuildInfo.class);
    Mockito.when(buildInfo.getVersion()).thenReturn("v1");
    Mockito.when(buildInfo.getBuiltRepoSha()).thenReturn("sha1");

    RuntimeInfo runtimeInfo = mockRuntimeInfo("id", testDir);

    Configuration config = new Configuration();

    SafeScheduledExecutorService scheduler = Mockito.mock(SafeScheduledExecutorService.class);

    StatsCollectorTask task = mockStatsCollectorTaskAndRunnable(buildInfo, runtimeInfo, config, scheduler);

    task.init();

    Assert.assertFalse(task.isOpted());
    Assert.assertFalse(task.isActive());
    Assert.assertNotNull(task.getStatsInfo());

    Mockito.verify(runnable, Mockito.times(1)).run();
    Mockito.verify(scheduler, Mockito.times(1)).scheduleAtFixedRate(
        Mockito.eq(runnable),
        Mockito.eq(60L),
        Mockito.eq(60L),
        Mockito.eq(TimeUnit.SECONDS)
    );

    Future future = Mockito.mock(ScheduledFuture.class);
    Mockito.doReturn(future).when(task).getFuture();

    Assert.assertEquals(1, task.getStatsInfo().getActiveStats().getUpTime().getMultiplier());

    task.stop();
    Mockito.verify(future, Mockito.times(1)).cancel(Mockito.eq(false));

    Mockito.verify(runnable, Mockito.times(2)).run();

    Assert.assertEquals(0, task.getStatsInfo().getActiveStats().getUpTime().getMultiplier());
  }

  @Test
  public void testInitialOptingOut() throws Exception {
    File testDir = createTestDir();

    BuildInfo buildInfo = Mockito.mock(BuildInfo.class);
    Mockito.when(buildInfo.getVersion()).thenReturn("v1");
    Mockito.when(buildInfo.getBuiltRepoSha()).thenReturn("sha1");

    RuntimeInfo runtimeInfo = mockRuntimeInfo("id", testDir);

    Configuration config = new Configuration();

    SafeScheduledExecutorService scheduler = Mockito.mock(SafeScheduledExecutorService.class);

    StatsCollectorTask task = mockStatsCollectorTaskAndRunnable(buildInfo, runtimeInfo, config, scheduler);

    task.init();

    Assert.assertFalse(task.isOpted());
    Assert.assertFalse(task.isActive());
    task.setActive(false);
    Assert.assertTrue(task.isOpted());
    Assert.assertFalse(task.isActive());

    task.stop();
  }

  @Test
  public void testInitialOptingIn() throws Exception {
    File testDir = createTestDir();

    BuildInfo buildInfo = Mockito.mock(BuildInfo.class);
    Mockito.when(buildInfo.getVersion()).thenReturn("v1");
    Mockito.when(buildInfo.getBuiltRepoSha()).thenReturn("sha1");

    RuntimeInfo runtimeInfo = mockRuntimeInfo("id", testDir);

    Configuration config = new Configuration();

    SafeScheduledExecutorService scheduler = Mockito.mock(SafeScheduledExecutorService.class);

    StatsCollectorTask task = mockStatsCollectorTaskAndRunnable(buildInfo, runtimeInfo, config, scheduler);

    task.init();

    Assert.assertFalse(task.isOpted());
    Assert.assertFalse(task.isActive());
    task.setActive(false);
    Assert.assertTrue(task.isOpted());
    Assert.assertFalse(task.isActive());

    task.stop();
  }

  @Test
  public void testOptedNo() throws Exception {
    File testDir = createTestDir();

    BuildInfo buildInfo = Mockito.mock(BuildInfo.class);
    Mockito.when(buildInfo.getVersion()).thenReturn("v1");
    Mockito.when(buildInfo.getBuiltRepoSha()).thenReturn("sha1");

    RuntimeInfo runtimeInfo = mockRuntimeInfo("id", testDir);

    Configuration config = new Configuration();

    SafeScheduledExecutorService scheduler = Mockito.mock(SafeScheduledExecutorService.class);

    StatsCollectorTask task = mockStatsCollectorTaskAndRunnable(buildInfo, runtimeInfo, config, scheduler);

    try (OutputStream os = new FileOutputStream(task.getOptFile())) {
      ObjectMapperFactory.get().writeValue(os, ImmutableMap.of(task.STATS_ACTIVE_KEY, false));
    }

    task.init();

    Assert.assertTrue(task.isOpted());
    Assert.assertFalse(task.isActive());
    Assert.assertNotNull(task.getStatsInfo());

    task.stop();

  }

  @Test
  public void testOptedYesNoPriorStats() throws Exception {
    File testDir = createTestDir();

    BuildInfo buildInfo = Mockito.mock(BuildInfo.class);
    Mockito.when(buildInfo.getVersion()).thenReturn("v1");
    Mockito.when(buildInfo.getBuiltRepoSha()).thenReturn("sha1");

    RuntimeInfo runtimeInfo = mockRuntimeInfo("id", testDir);

    Configuration config = new Configuration();

    SafeScheduledExecutorService scheduler = Mockito.mock(SafeScheduledExecutorService.class);

    StatsCollectorTask task = mockStatsCollectorTaskAndRunnable(buildInfo, runtimeInfo, config, scheduler);

    try (OutputStream os = new FileOutputStream(task.getOptFile())) {
      ObjectMapperFactory.get().writeValue(os, ImmutableMap.of(task.STATS_ACTIVE_KEY, true));
    }

    task.init();

    Assert.assertTrue(task.isOpted());
    Assert.assertTrue(task.isActive());

    task.stop();
  }

  @Test
  public void testOptedInvalid1() throws Exception {
    File testDir = createTestDir();

    BuildInfo buildInfo = Mockito.mock(BuildInfo.class);
    Mockito.when(buildInfo.getVersion()).thenReturn("v1");
    Mockito.when(buildInfo.getBuiltRepoSha()).thenReturn("sha1");

    RuntimeInfo runtimeInfo = mockRuntimeInfo("id", testDir);

    Configuration config = new Configuration();

    SafeScheduledExecutorService scheduler = Mockito.mock(SafeScheduledExecutorService.class);

    StatsCollectorTask task = mockStatsCollectorTaskAndRunnable(buildInfo, runtimeInfo, config, scheduler);

    try (OutputStream os = new FileOutputStream(task.getOptFile())) {
    }

    task.init();

    Assert.assertFalse(task.isOpted());
    Assert.assertFalse(task.isActive());

    task.stop();
  }

  @Test
  public void testOptedInvalid2() throws Exception {
    File testDir = createTestDir();

    BuildInfo buildInfo = Mockito.mock(BuildInfo.class);
    Mockito.when(buildInfo.getVersion()).thenReturn("v1");
    Mockito.when(buildInfo.getBuiltRepoSha()).thenReturn("sha1");

    RuntimeInfo runtimeInfo = mockRuntimeInfo("id", testDir);

    Configuration config = new Configuration();

    SafeScheduledExecutorService scheduler = Mockito.mock(SafeScheduledExecutorService.class);

    StatsCollectorTask task = mockStatsCollectorTaskAndRunnable(buildInfo, runtimeInfo, config, scheduler);

    try (OutputStream os = new FileOutputStream(task.getOptFile())) {
      ObjectMapperFactory.get().writeValue(os, null);
    }

    task.init();

    Assert.assertFalse(task.isOpted());
    Assert.assertFalse(task.isActive());

    task.stop();
  }

  @Test
  public void testOptedInvalid3() throws Exception {
    File testDir = createTestDir();

    BuildInfo buildInfo = Mockito.mock(BuildInfo.class);
    Mockito.when(buildInfo.getVersion()).thenReturn("v1");
    Mockito.when(buildInfo.getBuiltRepoSha()).thenReturn("sha1");

    RuntimeInfo runtimeInfo = mockRuntimeInfo("id", testDir);

    Configuration config = new Configuration();

    SafeScheduledExecutorService scheduler = Mockito.mock(SafeScheduledExecutorService.class);

    StatsCollectorTask task = mockStatsCollectorTaskAndRunnable(buildInfo, runtimeInfo, config, scheduler);

    try (OutputStream os = new FileOutputStream(task.getOptFile())) {
      ObjectMapperFactory.get().writeValue(os, ImmutableList.of());
    }

    task.init();

    Assert.assertFalse(task.isOpted());
    Assert.assertFalse(task.isActive());

    task.stop();
  }

  @Test
  public void testOptedInvalid4() throws Exception {
    File testDir = createTestDir();

    BuildInfo buildInfo = Mockito.mock(BuildInfo.class);
    Mockito.when(buildInfo.getVersion()).thenReturn("v1");
    Mockito.when(buildInfo.getBuiltRepoSha()).thenReturn("sha1");

    RuntimeInfo runtimeInfo = mockRuntimeInfo("id", testDir);

    Configuration config = new Configuration();

    SafeScheduledExecutorService scheduler = Mockito.mock(SafeScheduledExecutorService.class);

    StatsCollectorTask task = mockStatsCollectorTaskAndRunnable(buildInfo, runtimeInfo, config, scheduler);

    try (OutputStream os = new FileOutputStream(task.getOptFile())) {
      os.write("foo".getBytes());
    }

    task.init();

    Assert.assertFalse(task.isOpted());
    Assert.assertFalse(task.isActive());

    task.stop();
  }


  @Test
  public void testOptedYesPriorStats() throws Exception {
    File testDir = createTestDir();

    BuildInfo buildInfo = Mockito.mock(BuildInfo.class);
    Mockito.when(buildInfo.getVersion()).thenReturn("v1");
    Mockito.when(buildInfo.getBuiltRepoSha()).thenReturn("sha1");

    RuntimeInfo runtimeInfo = mockRuntimeInfo("id", testDir);

    Configuration config = new Configuration();

    SafeScheduledExecutorService scheduler = Mockito.mock(SafeScheduledExecutorService.class);

    StatsCollectorTask task = mockStatsCollectorTaskAndRunnable(buildInfo, runtimeInfo, config, scheduler);

    try (OutputStream os = new FileOutputStream(task.getOptFile())) {
      ObjectMapperFactory.get().writeValue(os, ImmutableMap.of(task.STATS_ACTIVE_KEY, true));
    }

    try (OutputStream os = new FileOutputStream(task.getStatsFile())) {
      StatsInfo statsInfo = new StatsInfo();
      statsInfo.getActiveStats().setDataCollectorVersion("v2");
      ObjectMapperFactory.get().writeValue(os, statsInfo);
    }

    task.init();

    Assert.assertTrue(task.isOpted());
    Assert.assertTrue(task.isActive());

    //getRunnable() is mocked out to nothing, that is why we get v2 that we read from file
    Assert.assertEquals("v2", task.getStatsInfo().getActiveStats().getDataCollectorVersion());
    task.stop();
  }

  @Test
  public void testRunnable() throws Exception {
    File testDir = createTestDir();

    BuildInfo buildInfo = Mockito.mock(BuildInfo.class);
    Mockito.when(buildInfo.getVersion()).thenReturn("v1");
    Mockito.when(buildInfo.getBuiltRepoSha()).thenReturn("sha1");

    RuntimeInfo runtimeInfo = mockRuntimeInfo("id", testDir);

    Configuration config = new Configuration();

    SafeScheduledExecutorService scheduler = Mockito.mock(SafeScheduledExecutorService.class);

    SupportBundleManager supportBundleManager = Mockito.mock(SupportBundleManager.class);

    StatsCollectorTask task = mockStatsCollectorTask(buildInfo, runtimeInfo, config, scheduler, true);

    try (OutputStream os = new FileOutputStream(task.getOptFile())) {
      ObjectMapperFactory.get().writeValue(os, ImmutableMap.of(task.STATS_ACTIVE_KEY, true));
    }

    try (OutputStream os = new FileOutputStream(task.getStatsFile())) {
      StatsInfo statsInfo = new StatsInfo();
      statsInfo.getActiveStats().setSdcId("id");
      statsInfo.getActiveStats().setDataCollectorVersion("v0");
      statsInfo.getActiveStats().setBuildRepoSha("sha1");
      statsInfo.getActiveStats().setExtraInfo(ImmutableMap.of("a", "A"));
      statsInfo.getCollectedStats().add(new StatsBean());
      ObjectMapperFactory.get().writeValue(os, statsInfo);
    }

    task.init();

    Assert.assertTrue(task.isOpted());
    Assert.assertTrue(task.isActive());

    //verifying we rolled the read stats
    Assert.assertEquals("v1", task.getStatsInfo().getActiveStats().getDataCollectorVersion());

    task.stop();
  }

  // Question to Tucu: How is this test trying to induce failures? How much of this test is really working?
  // You can easily cause a failure by mocking reportStats to be return true or false
  @Ignore("Must integrate new UPLOAD, then test")
  @Test
  public void testRunnableReportStatsException() throws Exception {
    File testDir = createTestDir();

    BuildInfo buildInfo = Mockito.mock(BuildInfo.class);
    Mockito.when(buildInfo.getVersion()).thenReturn("v1");

    RuntimeInfo runtimeInfo = mockRuntimeInfo("id", testDir);

    Configuration config = new Configuration();

    SafeScheduledExecutorService scheduler = Mockito.mock(SafeScheduledExecutorService.class);
    ScheduledFuture future = Mockito.mock(ScheduledFuture.class);
    Mockito.doReturn(future).when(scheduler).scheduleAtFixedRate(
        Mockito.any(),
        Mockito.anyLong(),
        Mockito.anyLong(),
        Mockito.any()
    );

    SupportBundleManager supportBundleManager = Mockito.mock(SupportBundleManager.class);
    StatsCollectorTask task = mockStatsCollectorTask(buildInfo, runtimeInfo, config, scheduler, true);

    try (OutputStream os = new FileOutputStream(task.getOptFile())) {
      ObjectMapperFactory.get().writeValue(os, ImmutableMap.of(StatsCollectorTask.STATS_ACTIVE_KEY, true));
    }

    try (OutputStream os = new FileOutputStream(task.getStatsFile())) {
      StatsInfo statsInfo = new StatsInfo();
      statsInfo.getActiveStats().setDataCollectorVersion("v0");
      ObjectMapperFactory.get().writeValue(os, statsInfo);
    }

    // one time
    task.init();
    Assert.assertTrue(task.isOpted());
    Assert.assertTrue(task.isActive());
    // TODO below assertion fails. Is it even correct?
    Assert.assertEquals(1, task.getStatsInfo().getCollectedStats().size());
    Mockito.verify(scheduler, Mockito.times(1)).scheduleAtFixedRate(
        Mockito.any(),
        Mockito.eq(60L),
        Mockito.eq(60L),
        Mockito.eq(TimeUnit.SECONDS)
    );

    // two times
    task.getRunnable(false).run();
    Assert.assertTrue(task.isOpted());
    Assert.assertTrue(task.isActive());

    // count resets because it works now
    task.getRunnable(false).run();
    Assert.assertTrue(task.isOpted());
    Assert.assertTrue(task.isActive());
    Assert.assertEquals(0, task.getStatsInfo().getCollectedStats().size());

    task.getStatsInfo().getCollectedStats().add(new StatsBean());

    // It will retry 5 times, once a minute, before backing off for 1 day.  After 1 day, it will retry the 5 times again
    // and if it continues to fail, it will back off for 2 days.  After that, 4 days.  Finally, it will give up and
    // switch off.  Here, we run through all of this, but it manages to temporarily succeed after on the 4th try of the
    // 3rd day (which resets all the retries and back offs).
    int doOnceOnly = 0;
    for (int k = 0; k < 4; k++) {
      System.out.println("AAA k = " + k);

      // one through five times
      for (int i = 0; i < 5; i++) {
        System.out.println("AAA k = " + k + " i = " + i);
        if (doOnceOnly == 0 && k == 2 && i == 3) {

          // count resets because it works now
          task.getRunnable(false).run();
          Assert.assertTrue(task.isOpted());
          Assert.assertTrue(task.isActive());
          Assert.assertEquals(0, task.getStatsInfo().getCollectedStats().size());

          task.getStatsInfo().getCollectedStats().add(new StatsBean());

          k = 0;
          i = -1;
          doOnceOnly = 1;
        } else {
          task.getRunnable(false).run();
          Assert.assertTrue(task.isOpted());
          Assert.assertTrue(task.isActive());
          Assert.assertEquals(1, task.getStatsInfo().getCollectedStats().size());
        }
      }

      // six times - we now try to back off
      task.getRunnable(false).run();
      Assert.assertTrue(task.isOpted());
      // Keep backing off while it's less than 3
      if (k < 3) {
        Assert.assertTrue(task.isActive());
        Assert.assertEquals(1, task.getStatsInfo().getCollectedStats().size());
        int expectedTimes = k <= 1 ? doOnceOnly + 1 : 1;
        Mockito.verify(scheduler, Mockito.times(expectedTimes)).scheduleAtFixedRate(
            Mockito.any(),
            Mockito.eq(60L * 60L * 24L * (int)Math.pow(2, k)),
            Mockito.eq(60L),
            Mockito.eq(TimeUnit.SECONDS)
        );
      // Otherwise, we'll just give up: switch it off
      } else {
        Assert.assertFalse(task.isActive());
        Assert.assertEquals(0, task.getStatsInfo().getCollectedStats().size());
        Mockito.verify(scheduler, Mockito.times(0)).scheduleAtFixedRate(
            Mockito.any(),
            Mockito.eq(60L * 60L * 24L * (int)Math.pow(2, k)),
            Mockito.eq(60L),
            Mockito.eq(TimeUnit.SECONDS)
        );
        Mockito.verify(scheduler, Mockito.times(2)).scheduleAtFixedRate(
            Mockito.any(),
            Mockito.eq(60L),
            Mockito.eq(60L),
            Mockito.eq(TimeUnit.SECONDS)
        );
      }
    }

    task.stop();
  }

  @Test
  public void testSetActiveNoChange() throws Exception {
    File testDir = createTestDir();

    BuildInfo buildInfo = Mockito.mock(BuildInfo.class);
    Mockito.when(buildInfo.getVersion()).thenReturn("v1");
    Mockito.when(buildInfo.getBuiltRepoSha()).thenReturn("sha1");

    RuntimeInfo runtimeInfo = mockRuntimeInfo("id", testDir);

    Configuration config = new Configuration();

    SafeScheduledExecutorService scheduler = Mockito.mock(SafeScheduledExecutorService.class);

    SupportBundleManager supportBundleManager = Mockito.mock(SupportBundleManager.class);

    StatsCollectorTask task = mockStatsCollectorTask(buildInfo, runtimeInfo, config, scheduler, true);

    try (OutputStream os = new FileOutputStream(task.getOptFile())) {
      ObjectMapperFactory.get().writeValue(os, ImmutableMap.of(task.STATS_ACTIVE_KEY, true));
    }

    try (OutputStream os = new FileOutputStream(task.getStatsFile())) {
      StatsInfo statsInfo = new StatsInfo();
      statsInfo.getActiveStats().setSdcId("id");
      statsInfo.getActiveStats().setDataCollectorVersion("v1");
      statsInfo.getActiveStats().setBuildRepoSha("sha1");
      statsInfo.getActiveStats().setExtraInfo(ImmutableMap.of("a", "A"));
      ObjectMapperFactory.get().writeValue(os, statsInfo);
    }

    task.init();

    Mockito.reset(task);

    task.setActive(task.isActive());

    Mockito.verify(task, Mockito.never()).saveStats();

    task.stop();
  }

  @Test
  public void testSetActiveFromTrueToFalse() throws Exception {
    File testDir = createTestDir();

    BuildInfo buildInfo = Mockito.mock(BuildInfo.class);
    Mockito.when(buildInfo.getVersion()).thenReturn("v1");
    Mockito.when(buildInfo.getBuiltRepoSha()).thenReturn("sha1");

    RuntimeInfo runtimeInfo = mockRuntimeInfo("id", testDir);

    Configuration config = new Configuration();

    SafeScheduledExecutorService scheduler = Mockito.mock(SafeScheduledExecutorService.class);

    StatsCollectorTask task = mockStatsCollectorTask(buildInfo, runtimeInfo, config, scheduler, true);

    try (OutputStream os = new FileOutputStream(task.getOptFile())) {
      ObjectMapperFactory.get().writeValue(os, ImmutableMap.of(task.STATS_ACTIVE_KEY, true));
    }

    try (OutputStream os = new FileOutputStream(task.getStatsFile())) {
      StatsInfo statsInfo = new StatsInfo();
      statsInfo.getActiveStats().setSdcId("id");
      statsInfo.getActiveStats().setDataCollectorVersion("v1");
      statsInfo.getActiveStats().setBuildRepoSha("sha1");
      statsInfo.getActiveStats().setExtraInfo(ImmutableMap.of("a", "A"));
      ObjectMapperFactory.get().writeValue(os, statsInfo);
      statsInfo.getCollectedStats().add(new StatsBean());
    }

    task.init();

    Mockito.reset(task);

    long start = task.getStatsInfo().getActiveStats().getStartTime();
    Thread.sleep(1);
    task.setActive(false);

    Assert.assertTrue(task.getStatsInfo().getActiveStats().getStartTime() > start);

    Assert.assertFalse(task.isActive());

    try (InputStream is = new FileInputStream(task.getOptFile())) {
      Map map = ObjectMapperFactory.get().readValue(is, Map.class);
      Assert.assertNotNull(map.get(StatsCollectorTask.STATS_ACTIVE_KEY));
      Assert.assertFalse((Boolean) map.get(StatsCollectorTask.STATS_ACTIVE_KEY));
    }
    Mockito.verify(task, Mockito.times(1)).saveStats();

    Assert.assertTrue(task.getStatsInfo().getCollectedStats().isEmpty());
    task.stop();
  }

  @Test
  public void testUpgradeFrom313WithOptedTrue() throws Exception {
    File testDir = createTestDir();

    BuildInfo buildInfo = Mockito.mock(BuildInfo.class);
    Mockito.when(buildInfo.getVersion()).thenReturn("v1");
    Mockito.when(buildInfo.getBuiltRepoSha()).thenReturn("sha1");

    RuntimeInfo runtimeInfo = mockRuntimeInfo("id", testDir);

    Configuration config = new Configuration();

    SafeScheduledExecutorService scheduler = Mockito.mock(SafeScheduledExecutorService.class);

    StatsCollectorTask task = mockStatsCollectorTaskAndRunnable(buildInfo, runtimeInfo, config, scheduler);

    try (OutputStream os = new FileOutputStream(task.getOptFile())) {
      // This is copied from a real 3.13.0 install
      IOUtils.write(
          "{\n" +
              "  \"stats.active\" : true,\n" +
              "  \"stats.lastReport\" : 1585760851613\n" +
              "}",
          os);
    }

    task.init();

    Assert.assertTrue(task.isOpted());
    Assert.assertTrue(task.isActive());

    Assert.assertEquals("v1", task.getStatsInfo().getActiveStats().getDataCollectorVersion());
    task.stop();
  }

  @Test
  public void testSetActiveFromFalseToTrue() throws Exception {
    File testDir = createTestDir();

    BuildInfo buildInfo = Mockito.mock(BuildInfo.class);
    Mockito.when(buildInfo.getVersion()).thenReturn("v1");
    Mockito.when(buildInfo.getBuiltRepoSha()).thenReturn("sha1");

    RuntimeInfo runtimeInfo = mockRuntimeInfo("id", testDir);

    Configuration config = new Configuration();

    SafeScheduledExecutorService scheduler = Mockito.mock(SafeScheduledExecutorService.class);

    StatsCollectorTask task = mockStatsCollectorTask(buildInfo, runtimeInfo, config, scheduler, true);

    try (OutputStream os = new FileOutputStream(task.getOptFile())) {
      ObjectMapperFactory.get().writeValue(os, ImmutableMap.of(StatsCollectorTask.STATS_ACTIVE_KEY, false));
    }

    task.init();

    long start = task.getStatsInfo().getActiveStats().getStartTime();
    Assert.assertTrue(task.getStatsInfo().getCollectedStats().isEmpty());


    Thread.sleep(1);
    task.setActive(true);

    Assert.assertTrue(task.getStatsInfo().getActiveStats().getStartTime() > start);

    Assert.assertTrue(task.getStatsInfo().getCollectedStats().isEmpty());

    Assert.assertTrue(task.isActive());

    try (InputStream is = new FileInputStream(task.getOptFile())) {
      Map map = ObjectMapperFactory.get().readValue(is, Map.class);
      Assert.assertNotNull(map.get(StatsCollectorTask.STATS_ACTIVE_KEY));
      Assert.assertTrue((Boolean) map.get(StatsCollectorTask.STATS_ACTIVE_KEY));
    }

    Mockito.verify(task, Mockito.times(1)).saveStats();
    task.stop();
  }

  @Test
  public void testReportStats() throws Exception {
    File testDir = createTestDir();

    BuildInfo buildInfo = Mockito.mock(BuildInfo.class);
    Mockito.when(buildInfo.getVersion()).thenReturn("v1");

    String sdcId = "0123456789-0123456789-0123456789";
    RuntimeInfo runtimeInfo = mockRuntimeInfo(sdcId, testDir);

    Configuration config = new Configuration();

    SafeScheduledExecutorService scheduler = Mockito.mock(SafeScheduledExecutorService.class);

    StatsCollectorTask task = mockStatsCollectorTask(buildInfo, runtimeInfo, config, scheduler, true);

    List<StatsBean> stats = ImmutableList.of(new StatsBean());
    stats.get(0).setActivePipelines(5);

    Assert.assertTrue(task.reportStats(stats));

    Mockito.verify(task).postToGetTelemetryUrl(
        Mockito.any(),
        Mockito.eq(ImmutableMap.of(
            StatsCollectorTask.GET_TELEMETRY_URL_ARG_CLIENT_ID, sdcId,
            StatsCollectorTask.GET_TELEMETRY_URL_ARG_EXTENSION, StatsCollectorTask.GET_TELEMETRY_URL_ARG_EXTENSION_JSON)));
    Mockito.verify(task).getHttpURLConnection(new URL(POST_TELEMETRY_URL));
    Mockito.verify(uploadConnectionHolder[0]).getResponseCode();
    List<StatsBean> uploadedStats = OBJECT_MAPPER.readValue(
        ((ByteArrayOutputStream) uploadConnectionHolder[0].getOutputStream()).toByteArray(),
        new TypeReference<List<StatsBean>>(){});
    Assert.assertEquals(1, uploadedStats.size());
    Assert.assertEquals(5, uploadedStats.get(0).getActivePipelines());
  }

  @Test
  public void testRunnableMultipleRollsAndReport() {
    File testDir = createTestDir();

    BuildInfo buildInfo = Mockito.mock(BuildInfo.class);
    Mockito.when(buildInfo.getVersion()).thenReturn("v1");
    Mockito.when(buildInfo.getBuiltRepoSha()).thenReturn("abc");
    Mockito.when(buildInfo.getBuiltDate()).thenReturn(new Date().toString());
    Mockito.when(buildInfo.getBuiltBy()).thenReturn("System");

    String sdcId = "0123456789-0123456789-0123456789";
    RuntimeInfo runtimeInfo = mockRuntimeInfo(sdcId, testDir);

    Configuration config = new Configuration();

    SafeScheduledExecutorService scheduler = Mockito.mock(SafeScheduledExecutorService.class);

    StatsCollectorTask task = mockStatsCollectorTask(buildInfo, runtimeInfo, config, scheduler, true);
    Mockito.when(task.isActive()).thenReturn(true);
    SysInfo sysInfo = task.getSysInfo();

    StatsInfo statsInfo = Mockito.spy(new StatsInfo());
    Mockito.when(task.getStatsInfo()).thenReturn(statsInfo);
    statsInfo.setCurrentSystemInfo(buildInfo, runtimeInfo, sysInfo);

    long rollFrequencyMillis = task.getRollFrequencyMillis();
    Assert.assertEquals(TimeUnit.HOURS.toMillis(StatsCollectorTask.ROLL_PERIOD_CONFIG_MAX), rollFrequencyMillis);
    int expectedRolls = 0;
    int expectedReports = 0;
    int expectedSaves = 0;

    // first run, do initial roll and report with force=true
    task.initTask();
    expectedRolls++;
    expectedReports++;
    expectedSaves++;
    Mockito.verify(statsInfo, Mockito.times(expectedRolls)).setActiveStats(Mockito.any());
    Mockito.verify(task, Mockito.times(expectedReports)).reportStats(Mockito.anyListOf(StatsBean.class));
    Mockito.verify(task, Mockito.times(expectedSaves)).saveStats();

    // run again, should just save now that we don't force
    Runnable runnable = task.getRunnable(false);
    runnable.run();
    expectedSaves++;
    Mockito.verify(statsInfo, Mockito.times(expectedRolls)).setActiveStats(Mockito.any());
    Mockito.verify(task, Mockito.times(expectedReports)).reportStats(Mockito.anyListOf(StatsBean.class));
    Mockito.verify(task, Mockito.times(expectedSaves)).saveStats();

    // run after roll period but before report period
    Mockito.when(task.getRollFrequencyMillis()).thenReturn(0L);
    runnable.run();
    expectedRolls++;
    expectedSaves++;
    Mockito.verify(statsInfo, Mockito.times(expectedRolls)).setActiveStats(Mockito.any());
    Mockito.verify(task, Mockito.times(expectedReports)).reportStats(Mockito.anyListOf(StatsBean.class));
    Mockito.verify(task, Mockito.times(expectedSaves)).saveStats();

    // run after report period
    Mockito.when(task.getReportPeriodSeconds()).thenReturn(0L);
    runnable.run();
    expectedRolls++;
    expectedReports++;
    expectedSaves++;
    Mockito.verify(statsInfo, Mockito.times(expectedRolls)).setActiveStats(Mockito.any());
    Mockito.verify(task, Mockito.times(expectedReports)).reportStats(Mockito.anyListOf(StatsBean.class));
    Mockito.verify(task, Mockito.times(expectedSaves)).saveStats();

    // reset periods and make sure we only save
    Mockito.when(task.getRollFrequencyMillis()).thenReturn(
        TimeUnit.HOURS.toMillis(StatsCollectorTask.ROLL_PERIOD_CONFIG_MAX));
    Mockito.when(task.getReportPeriodSeconds()).thenReturn(
        (long) StatsCollectorTask.TELEMETRY_REPORT_PERIOD_SECONDS_DEFAULT);
    runnable.run();
    expectedSaves++;
    Mockito.verify(statsInfo, Mockito.times(expectedRolls)).setActiveStats(Mockito.any());
    Mockito.verify(task, Mockito.times(expectedReports)).reportStats(Mockito.anyListOf(StatsBean.class));
    Mockito.verify(task, Mockito.times(expectedSaves)).saveStats();

    // stop task and make sure we do one full set of activity
    // we didn't properly start statsInfo, so fake the stop call else it will throw
    Mockito.doNothing().when(statsInfo).stopSystem();
    task.stopTask();
    expectedRolls++;
    expectedReports++;
    expectedSaves++;
    Mockito.verify(statsInfo, Mockito.times(expectedRolls)).setActiveStats(Mockito.any());
    Mockito.verify(task, Mockito.times(expectedReports)).reportStats(Mockito.anyListOf(StatsBean.class));
    Mockito.verify(task, Mockito.times(expectedSaves)).saveStats();
    Mockito.verify(statsInfo).stopSystem();
  }

  @Test
  public void testSkipSnapshotTelemetry() throws Exception {
    File testDir = createTestDir();

    BuildInfo buildInfo = Mockito.mock(BuildInfo.class);
    Mockito.when(buildInfo.getVersion()).thenReturn("v1-SNAPSHOT");

    String sdcId = "0123456789-0123456789-0123456789";
    RuntimeInfo runtimeInfo = mockRuntimeInfo(sdcId, testDir);

    Configuration config = new Configuration();

    SafeScheduledExecutorService scheduler = Mockito.mock(SafeScheduledExecutorService.class);

    StatsCollectorTask task = mockStatsCollectorTask(buildInfo, runtimeInfo, config, scheduler, true);

    List<StatsBean> stats = ImmutableList.of(new StatsBean());
    stats.get(0).setActivePipelines(5);

    Assert.assertTrue(task.reportStats(stats));

    Mockito.verify(task, Mockito.never()).postToGetTelemetryUrl(
        Mockito.any(),
        Mockito.any());
    Mockito.verify(task, Mockito.never()).getHttpURLConnection(Mockito.any());
  }

  @Test
  public void testEnableSnapshotTelemetry() throws Exception {
    File testDir = createTestDir();

    BuildInfo buildInfo = Mockito.mock(BuildInfo.class);
    Mockito.when(buildInfo.getVersion()).thenReturn("v1-SNAPSHOT");

    String sdcId = "0123456789-0123456789-0123456789";
    RuntimeInfo runtimeInfo = mockRuntimeInfo(sdcId, testDir);

    Configuration config = new Configuration();
    config.set(StatsCollectorTask.TELEMETRY_FOR_SNAPSHOT_BUILDS, true);

    SafeScheduledExecutorService scheduler = Mockito.mock(SafeScheduledExecutorService.class);

    StatsCollectorTask task = mockStatsCollectorTask(buildInfo, runtimeInfo, config, scheduler, true);

    List<StatsBean> stats = ImmutableList.of(new StatsBean());

    Assert.assertTrue(task.reportStats(stats));

    Mockito.verify(task).postToGetTelemetryUrl(
        Mockito.any(),
        Mockito.any());
    Mockito.verify(task).getHttpURLConnection(Mockito.any());
  }

  @Test
  public void testReportFrequencySecondConfiguration() throws Exception {
    File testDir = createTestDir();

    BuildInfo buildInfo = Mockito.mock(BuildInfo.class);
    Mockito.when(buildInfo.getVersion()).thenReturn("v1");
    Mockito.when(buildInfo.getBuiltRepoSha()).thenReturn("abc");
    Mockito.when(buildInfo.getBuiltDate()).thenReturn(new Date().toString());
    Mockito.when(buildInfo.getBuiltBy()).thenReturn("System");

    String sdcId = "0123456789-0123456789-0123456789";
    RuntimeInfo runtimeInfo = mockRuntimeInfo(sdcId, testDir);

    Configuration config = new Configuration();
    config.set(StatsCollectorTask.TELEMETRY_REPORT_PERIOD_SECONDS, 120);

    SafeScheduledExecutorService scheduler = Mockito.mock(SafeScheduledExecutorService.class);

    StatsCollectorTask task = mockStatsCollectorTask(buildInfo, runtimeInfo, config, scheduler, true);
    task.initTask();
    Mockito.verify(scheduler).scheduleAtFixedRate(
        Matchers.any(Runnable.class),
        Matchers.eq(60L),
        Matchers.eq(60L),
        Mockito.eq(TimeUnit.SECONDS)
    );
    Assert.assertEquals(120, task.getReportPeriodSeconds());

    scheduler = Mockito.mock(SafeScheduledExecutorService.class);
    //Set it to 48 hours - max at 24 hours
    config.set(StatsCollectorTask.TELEMETRY_REPORT_PERIOD_SECONDS, TimeUnit.DAYS.toSeconds(2));

    task = mockStatsCollectorTask(buildInfo, runtimeInfo, config, scheduler, true);
    task.initTask();
    Mockito.verify(scheduler).scheduleAtFixedRate(
        Matchers.any(Runnable.class),
        Matchers.eq(60L),
        Matchers.eq(60L),
        Mockito.eq(TimeUnit.SECONDS)
    );
    Assert.assertEquals(
        Long.valueOf(StatsCollectorTask.TELEMETRY_REPORT_PERIOD_SECONDS_DEFAULT).longValue(),
        task.getReportPeriodSeconds());
  }

  @Test
  public void testRollFrequencyTestMinutesConfiguration() throws Exception {
    File testDir = createTestDir();

    BuildInfo buildInfo = Mockito.mock(BuildInfo.class);
    Mockito.when(buildInfo.getVersion()).thenReturn("v1");
    Mockito.when(buildInfo.getBuiltRepoSha()).thenReturn("abc");
    Mockito.when(buildInfo.getBuiltDate()).thenReturn(new Date().toString());
    Mockito.when(buildInfo.getBuiltBy()).thenReturn("System");

    String sdcId = "0123456789-0123456789-0123456789";
    RuntimeInfo runtimeInfo = mockRuntimeInfo(sdcId, testDir);

    Configuration config = new Configuration();
    config.set(StatsCollectorTask.TEST_ROLL_PERIOD_CONFIG, 20);

    SafeScheduledExecutorService scheduler = Mockito.mock(SafeScheduledExecutorService.class);

    StatsCollectorTask task = mockStatsCollectorTask(buildInfo, runtimeInfo, config, scheduler, true);
    task.initTask();
    Assert.assertEquals(TimeUnit.MINUTES.toMillis(20), task.getRollFrequencyMillis());

    //120 mins - will max at 60 mins
    config.set(StatsCollectorTask.TEST_ROLL_PERIOD_CONFIG, 120);
    scheduler = Mockito.mock(SafeScheduledExecutorService.class);

    task = mockStatsCollectorTask(buildInfo, runtimeInfo, config, scheduler, true);
    task.initTask();
    Assert.assertEquals(TimeUnit.HOURS.toMillis(StatsCollectorTask.ROLL_PERIOD_CONFIG_MAX), task.getRollFrequencyMillis());
  }

  private static final Logger LOG = LoggerFactory.getLogger(TestStatsCollectorTask.class);

  public static final class UsageServlet extends HttpServlet {
    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
      boolean ok;
      String str = req.getContentType();
      if (str == null) {
        LOG.error("Missing content-type header");
        ok = false;
      } else {
        if (str.toLowerCase().startsWith("application/json")) {
          str = req.getHeader("x-requested-by");
          if (str == null) {
            LOG.error("Missing x-requested-by header");
            ok = false;
          } else {
            try {
              UUID.fromString(str);
              try {
                List<StatsBean> list = ObjectMapperFactory.get().readValue(
                    req.getReader(),
                    new TypeReference<List<StatsBean>>() {
                    }
                );
                if (list == null) {
                  LOG.error("Missing payload");
                  ok = false;
                } else {
                  if (list.isEmpty()) {
                    LOG.error("No stats in list");
                    ok = false;
                  } else {
                    ok = true;
                  }
                }
              } catch (IOException ex) {
                LOG.error("Invalid payload: " + ex);
                ok = false;
              }
            } catch (Exception ex) {
              LOG.error("Invalid x-requested-by header, should be SDC ID (a UUID): {}", ex, ex);
              ok = false;
            }
          }
        } else {
          LOG.error("Invalid content-type: {}", str);
          ok = false;
        }
      }
      resp.setStatus((ok) ? HttpServletResponse.SC_OK : HttpServletResponse.SC_BAD_REQUEST);
    }
  }

  @Test
  public void testHttp() throws Exception {
    Server server = new Server(0);
    ServletContextHandler context = new ServletContextHandler();
    Servlet servlet = new UsageServlet();
    context.addServlet(new ServletHolder(servlet), StatsCollectorTask.USAGE_PATH_DEFAULT);
    context.setContextPath("/");
    server.setHandler(context);
    try {
      server.start();

      BuildInfo buildInfo = Mockito.mock(BuildInfo.class);
      Mockito.when(buildInfo.getVersion()).thenReturn("v1");

      RuntimeInfo runtimeInfo = mockRuntimeInfo(UUID.randomUUID().toString(), null);

      Configuration config = new Configuration();

      StatsCollectorTask collector = mockStatsCollectorTask(buildInfo, runtimeInfo, config, null, true);

      List<StatsBean> list = Arrays.asList(new StatsBean());

      Assert.assertTrue(collector.reportStats(list));

    } finally {
      server.stop();
    }
  }

  private StatsCollectorTask mockStatsCollectorTask(
      BuildInfo buildInfo,
      RuntimeInfo runtimeInfo,
      Configuration config,
      SafeScheduledExecutorService executorService,
      boolean postTelemetrySuccess
  ) {
    SysInfo sysInfo = Mockito.mock(SysInfo.class);
    Mockito.when(sysInfo.toMap()).thenReturn(DEFAULT_SYS_INFO_MAP);
    StatsCollectorTask spy = Mockito.spy(new StatsCollectorTask(buildInfo, runtimeInfo, config, executorService, sysInfo));

    // to test real interactions, comment out starting from here, change StatsCollectorTask.TELEMETRY_USE_TEST_BUCKET_DEFAULT to true, and run testReportStats
    // This will put a real file into the S3 bucket customer-support-bundles-test that you can verify.
    try {
      RestClient.Response getUrlResponse = Mockito.mock(RestClient.Response.class);
      Mockito.doReturn(getUrlResponse).when(spy).postToGetTelemetryUrl(Mockito.any(), Mockito.any());
      Mockito.when(getUrlResponse.successful()).thenReturn(postTelemetrySuccess);
      Mockito.when(getUrlResponse.getData(Mockito.any(TypeReference.class))).thenReturn(ImmutableMap.of(
          StatsCollectorTask.TELEMETRY_URL_KEY, POST_TELEMETRY_URL
      ));

      Mockito.doAnswer(new Answer() {
        @Override
        public HttpURLConnection answer(InvocationOnMock invocation) throws Throwable {
          uploadConnectionHolder[0] = Mockito.spy((HttpURLConnection) invocation.callRealMethod());
          // avoid real external calls
          ByteArrayOutputStream output = new ByteArrayOutputStream();
          Mockito.doReturn(output).when(uploadConnectionHolder[0]).getOutputStream();
          Mockito.doReturn(200).when(uploadConnectionHolder[0]).getResponseCode();
          return uploadConnectionHolder[0];
        }
      }).when(spy).getHttpURLConnection(Mockito.any());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    // end section to comment out to perform real interactions

    return spy;
  }

  @Test
  public void testRunnableRollingNotPublishing() throws Exception {
    File testDir = createTestDir();

    BuildInfo buildInfo = Mockito.mock(BuildInfo.class);
    Mockito.when(buildInfo.getVersion()).thenReturn("v1");
    Mockito.when(buildInfo.getBuiltRepoSha()).thenReturn("sha1");

    RuntimeInfo runtimeInfo = mockRuntimeInfo("id", testDir);

    Configuration config = new Configuration();

    SafeScheduledExecutorService scheduler = Mockito.mock(SafeScheduledExecutorService.class);

    StatsCollectorTask task = mockStatsCollectorTask(buildInfo, runtimeInfo, config, scheduler, false);

    try (OutputStream os = new FileOutputStream(task.getOptFile())) {
      ObjectMapperFactory.get().writeValue(os, ImmutableMap.of(task.STATS_ACTIVE_KEY, true));
    }

    try (OutputStream os = new FileOutputStream(task.getStatsFile())) {
      StatsInfo statsInfo = new StatsInfo();
      statsInfo.getActiveStats().setSdcId("id");
      statsInfo.getActiveStats().setDataCollectorVersion("v1");
      statsInfo.getActiveStats().setBuildRepoSha("sha1");
      statsInfo.getActiveStats().setExtraInfo(ImmutableMap.of("a", "A"));
      ObjectMapperFactory.get().writeValue(os, statsInfo);
    }

    task.init();

    Assert.assertTrue(task.isOpted());
    Assert.assertTrue(task.isActive());

    //verifying we rolled the read stats
    Assert.assertEquals("v1", task.getStatsInfo().getActiveStats().getDataCollectorVersion());

    try (InputStream is = new FileInputStream(task.getStatsFile())) {
      StatsInfo statsInfo = ObjectMapperFactory.get().readValue(is, StatsInfo.class);
      Assert.assertEquals(DEFAULT_SYS_INFO_MAP, statsInfo.getExtraInfo(task.getSysInfo()));
      Assert.assertEquals(1, statsInfo.getCollectedStats().size());
      Assert.assertEquals("id",
          statsInfo.getCollectedStats().get(0).getSdcId());
      Assert.assertEquals("v1",
          statsInfo.getCollectedStats().get(0).getDataCollectorVersion());
      Assert.assertEquals("sha1",
          statsInfo.getCollectedStats().get(0).getBuildRepoSha());
      Assert.assertEquals(ImmutableMap.of("a", "A"),
          statsInfo.getCollectedStats().get(0).getExtraInfo());
    }
    task.stop();
  }

  @Test
  public void testSetActiveRunnableCalled() throws Exception {
    File testDir = createTestDir();

    BuildInfo buildInfo = Mockito.mock(BuildInfo.class);
    Mockito.when(buildInfo.getVersion()).thenReturn("v1");
    Mockito.when(buildInfo.getBuiltRepoSha()).thenReturn("sha1");

    RuntimeInfo runtimeInfo = mockRuntimeInfo("id", testDir);

    Configuration config = new Configuration();

    SafeScheduledExecutorService scheduler = Mockito.mock(SafeScheduledExecutorService.class);

    StatsCollectorTask task = mockStatsCollectorTask(buildInfo, runtimeInfo, config, scheduler, false);

    Map<Boolean, AtomicLong> runnableForceParamToTimes =
        new HashMap<>(ImmutableMap.of(Boolean.TRUE, new AtomicLong(0), Boolean.FALSE, new AtomicLong(0)));
    Mockito.when(task.getRunnable(Matchers.anyBoolean())).thenAnswer((Answer<Runnable>) invocation -> {
      final Runnable r = (Runnable) invocation.callRealMethod();
      return (Runnable) () -> {
        runnableForceParamToTimes.get(invocation.getArgumentAt(0, Boolean.class)).incrementAndGet();
        r.run();
      };
    });

    task.initTask();
    Assert.assertEquals(1, runnableForceParamToTimes.get(Boolean.TRUE).get());

    //Set InActive should not trigger roll and report
    task.setActive(false);
    // 1 from start
    Assert.assertEquals(1, runnableForceParamToTimes.get(Boolean.TRUE).get());
    Assert.assertEquals(0, runnableForceParamToTimes.get(Boolean.FALSE).get());


    //Set Active should trigger roll and report
    task.setActive(true);
    Assert.assertEquals(2, runnableForceParamToTimes.get(Boolean.TRUE).get());
    Assert.assertEquals(0, runnableForceParamToTimes.get(Boolean.FALSE).get());


    // try it again, but should not roll and report again
    task.setActive(true);
    Assert.assertEquals(2, runnableForceParamToTimes.get(Boolean.TRUE).get());
    Assert.assertEquals(0, runnableForceParamToTimes.get(Boolean.FALSE).get());

    //Stop should trigger roll and report
    task.stopTask();
    Assert.assertEquals(3, runnableForceParamToTimes.get(Boolean.TRUE).get());
    Assert.assertEquals(0, runnableForceParamToTimes.get(Boolean.FALSE).get());
  }

  private RuntimeInfo mockRuntimeInfo(String sdcId, File dataDir) {
    RuntimeInfo ret = Mockito.mock(RuntimeInfo.class);
    Mockito.when(ret.getId()).thenReturn(sdcId);
    Mockito.when(ret.getProductName()).thenReturn(RuntimeInfo.SDC_PRODUCT);
    if (dataDir != null) {
      Mockito.when(ret.getDataDir()).thenReturn(dataDir.getAbsolutePath());
    }
    Mockito.when(ret.getLibexecDir()).thenReturn(
        System.getenv("PWD").replace("container/src/main/.*","") + "/dist/src/main/libexec");
    return ret;
  }

  private StatsCollectorTask mockStatsCollectorTaskAndRunnable(
      BuildInfo buildInfo,
      RuntimeInfo runtimeInfo,
      Configuration config,
      SafeScheduledExecutorService executorService) {

    StatsCollectorTask spy = mockStatsCollectorTask(buildInfo, runtimeInfo, config, executorService, true);

    runnable = Mockito.mock(Runnable.class);
    Mockito.doReturn(runnable).when(spy).getRunnable(Mockito.anyBoolean());

    return spy;
  }
}
