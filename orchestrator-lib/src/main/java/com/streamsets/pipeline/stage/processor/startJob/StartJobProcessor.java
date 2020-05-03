/*
 * Copyright 2019 StreamSets Inc.
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
package com.streamsets.pipeline.stage.processor.startJob;

import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.base.SingleLaneProcessor;
import com.streamsets.pipeline.lib.CommonUtil;
import com.streamsets.pipeline.lib.startJob.StartJobCommon;
import com.streamsets.pipeline.lib.startJob.StartJobConfig;
import com.streamsets.pipeline.lib.startJob.StartJobErrors;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class StartJobProcessor extends SingleLaneProcessor {

  private static final Logger LOG = LoggerFactory.getLogger(StartJobProcessor.class);
  private final StartJobCommon startJobCommon;
  private final StartJobConfig conf;
  private ErrorRecordHandler errorRecordHandler;

  StartJobProcessor(StartJobConfig conf) {
    this.conf = conf;
    this.startJobCommon = new StartJobCommon(conf);
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();
    errorRecordHandler = new DefaultErrorRecordHandler(getContext());
    return this.startJobCommon.init(issues, errorRecordHandler, getContext());
  }

  public void process(Batch batch, SingleLaneBatchMaker batchMaker) throws StageException {
    List<CompletableFuture<Field>> startJobFutures = new ArrayList<>();
    Executor executor = Executors.newCachedThreadPool();
    Iterator<Record> it = batch.getRecords();
    Record firstRecord = null;
    while (it.hasNext()) {
      Record record = it.next();
      if (firstRecord == null) {
        firstRecord = record;
      }
      try {
        startJobFutures.addAll(startJobCommon.getStartJobFutures(executor, record));
      } catch (OnRecordErrorException ex) {
        LOG.error(ex.toString(), ex);
        errorRecordHandler.onError(ex);
      }
    }

    if (startJobFutures.isEmpty()) {
      return;
    }

    try {
      LinkedHashMap<String, Field> outputField = startJobCommon.startJobInParallel(startJobFutures);
      firstRecord = CommonUtil.createOrchestratorTaskRecord(
          firstRecord,
          getContext(),
          conf.taskName,
          outputField
      );
      batchMaker.addRecord(firstRecord);
    } catch (Exception ex) {
      LOG.error(ex.toString(), ex);
      errorRecordHandler.onError(StartJobErrors.START_JOB_08, ex.toString(), ex);
    }
  }

}
