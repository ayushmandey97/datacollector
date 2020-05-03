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
package com.streamsets.pipeline.stage.processor.waitForPipelineCompletion;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.credential.CredentialValue;
import com.streamsets.pipeline.lib.tls.TlsConfigBean;

public class WaitForPipelineCompletionConfig {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "http://localhost:18630",
      label = "Execution Engine URL",
      description = "URL of Data Collector, Data Collector Edge, or Transformer that runs the specified pipelines",
      displayPosition = 10,
      group = "PIPELINE"
  )
  public String baseUrl = "http://localhost:18630";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      label = "Control Hub Enabled",
      description = "Execution engine is registered with Control Hub",
      defaultValue = "false",
      displayPosition = 50,
      group = "PIPELINE"
  )
  public boolean controlHubEnabled = false;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "https://cloud.streamsets.com",
      label = "Control Hub URL",
      description = "Control Hub base URL",
      displayPosition = 60,
      group = "PIPELINE",
      dependsOn = "controlHubEnabled",
      triggeredByValue = { "true" }
  )
  public String controlHubUrl = "https://cloud.streamsets.com";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "5000",
      label = "Delay Between State Checks",
      description = "Milliseconds to wait before checking pipeline state",
      displayPosition = 80,
      group = "PIPELINE",
      min = 0,
      max = Integer.MAX_VALUE
  )
  public int waitTime;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.CREDENTIAL,
      label = "User Name",
      description = "User that runs the pipeline. Enter a Data Collector, Data Collector Edge, or Transformer user, " +
          "or enter a Control Hub user for execution engines registered with Control Hub.",
      defaultValue = "admin",
      displayPosition = 81,
      group = "CREDENTIALS"
  )
  public CredentialValue username = () -> "admin";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.CREDENTIAL,
      label = "Password",
      description = "Password for the user",
      defaultValue = "admin",
      displayPosition = 82,
      group = "CREDENTIALS"
  )
  public CredentialValue password = () -> "admin";

  @ConfigDefBean(groups = "TLS")
  public TlsConfigBean tlsConfig = new TlsConfigBean();

}
