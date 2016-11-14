/**
 * Copyright 2016, Xiaomi.
 * All rights reserved.
 * Author: xiajun@xiaomi.com
 */

package com.xiaomi.infra.galaxy.lcs.agent.config;


import com.xiaomi.infra.galaxy.lcs.agent.model.SourceType;
import com.xiaomi.infra.galaxy.lcs.log.core.utils.ConfigureChecker;

public class FileSourceConfig extends AgentSourceConfig {
  private String rootFilePath;
  private long checkPeriodMillis;

  public FileSourceConfig() {
    super(SourceType.FILE);
  }

  public String getRootFilePath() {
    return rootFilePath;
  }

  public void setRootFilePath(String rootFilePath) {
    ConfigureChecker.checkConfigureBlank("rootFilePath", rootFilePath);

    this.rootFilePath = rootFilePath;
  }

  public long getCheckPeriodMillis() {
    return checkPeriodMillis;
  }

  public void setCheckPeriodMillis(long checkPeriodMillis) {
    ConfigureChecker.checkConfigureRange("checkPeriodMillis",
        checkPeriodMillis, 1, 10L * 60 * 1000);

    this.checkPeriodMillis = checkPeriodMillis;
  }

  @Override
  public String toString() {
    return "FileSourceConfig{" +
        "rootFilePath='" + rootFilePath + '\'' +
        ", checkPeriodMillis=" + checkPeriodMillis +
        '}';
  }
}
