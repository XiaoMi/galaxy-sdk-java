/**
 * Copyright 2016, Xiaomi.
 * All rights reserved.
 * Author: xiajun@xiaomi.com
 */

package com.xiaomi.infra.galaxy.lcs.agent.config;


import com.xiaomi.infra.galaxy.lcs.agent.model.SinkType;
import com.xiaomi.infra.galaxy.lcs.log.core.utils.ConfigureChecker;

public class TalosSinkConfig extends AgentSinkConfig {
  private String talosEndpoint;
  private String secretKeyId;
  private String secretKey;
  private String secretType;
  private long refreshPeriodMillis;

  public TalosSinkConfig() {
    super(SinkType.TALOS);
  }

  public String getTalosEndpoint() {
    return talosEndpoint;
  }

  public void setTalosEndpoint(String talosEndpoint) {
    ConfigureChecker.checkConfigureBlank("talosEndpoint", talosEndpoint);

    this.talosEndpoint = talosEndpoint;
  }

  public String getSecretKeyId() {
    return secretKeyId;
  }

  public void setSecretKeyId(String secretKeyId) {
    ConfigureChecker.checkConfigureBlank("secretKeyId", secretKeyId);

    this.secretKeyId = secretKeyId;
  }

  public String getSecretKey() {
    return secretKey;
  }

  public void setSecretKey(String secretKey) {
    ConfigureChecker.checkConfigureBlank("secretKey", secretKey);

    this.secretKey = secretKey;
  }

  public String getSecretType() {
    return secretType;
  }

  public void setSecretType(String secretType) {
    ConfigureChecker.checkConfigureBlank("secretType", secretType);

    this.secretType = secretType;
  }

  public long getRefreshPeriodMillis() {
    return refreshPeriodMillis;
  }

  public void setRefreshPeriodMillis(long refreshPeriodMillis) {
    ConfigureChecker.checkConfigureRange("refreshPeriodMillis",
        refreshPeriodMillis, 60L * 1000, 24L * 60 * 60 * 1000);

    this.refreshPeriodMillis = refreshPeriodMillis;
  }

  @Override
  public String toString() {
    return "TalosSinkConfig{" +
        "talosEndpoint='" + talosEndpoint + '\'' +
        ", secretKeyId='" + secretKeyId + '\'' +
        ", secretKey='" + secretKey + '\'' +
        ", secretType='" + secretType + '\'' +
        ", refreshPeriodMillis=" + refreshPeriodMillis +
        '}';
  }
}
