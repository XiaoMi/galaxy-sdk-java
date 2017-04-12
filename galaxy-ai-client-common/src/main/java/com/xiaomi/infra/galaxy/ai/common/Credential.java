package com.xiaomi.infra.galaxy.ai.common;

public class Credential {
  private String accessKey;
  private String accessSecret;

  public Credential(String key, String secret) {
    accessKey = key;
    accessSecret = secret;
  }

  public String getAccessKey() {
    return accessKey;
  }

  public void setAccessKey(String accessKey) {
    this.accessKey = accessKey;
  }

  public String getAccessSecret() {
    return accessSecret;
  }

  public void setAccessSecret(String accessSecret) {
    this.accessSecret = accessSecret;
  }
}
