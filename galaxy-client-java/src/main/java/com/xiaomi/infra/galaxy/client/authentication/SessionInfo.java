package com.xiaomi.infra.galaxy.client.authentication;

import java.util.HashMap;
import java.util.Map;

public class SessionInfo {

  public enum UserType {
    // non-login user
    GUEST,

    // unprivileged user
    LOGIN_USER,

    // Administrator
    ADMIN,
  }

  public enum AuthType {
    DEVELOPER,
    APPLICATION,
    APPROOT;
  }

  public static class Builder {
    private String userId;
    private String developerId;
    private String appId;
    private String accessKeyId;
    private String secretAccessKeyId;
    private UserType userType;
    private AuthType authType;
    private String requestId;

    public Builder() {
    }

    public Builder withUserId(String userId) {
      this.userId = userId;
      return this;
    }

    public Builder withDeveloperId(String developerId) {
      this.developerId = developerId;
      return this;
    }

    public Builder withAppId(String appId) {
      this.appId = appId;
      return this;
    }

    public Builder withAccessKeyId(String accessKeyId) {
      this.accessKeyId = accessKeyId;
      return this;
    }

    public Builder withSecretAccessKeyId(String secretAccessKeyId) {
      this.secretAccessKeyId = secretAccessKeyId;
      return this;
    }

    public Builder withUserType(UserType userType) {
      this.userType = userType;
      return this;
    }

    public Builder withAuthType(AuthType authType) {
      this.authType = authType;
      return this;
    }

    public Builder withRequestId(String requestId) {
      this.requestId = requestId;
      return this;
    }

    public SessionInfo build() {
      SessionInfo sessionInfo = new SessionInfo();
      sessionInfo.userId = userId;
      sessionInfo.developerId = developerId;
      sessionInfo.appId = appId;
      sessionInfo.accessKeyId = accessKeyId;
      sessionInfo.secretAccessKeyId = secretAccessKeyId;
      sessionInfo.userType = userType;
      sessionInfo.authType = authType;
      sessionInfo.requestId = requestId;
      return sessionInfo;
    }
  }

  private String userId;
  private String developerId;
  private String appId;
  private String accessKeyId;
  private String secretAccessKeyId;
  private UserType userType;
  private AuthType authType;
  private String requestId;
  private final Map<String, Object> properties = new HashMap<String, Object>();

  private SessionInfo() {
  }

  public String getUserId() {
    return userId;
  }

  public UserType getUserType() {
    return userType;
  }

  public void setUserType(UserType userType){
    this.userType = userType;
  }

  public void addProperty(String name, Object value) {
    properties.put(name, value);
  }

  public Object getProperty(String name) {
    return properties.get(name);
  }

  public void setRequestId(String requestId) {
    this.requestId = requestId;
  }

  public String getRequestId() {
    return requestId;
  }

  public String getAccessKeyId() {
    return accessKeyId;
  }

  public void setAccessKeyId(String accessKeyId) {
    this.accessKeyId = accessKeyId;
  }

  public String getSecretAccessKeyId() {
    return secretAccessKeyId;
  }

  public void setSecretAccessKeyId(String secretAccessKeyId) {
    this.secretAccessKeyId = secretAccessKeyId;
  }

  public String getDeveloperId() {
    return developerId;
  }

  public String getAppId() {
    return appId;
  }

  public void setAppId(String appId) {
    this.appId = appId;
  }

  public AuthType getAuthType() {
    return authType;
  }

  public void setAuthType(AuthType authType) {
    this.authType = authType;
  }

  @Override
  public String toString() {
    return "SessionInfo{" +
        "userId='" + userId + '\'' +
        ", developerId='" + developerId + '\'' +
        ", appId='" + appId + '\'' +
        ", accessKeyId='" + accessKeyId + '\'' +
        ", secretAccessKeyId='" + secretAccessKeyId + '\'' +
        ", userType=" + userType +
        ", authType=" + authType +
        ", requestId='" + requestId + '\'' +
        ", properties=" + properties +
        '}';
  }

}
