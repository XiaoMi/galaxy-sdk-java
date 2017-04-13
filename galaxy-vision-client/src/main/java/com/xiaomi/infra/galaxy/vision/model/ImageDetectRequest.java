package com.xiaomi.infra.galaxy.vision.model;

/**
 * Copyright 2017, Xiaomi.
 * All rights reserved.
 */
public class ImageDetectRequest {
  private DetectFacesRequest detectFacesRequest;
  private DetectLabelsRequest detectLabelsRequest;
  
  public DetectFacesRequest getDetectFacesRequest() {
    return detectFacesRequest;
  }

  public void setDetectFacesRequest(DetectFacesRequest detectFacesRequest) {
    this.detectFacesRequest = detectFacesRequest;
  }

  public DetectLabelsRequest getDetectLabelsRequest() {
    return detectLabelsRequest;
  }

  public void setDetectLabelsRequest(DetectLabelsRequest detectLabelsRequest) {
    this.detectLabelsRequest = detectLabelsRequest;
  }
}
