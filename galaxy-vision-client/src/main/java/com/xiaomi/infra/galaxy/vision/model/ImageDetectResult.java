package com.xiaomi.infra.galaxy.vision.model;

/**
 * Copyright 2017, Xiaomi.
 * All rights reserved.
 */
public class ImageDetectResult {
  private DetectFacesResult detectFacesResult;
  private DetectLabelsResult detectLabelsResult;
  private FaceMatchResult faceMatchResult;

  public DetectFacesResult getDetectFacesResult() {
    return detectFacesResult;
  }

  public void setDetectFacesResult(DetectFacesResult detectFacesResult) {
    this.detectFacesResult = detectFacesResult;
  }

  public DetectLabelsResult getDetectLabelsResult() {
    return detectLabelsResult;
  }

  public void setDetectLabelsResult(DetectLabelsResult detectLabelsResult) {
    this.detectLabelsResult = detectLabelsResult;
  }

  public FaceMatchResult getFaceMatchResult() { return faceMatchResult; }

  public void setFaceMatchResult(FaceMatchResult faceMatchResult) {
    this.faceMatchResult = faceMatchResult;
  }
}
