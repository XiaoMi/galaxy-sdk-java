package com.xiaomi.infra.galaxy.vision.model;

/**
 * Copyright 2017, Xiaomi.
 * All rights reserved.
 */
public class FaceInfo {
  private FacePosition facePos;
  private AgeInfo ageInfo;
  private GenderInfo genderInfo;
  private String feature;

  public AgeInfo getAgeInfo() {
    return ageInfo;
  }

  public FacePosition getfacePos() {
    return facePos;
  }

  public GenderInfo getGenderInfo() {
    return genderInfo;
  }

  public String getFeature() {
    return feature;
  }

  public void setAgeInfo(AgeInfo ageInfo) {
    this.ageInfo = ageInfo;
  }

  public void setfacePos(FacePosition facePos) {
    this.facePos = facePos;
  }

  public void setFeature(String feature) {
    this.feature = feature;
  }

  public void setGenderInfo(GenderInfo genderInfo) {
    this.genderInfo = genderInfo;
  }
}
