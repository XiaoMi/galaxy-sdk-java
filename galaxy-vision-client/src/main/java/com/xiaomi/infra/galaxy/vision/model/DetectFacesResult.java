package com.xiaomi.infra.galaxy.vision.model;

import java.util.ArrayList;
import java.util.List;

/**
 * Copyright 2017, Xiaomi.
 * All rights reserved.
 */
public class DetectFacesResult {
  private List<FaceInfo> faceInfo;

  public List<FaceInfo> getFaceInfo() {
    return faceInfo;
  }

  public void setFaceInfo(List<FaceInfo> faceInfo) {
    this.faceInfo = faceInfo;
  }
  
  public void addFaceInfo(FaceInfo... faceInfo) {
    if (this.faceInfo == null) {
      this.faceInfo = new ArrayList<FaceInfo>();
    }
    for (FaceInfo newfaceInfo : faceInfo) {
      this.faceInfo.add(newfaceInfo);
    }
  }
}
