package com.xiaomi.infra.galaxy.vision.model;

import java.util.ArrayList;
import java.util.List;

public class DetectFacesResult {
  private List<FaceDetail> faceDetails;

  public List<FaceDetail> getFaceDetails() {
    return faceDetails;
  }

  public void setFaceDetails(List<FaceDetail> faceDetails) {
    this.faceDetails = faceDetails;
  }
  
  public void addFaceDetail(FaceDetail... faceDetails) {
    if (this.faceDetails == null) {
      this.faceDetails = new ArrayList<FaceDetail>();
    }
    for (FaceDetail faceDetail : faceDetails) {
      this.faceDetails.add(faceDetail);
    }
  }
}
