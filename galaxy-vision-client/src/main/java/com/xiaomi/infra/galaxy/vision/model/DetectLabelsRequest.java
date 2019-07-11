package com.xiaomi.infra.galaxy.vision.model;

/**
 * Copyright 2017, Xiaomi.
 * All rights reserved.
 */
public class DetectLabelsRequest {
  
  private DetectLabelsParam param;
  private Image image;

  public void setParam(DetectLabelsParam param) {
    this.param = param;
  }

  public DetectLabelsParam getParam() {
    return param;
  }

  public Image getImage() {
    return image;
  }
  public void setImage(Image image) {
    this.image = image;
  }
}
