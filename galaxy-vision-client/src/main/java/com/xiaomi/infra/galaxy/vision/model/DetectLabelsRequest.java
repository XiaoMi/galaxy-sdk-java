package com.xiaomi.infra.galaxy.vision.model;

/**
 * Copyright 2017, Xiaomi.
 * All rights reserved.
 */
public class DetectLabelsRequest {
  public static final int DEFAULT_MAX_LABELS = 5;
  public static final float DEFAULT_MIN_CONFIDENCE = 0.01f;
  
  private int maxLabels = DEFAULT_MAX_LABELS;
  private float minConfidence = DEFAULT_MIN_CONFIDENCE;
  private Image image;
  
  public int getMaxLabels() {
    return maxLabels;
  }
  public void setMaxLabels(int maxLabels) {
    this.maxLabels = maxLabels;
  }
  public float getMinConfidence() {
    return minConfidence;
  }
  public void setMinConfidence(float minConfidence) {
    this.minConfidence = minConfidence;
  }
  public Image getImage() {
    return image;
  }
  public void setImage(Image image) {
    this.image = image;
  }
}
