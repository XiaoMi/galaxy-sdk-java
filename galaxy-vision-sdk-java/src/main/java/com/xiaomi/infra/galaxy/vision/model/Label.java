package com.xiaomi.infra.galaxy.vision.model;

public class Label {
  private float confidence;
  private String name;
  
  public float getConfidence() {
    return confidence;
  }
  public void setConfidence(float confidence) {
    this.confidence = confidence;
  }
  public String getName() {
    return name;
  }
  public void setName(String name) {
    this.name = name;
  }
}
