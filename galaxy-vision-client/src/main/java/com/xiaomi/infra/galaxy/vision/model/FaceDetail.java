package com.xiaomi.infra.galaxy.vision.model;

/**
 * Copyright 2017, Xiaomi.
 * All rights reserved.
 */
public class FaceDetail {
  private BoundingBox boundingBox;
  private int age;
  private String features;

  public BoundingBox getBoundingBox() {
    return boundingBox;
  }

  public void setBoundingBox(BoundingBox boundingBox) {
    this.boundingBox = boundingBox;
  }

  public int getAge() {
    return age;
  }

  public void setAge(int age) {
    this.age = age;
  }

  public String getFeatures() {
    return this.features;
  }

  public void setFeatures(String features){
    this.features = features;
  }


}
