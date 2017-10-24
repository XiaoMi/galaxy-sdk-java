package com.xiaomi.infra.galaxy.vision.model;

/**
 * Copyright 2017, Xiaomi.
 * All rights reserved.
 */
public class FaceDetail {
  private BoundingBox boundingBox;
  private int age;
  private byte[] features;

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

  public byte[] getFeatures() {
    return this.features;
  }

  public void setFeatures(byte[] features){
    this.features = features.clone();
  }


}
