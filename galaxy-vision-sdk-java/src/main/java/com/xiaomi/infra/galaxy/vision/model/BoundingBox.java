package com.xiaomi.infra.galaxy.vision.model;

public class BoundingBox {
  private int left;
  private int top;
  private int width;
  private int hight;
  
  public int getLeft() {
    return left;
  }
  public void setLeft(int left) {
    this.left = left;
  }
  public int getTop() {
    return top;
  }
  public void setTop(int top) {
    this.top = top;
  }
  public int getWidth() {
    return width;
  }
  public void setWidth(int width) {
    this.width = width;
  }
  public int getHight() {
    return hight;
  }
  public void setHight(int hight) {
    this.hight = hight;
  }
}
