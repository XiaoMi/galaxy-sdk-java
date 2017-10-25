package com.xiaomi.infra.galaxy.vision.model;

public class FaceMatchRequest {
    private String firstFeatures;
    private String  secondFeatures;
    private Integer threshold;

    public String  getFirstFeatures() {
        return firstFeatures;
    }

    public void setFirstFeatures(String  firstFeatures) {
        this.firstFeatures = firstFeatures;
    }

    public String  getSecondFeatures() {
        return secondFeatures;
    }

    public void setSecondFeatures(String  secondFeatures) {
        this.secondFeatures = secondFeatures;
    }

    public Integer getThreshold() {
        return threshold;
    }

    public void setThreshold(int threshold) {
        this.threshold = threshold;
    }
}
