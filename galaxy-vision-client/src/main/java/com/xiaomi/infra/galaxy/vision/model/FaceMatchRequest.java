package com.xiaomi.infra.galaxy.vision.model;

public class FaceMatchRequest {
    private String firstFeatures;
    private String secondFeatures;
    private Integer matchThreshold;

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

    public Integer getMatchThreshold() {
        return matchThreshold;
    }

    public void setMatchThreshold(Integer matchThreshold) {
        this.matchThreshold = matchThreshold;
    }
}
