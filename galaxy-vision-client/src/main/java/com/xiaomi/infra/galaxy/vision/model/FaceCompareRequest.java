package com.xiaomi.infra.galaxy.vision.model;

public class FaceCompareRequest {
    private FaceCompareData firstFace;
    private FaceCompareData secondFace;
    private Integer matchThreshold;

    public FaceCompareData getFirstFace() {
        return firstFace;
    }

    public FaceCompareData getSecondFace() {
        return secondFace;
    }

    public Integer getMatchThreshold() {
        return matchThreshold;
    }

    public void setFirstFace(FaceCompareData firstFace) {
        this.firstFace = firstFace;
    }

    public void setMatchThreshold(Integer matchThreshold) {
        this.matchThreshold = matchThreshold;
    }

    public void setSecondFace(FaceCompareData secondFace) {
        this.secondFace = secondFace;
    }
}
