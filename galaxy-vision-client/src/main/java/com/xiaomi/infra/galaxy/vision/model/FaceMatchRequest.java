package com.xiaomi.infra.galaxy.vision.model;

public class FaceMatchRequest {
    private byte[] firstFeatures;
    private byte[] secondFeatures;
    private int threshold;

    public byte[] getFirstFeatures() {
        return firstFeatures;
    }

    public void setFirstFeatures(byte[] firstFeatures) {
        this.firstFeatures = firstFeatures;
    }

    public byte[] getSecondFeatures() {
        return secondFeatures;
    }

    public void setSecondFeatures(byte[] secondFeatures) {
        this.secondFeatures = secondFeatures;
    }

    public int getThreshold() {
        return threshold;
    }

    public void setThreshold(int threshold) {
        this.threshold = threshold;
    }
}
