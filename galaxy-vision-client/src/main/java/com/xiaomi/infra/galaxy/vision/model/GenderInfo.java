package com.xiaomi.infra.galaxy.vision.model;

public class GenderInfo {
    private int gender;
    private float confidence;

    public int getGender() {
        return gender;
    }

    public float getConfidence() {
        return confidence;
    }

    public void setGender(int gender) {
        this.gender = gender;
    }

    public void setConfidence(float confidence) {
        this.confidence = confidence;
    }

}
