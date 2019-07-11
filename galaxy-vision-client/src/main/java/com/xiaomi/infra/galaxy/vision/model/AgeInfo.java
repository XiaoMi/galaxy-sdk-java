package com.xiaomi.infra.galaxy.vision.model;

public class AgeInfo {
    private int age;
    private float confidence;

    public float getConfidence() {
        return confidence;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public void setConfidence(float confidence) {
        this.confidence = confidence;
    }


}
