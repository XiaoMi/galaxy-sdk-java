package com.xiaomi.infra.galaxy.vision.model;

public class DetectLabelsParam {

    public static final int DEFAULT_MAX_LABELS = 5;
    public static final float DEFAULT_MIN_CONFIDENCE = 0.01f;

    private int MaxLabels = DEFAULT_MAX_LABELS;
    private float MinConfidence = DEFAULT_MIN_CONFIDENCE;

    public int getMaxLabels() {
        return MaxLabels;
    }

    public float getMinConfidence(){
        return MinConfidence;
    }

    public void setMaxLabels(int maxLabels) {
        MaxLabels = maxLabels;
    }

    public void setMinConfidence(float minConfidence) {
        MinConfidence = minConfidence;
    }

}
