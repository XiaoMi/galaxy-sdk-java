package com.xiaomi.infra.galaxy.vision.model;

public class FaceCompareData {
    private Image faceImage;
    private String faceFeature;

    public Image getFaceImage() {
        return faceImage;
    }

    public String getFaceFeature() {
        return faceFeature;
    }

    public void setFaceFeature(String faceFeature) {
        this.faceFeature = faceFeature;
    }

    public void setFaceImage(Image faceImage) {
        this.faceImage = faceImage;
    }
}
