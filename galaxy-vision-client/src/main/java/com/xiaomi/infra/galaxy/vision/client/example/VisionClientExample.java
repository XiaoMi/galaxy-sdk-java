package com.xiaomi.infra.galaxy.vision.client.example;

import com.google.gson.Gson;
import com.xiaomi.infra.galaxy.ai.common.Credential;
import com.xiaomi.infra.galaxy.vision.client.GalaxyVisionClient;
import com.xiaomi.infra.galaxy.vision.client.IOUtils;
import com.xiaomi.infra.galaxy.vision.client.VisionConfig;
import com.xiaomi.infra.galaxy.vision.model.*;

import java.io.BufferedReader;

/**
 * Copyright 2017, Xiaomi.
 * All rights reserved.
 */
public class VisionClientExample {
  public static void main(String args[]) throws Exception {

    Credential credential = new Credential("Your_AK", "Your_SK");
    VisionConfig config = new VisionConfig("cnbj2.vision.api.xiaomi.com");
    GalaxyVisionClient visionClient = new GalaxyVisionClient(credential, config);
    Image image = new Image();
    image.setUri("fds://cnbj2.fds.api.xiaomi.com/vision-test/test_img.jpg");
    // Alternatively, you can specify the image locally:
    // byte[] data = IOUtils.loadImage("test_image.jpg");
    // image.setContent(data);

    // send detect faces request
    DetectFacesRequest facesRequest = new DetectFacesRequest();
    facesRequest.setImage(image);
    DetectFacesResult result = visionClient.detectFaces(facesRequest);
    System.out.println("faces result: " + new Gson().toJson(result));


    // send detect labels request
    DetectLabelsRequest labelsRequest = new DetectLabelsRequest();
    labelsRequest.setImage(image);
    DetectLabelsResult labelsResult = visionClient.detectLabels(labelsRequest);
    System.out.println("labels result: " + new Gson().toJson(labelsResult));

    Image faceImage1 = new Image();
    image.setUri("fds://cnbj2.fds.api.xiaomi.com/vision-test/test_img1.jpg");
    DetectFacesRequest faceImage1Request = new DetectFacesRequest();
    faceImage1Request.setImage(faceImage1);
    DetectFacesResult faceImage1Result = visionClient.detectFaces(faceImage1Request);


    Image faceImage2 = new Image();
    image.setUri("fds://cnbj2.fds.api.xiaomi.com/vision-test/test_img2.jpg");
    DetectFacesRequest faceImage2Request = new DetectFacesRequest();
    faceImage2Request.setImage(faceImage2);
    DetectFacesResult faceImage2Result = visionClient.detectFaces(faceImage2Request);

    FaceMatchRequest faceMatchRequest = new FaceMatchRequest();
    faceMatchRequest.setFirstFeatures(faceImage1Result.getFaceDetails().get(0).getFeatures());
    faceMatchRequest.setSecondFeatures(faceImage2Result.getFaceDetails().get(0).getFeatures());

    faceMatchRequest.setMatchThreshold(3500);
    FaceMatchResult faceMatchResult = visionClient.matchFaces(faceMatchRequest);
    System.out.println("face match result: " + new Gson().toJson(faceMatchResult));

  }
}


