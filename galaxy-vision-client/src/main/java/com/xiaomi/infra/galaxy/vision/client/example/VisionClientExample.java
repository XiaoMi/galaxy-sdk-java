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

    Credential credential = new Credential("AKLFWE25G7USVQLQX5", "RKQmtJF+JI/pMtnbxie8jGu6rFTwD59r/Vwv9Rki");
    VisionConfig config = new VisionConfig("127.0.0.1:10086");
    GalaxyVisionClient visionClient = new GalaxyVisionClient(credential, config);
    Image image = new Image();
    image.setUri("fds://cnbj1-fds.api.xiaomi.net/test-upload/img_90.jpg");

    // Alternatively, you can specify the image locally:
    //byte[] data = IOUtils.loadImage("img_130.jpg");
    //image.setContent(data);

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
    faceImage1.setUri("fds://cnbj1-fds.api.xiaomi.net/test-upload/img_18.jpg");
    DetectFacesRequest faceImage1Request = new DetectFacesRequest();
    faceImage1Request.setImage(faceImage1);
    DetectFacesResult faceImage1Result = visionClient.detectFaces(faceImage1Request);
    System.out.println("Image1 faces result: " + new Gson().toJson(faceImage1Result));

    Image faceImage2 = new Image();
    faceImage2.setUri("fds://cnbj1-fds.api.xiaomi.net/test-upload/img_65.jpg");
    DetectFacesRequest faceImage2Request = new DetectFacesRequest();
    faceImage2Request.setImage(faceImage2);
    DetectFacesResult faceImage2Result = visionClient.detectFaces(faceImage2Request);
    System.out.println("Image2 faces result: " + new Gson().toJson(faceImage2Result));

    FaceMatchRequest faceMatchRequest = new FaceMatchRequest();
    faceMatchRequest.setFirstFeatures(faceImage1Result.getFaceDetails().get(0).getFeatures());
    faceMatchRequest.setSecondFeatures(faceImage2Result.getFaceDetails().get(0).getFeatures());

    faceMatchRequest.setMatchThreshold(3500);
    FaceMatchResult faceMatchResult = visionClient.matchFaces(faceMatchRequest);
    System.out.println("face match result: " + new Gson().toJson(faceMatchResult));

  }
}


