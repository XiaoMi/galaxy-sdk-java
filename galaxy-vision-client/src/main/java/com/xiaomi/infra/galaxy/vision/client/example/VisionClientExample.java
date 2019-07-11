package com.xiaomi.infra.galaxy.vision.client.example;

import com.google.gson.Gson;
import com.xiaomi.infra.galaxy.ai.common.Credential;
import com.xiaomi.infra.galaxy.vision.client.GalaxyVisionClient;
import com.xiaomi.infra.galaxy.vision.client.VisionConfig;
import com.xiaomi.infra.galaxy.vision.model.*;

/**
 * Copyright 2017, Xiaomi.
 * All rights reserved.
 */
public class VisionClientExample {
  public static void main(String args[]) throws Exception {

    Credential credential = new Credential("YOUR_AK", "YOUR_SK");
    VisionConfig config = new VisionConfig("cnbj2.vision.api.xiaomi.com");
    GalaxyVisionClient visionClient = new GalaxyVisionClient(credential, config);
    Image image = new Image();
    image.setUri("fds://cnbj1-fds.api.xiaomi.net/test-upload/img_90.jpg");

    // Alternatively, you can specify the image locally:
//    byte[] data = IOUtils.loadImage("/home/mi/phabricator/arcanist/workspace/cloud-vision-go/book1.jpg");
//    image.setContent(data);

    // send detect faces request
    DetectFacesRequest facesRequest = new DetectFacesRequest();
    facesRequest.setImage(image);
    DetectFacesResult result = visionClient.detectFaces(facesRequest);
    System.out.println("faces result: " + new Gson().toJson(result));


    // send detect labels request
    DetectLabelsParam param = new DetectLabelsParam();
    //optional:set the maximum number of labels you want
    param.setMaxLabels(5);
    //optional:set the minimum confidence bar for the labels above that level to display
    param.setMinConfidence((float)0.1);
    DetectLabelsRequest labelsRequest = new DetectLabelsRequest();
    labelsRequest.setImage(image);
    labelsRequest.setParam(param);
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

    FaceCompareData face1 = new FaceCompareData();
    FaceCompareData face2 = new FaceCompareData();
    // option1:set face by image
    face1.setFaceImage(faceImage1);
    face2.setFaceImage(faceImage2);
    // option2:set face by feature
//    face1.setFaceFeature(faceImage1Result.getFaceInfo().get(0).getFeature());
//    face2.setFaceFeature(faceImage2Result.getFaceInfo().get(0).getFeature());
    FaceCompareRequest faceMatchRequest = new FaceCompareRequest();
    faceMatchRequest.setFirstFace(face1);
    faceMatchRequest.setSecondFace(face2);
    faceMatchRequest.setMatchThreshold(3500);
    FaceMatchResult faceMatchResult = visionClient.matchFaces(faceMatchRequest);
    System.out.println("face match result: " + new Gson().toJson(faceMatchResult));

  }
}


