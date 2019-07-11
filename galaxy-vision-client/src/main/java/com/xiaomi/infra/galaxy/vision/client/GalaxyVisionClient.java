package com.xiaomi.infra.galaxy.vision.client;

import java.io.IOException;
import java.net.URI;

import com.xiaomi.infra.galaxy.vision.model.*;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpUriRequest;

import com.xiaomi.infra.galaxy.ai.common.BaseClient;
import com.xiaomi.infra.galaxy.ai.common.Credential;
import com.xiaomi.infra.galaxy.client.authentication.HttpMethod;
import com.xiaomi.infra.galaxy.client.authentication.signature.SubResource;

/**
 * Copyright 2017, Xiaomi.
 * All rights reserved.
 */
public class GalaxyVisionClient extends BaseClient implements VisionClientInterface {
  public static final String IMAGE_DETECT_RESOURCE = "/api/v1/vision/image-detection";
  public static final String FACE_ANALYSIS_RESOURCE = "/api/v1/vision/face-analysis";
  public static final String FACE_COMPARISION_RESOURCE = "/api/v1/vision/face-comparison";

  public GalaxyVisionClient(Credential credential,
      VisionConfig fdsConfig) {
    super(credential, fdsConfig);
  }

  @Override
  public DetectFacesResult detectFaces(DetectFacesRequest request) throws IOException {
    URI uri = formatUri(config.getBaseUri(), FACE_ANALYSIS_RESOURCE, (SubResource[]) null);
    HttpUriRequest httpRequest = makeJsonEntityRequest(request, uri, HttpMethod.POST);
    HttpResponse response = executeHttpRequest(httpRequest);
    DetectFacesResult result = (DetectFacesResult) processResponse(response,
            DetectFacesResult.class, FACE_ANALYSIS_RESOURCE);

    return result;
  }

  @Override
  public DetectLabelsResult detectLabels(DetectLabelsRequest request) throws IOException {
    URI uri = formatUri(config.getBaseUri(), IMAGE_DETECT_RESOURCE, (SubResource[]) null);
    HttpUriRequest httpRequest = makeJsonEntityRequest(request, uri, HttpMethod.POST);
    HttpResponse response = executeHttpRequest(httpRequest);
    DetectLabelsResult result = (DetectLabelsResult) processResponse(response,
            DetectLabelsResult.class, IMAGE_DETECT_RESOURCE);
    return result;
  }

  @Override
  public FaceMatchResult matchFaces(FaceCompareRequest request) throws IOException {
    URI uri = formatUri(config.getBaseUri(), FACE_COMPARISION_RESOURCE, (SubResource[]) null);
    HttpUriRequest httpRequest = makeJsonEntityRequest(request, uri, HttpMethod.POST);
    HttpResponse response = executeHttpRequest(httpRequest);
    try{
      FaceMatchResult result = (FaceMatchResult) processResponse(response,
              FaceMatchResult.class, FACE_COMPARISION_RESOURCE);
      return result;
    }catch (Exception e){
      System.out.println("Invalid First or Second Features! Please Check Again.(Should Be Features Returned from detectFaces Method)");
      return null;
    }
  }
}
