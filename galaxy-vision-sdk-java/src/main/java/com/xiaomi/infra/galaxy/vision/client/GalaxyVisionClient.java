package com.xiaomi.infra.galaxy.vision.client;

import java.io.IOException;
import java.net.URI;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpUriRequest;

import com.xiaomi.infra.galaxy.ai.common.BaseClient;
import com.xiaomi.infra.galaxy.ai.common.Credential;
import com.xiaomi.infra.galaxy.client.authentication.HttpMethod;
import com.xiaomi.infra.galaxy.client.authentication.signature.SubResource;
import com.xiaomi.infra.galaxy.vision.model.DetectFacesRequest;
import com.xiaomi.infra.galaxy.vision.model.DetectFacesResult;
import com.xiaomi.infra.galaxy.vision.model.DetectLabelsRequest;
import com.xiaomi.infra.galaxy.vision.model.DetectLabelsResult;
import com.xiaomi.infra.galaxy.vision.model.ImageDetectRequest;
import com.xiaomi.infra.galaxy.vision.model.ImageDetectResult;

public class GalaxyVisionClient extends BaseClient implements VisionClientInterface {
  public static final String IMAGE_DETECT_RESOURCE = "v1/image:detect";
  
  public GalaxyVisionClient(Credential credential,
      VisionConfig fdsConfig) {
    super(credential, fdsConfig);
  }

  @Override
  public DetectFacesResult detectFaces(DetectFacesRequest request) throws IOException {
    URI uri = formatUri(config.getBaseUri(), IMAGE_DETECT_RESOURCE, (SubResource[]) null);
    ImageDetectRequest imageDetectRequest = new ImageDetectRequest();
    imageDetectRequest.setDetectFacesRequest(request);
    
    HttpUriRequest httpRequest = makeJsonEntityRequest(imageDetectRequest, uri, HttpMethod.POST);
    HttpResponse response = executeHttpRequest(httpRequest);
    ImageDetectResult result = (ImageDetectResult) processResponse(response,
      ImageDetectResult.class, IMAGE_DETECT_RESOURCE);
    
    return result.getDetectFacesResult();
  }

  @Override
  public DetectLabelsResult detectLabels(DetectLabelsRequest request) throws IOException {
    URI uri = formatUri(config.getBaseUri(), IMAGE_DETECT_RESOURCE, (SubResource[]) null);
    ImageDetectRequest imageDetectRequest = new ImageDetectRequest();
    imageDetectRequest.setDetectLabelsRequest(request);
    
    HttpUriRequest httpRequest = makeJsonEntityRequest(imageDetectRequest, uri, HttpMethod.POST);
    HttpResponse response = executeHttpRequest(httpRequest);
    ImageDetectResult result = (ImageDetectResult) processResponse(response,
      ImageDetectResult.class, IMAGE_DETECT_RESOURCE);
    
    return result.getDetectLabelsResult();
  }
}
