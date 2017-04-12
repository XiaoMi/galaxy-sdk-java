package com.xiaomi.infra.galaxy.vision.client.example;

import com.google.gson.Gson;
import com.xiaomi.infra.galaxy.ai.common.Credential;
import com.xiaomi.infra.galaxy.vision.client.GalaxyVisionClient;
import com.xiaomi.infra.galaxy.vision.client.IOUtils;
import com.xiaomi.infra.galaxy.vision.client.VisionConfig;
import com.xiaomi.infra.galaxy.vision.model.DetectFacesRequest;
import com.xiaomi.infra.galaxy.vision.model.DetectFacesResult;
import com.xiaomi.infra.galaxy.vision.model.DetectLabelsRequest;
import com.xiaomi.infra.galaxy.vision.model.DetectLabelsResult;
import com.xiaomi.infra.galaxy.vision.model.Image;

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
  }
}
