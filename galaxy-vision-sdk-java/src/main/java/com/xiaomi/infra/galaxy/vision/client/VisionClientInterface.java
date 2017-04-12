package com.xiaomi.infra.galaxy.vision.client;

import java.io.IOException;

import com.xiaomi.infra.galaxy.vision.model.DetectFacesRequest;
import com.xiaomi.infra.galaxy.vision.model.DetectFacesResult;
import com.xiaomi.infra.galaxy.vision.model.DetectLabelsRequest;
import com.xiaomi.infra.galaxy.vision.model.DetectLabelsResult;

public interface VisionClientInterface {
	public DetectFacesResult detectFaces(DetectFacesRequest request) throws IOException;
	
	public DetectLabelsResult detectLabels(DetectLabelsRequest request) throws IOException;
}
