package com.xiaomi.infra.galaxy.vision.client;

import java.io.IOException;

import com.xiaomi.infra.galaxy.vision.model.DetectFacesRequest;
import com.xiaomi.infra.galaxy.vision.model.DetectFacesResult;
import com.xiaomi.infra.galaxy.vision.model.DetectLabelsRequest;
import com.xiaomi.infra.galaxy.vision.model.DetectLabelsResult;

/**
 * Copyright 2017, Xiaomi.
 * All rights reserved.
 */
public interface VisionClientInterface {
	public DetectFacesResult detectFaces(DetectFacesRequest request) throws IOException;
	
	public DetectLabelsResult detectLabels(DetectLabelsRequest request) throws IOException;
}
