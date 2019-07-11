package com.xiaomi.infra.galaxy.vision.client;

import java.io.IOException;

import com.xiaomi.infra.galaxy.vision.model.*;

/**
 * Copyright 2017, Xiaomi.
 * All rights reserved.
 */
public interface VisionClientInterface {
	public DetectFacesResult detectFaces(DetectFacesRequest request) throws IOException;

	public DetectLabelsResult detectLabels(DetectLabelsRequest request) throws IOException;

	public FaceMatchResult matchFaces(FaceCompareRequest request) throws IOException;
}
