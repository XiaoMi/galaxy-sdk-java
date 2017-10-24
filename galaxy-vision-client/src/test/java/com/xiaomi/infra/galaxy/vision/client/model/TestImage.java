package com.xiaomi.infra.galaxy.vision.client.model;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

import com.xiaomi.infra.galaxy.vision.client.VisionConfig;
import com.xiaomi.infra.galaxy.vision.model.Image;

/**
 * Copyright 2017, Xiaomi.
 * All rights reserved.
 */
public class TestImage {
  public static final byte[] test_image_bytes = new byte[VisionConfig.MAX_REQUEST_IMAGE_SIZE + 1];

  @Test
  public void testImageSizeAndUri() throws IOException {
    // check size
    Image image = new Image();
    try {
      image.setContent(test_image_bytes);
      Assert.fail("image binary should exceed: " + VisionConfig.MAX_REQUEST_IMAGE_SIZE
          + ", actual is: " + test_image_bytes.length);
    } catch (IOException e) {
      Assert.assertTrue(e.getMessage().contains("image length exceeded"));
    }

    // check uri scheme
    try {
      image.setUri("abc://test.jpg");
      Assert.fail("scheme 'abc' should not support");
    } catch (IOException e) {
      Assert.assertTrue(e.getMessage().contains("illegal uri scheme"));
    }

    // check image format
    try {
      image.setUri("fds://test.gif");
      Assert.fail("gif should not support");
    } catch (IOException e) {
      Assert.assertTrue(e.getMessage().contains("illegal image path"));
    }
  }
}
