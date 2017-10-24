package com.xiaomi.infra.galaxy.vision.model;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import com.sun.org.apache.xerces.internal.impl.dv.util.Base64;
import com.xiaomi.infra.galaxy.vision.client.VisionConfig;

/**
 * Copyright 2017, Xiaomi.
 * All rights reserved.
 */
public class Image {
  private transient byte[] rawContent; // raw content not serialized
  private String content; // base64 content to be serialized
  private String uri;

  public void setContent(byte[] content) throws IOException {
    checkContentLength(content == null ? 0 : content.length);
    this.rawContent = content;
    this.content = Base64.encode(content);
  }

  public byte[] getContent() {
    return rawContent;
  }

  public String getBase64Content() {
    return content;
  }

  public String getUri() {
    return uri;
  }

  public void setUri(String uriStr) throws IOException {
    if (uriStr != null) {
      try {
        URI uri = new URI(uriStr);
        checkImageUri(uri);
        this.uri = uriStr;
      } catch (URISyntaxException e) {
        throw new IOException("uri syntax error, uri: " + uriStr, e);
      }
    } else {
      this.uri = uriStr;
    }
  }

  public static void checkContentLength(long len) throws IOException {
    if (len > VisionConfig.MAX_REQUEST_IMAGE_SIZE) {
      throw new IOException("image length exceeded, max allowed: "
          + VisionConfig.MAX_REQUEST_IMAGE_SIZE);
    }
  }

  public static void checkImageUri(URI uri) throws IOException {
    if (uri.getScheme() == null || !uri.getScheme().equals(VisionConfig.FDS_URI_SCHEME)) {
      throw new IOException("illegal uri scheme: " + uri.getScheme()
          + ", only support scheme: " + VisionConfig.FDS_URI_SCHEME);
    }

    checkImageFormat(uri.getPath());
  }

  public static void checkImageFormat(String imagePath) throws IOException {
    if (imagePath == null || !imagePath.toUpperCase().endsWith(".JPG")) {
      throw new IOException("illegal image path: " + imagePath + ", only support '.jpg' format image");
    }
  }
}
