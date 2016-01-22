package com.xiaomi.infra.galaxy.client.authentication.signature;

public enum SubResource {
  // Following are amazon S3 supported subresources:
  // acl, lifecycle, location, logging, notification, partNumber,
  // policy, requestPayment, torrent, uploadId, uploads, versionId,
  // versioning, versions and website

  // Currently, we only support a subset of the above subresources:
  ACL("acl"),
  QUOTA("quota"),
  UPLOADS("uploads"),
  PART_NUMBER("partNumber"),
  UPLOAD_ID("uploadId"),
  STORAGE_ACCESS_TOKEN("storageAccessToken"),
  METADATA("metadata");

  private final String name;

  private SubResource(String name) {
    this.name = name;
  }

  public String getName() {
    return this.name;
  }
}
