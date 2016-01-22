package com.xiaomi.infra.galaxy.emr.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xiaomi.infra.galaxy.emr.thrift.EMRAdminService;
import com.xiaomi.infra.galaxy.emr.thrift.EMRMasterService;
import com.xiaomi.infra.galaxy.emr.thrift.EMRSchedulerService;
import com.xiaomi.infra.galaxy.emr.thrift.EMRServiceConstants;
import com.xiaomi.infra.galaxy.rpc.client.BaseClientFactory;
import com.xiaomi.infra.galaxy.rpc.thrift.CommonConstants;
import com.xiaomi.infra.galaxy.rpc.thrift.Credential;
import com.xiaomi.infra.galaxy.sds.thrift.ErrorsConstants;

/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: zhangjing8@xiaomi.com
 */
public class EMRClientFactory extends BaseClientFactory {
  private static final Logger LOG = LoggerFactory.getLogger(EMRClientFactory.class);
  public EMRClientFactory() {
    super();
  }

  public EMRClientFactory(Credential credential) {
    super(credential);
  }

  public EMRSchedulerService.Iface newEMRSchedulerService() {
    String url = EMRServiceConstants.DEFAULT_SERVICE_ENDPOINT + EMRServiceConstants.API_ROOT_PATH;
    return newEMRSchedulerService(url);
  }

  public EMRSchedulerService.Iface newEMRSchedulerService(boolean supportAccountKey) {
    String url = EMRServiceConstants.DEFAULT_SERVICE_ENDPOINT + EMRServiceConstants.API_ROOT_PATH;
    return newEMRSchedulerService(url, supportAccountKey);
  }

  public EMRSchedulerService.Iface newEMRSchedulerService(String url) {
    return newEMRSchedulerService(url, false);
  }

  public EMRSchedulerService.Iface newEMRSchedulerService(String url, boolean supportAccountKey) {
    return newEMRSchedulerService(url,
        (int)CommonConstants.DEFAULT_CLIENT_TIMEOUT,
        (int)CommonConstants.DEFAULT_CLIENT_CONN_TIMEOUT,
        supportAccountKey);
  }

  public EMRSchedulerService.Iface newEMRSchedulerService(String url, int socketTimeout, int connTimeout) {
    return newEMRSchedulerService(url, socketTimeout, connTimeout, false);
  }

  public EMRSchedulerService.Iface newEMRSchedulerService(String url, int socketTimeout, int connTimeout,
                                                          boolean supportAccountKey) {
    return newEMRSchedulerService(url, socketTimeout, connTimeout, false, ErrorsConstants.MAX_RETRY, supportAccountKey);
  }

  public EMRSchedulerService.Iface newEMRSchedulerService(String url, boolean isRetry, int maxRetry) {
    return newEMRSchedulerService(url, isRetry, maxRetry, false);
  }

  public EMRSchedulerService.Iface newEMRSchedulerService(String url, boolean isRetry, int maxRetry,
                                                          boolean supportAccountKey) {
    return newEMRSchedulerService(url,
        (int)CommonConstants.DEFAULT_CLIENT_TIMEOUT,
        (int)CommonConstants.DEFAULT_CLIENT_CONN_TIMEOUT,
        isRetry, maxRetry, supportAccountKey);
  }

  public EMRSchedulerService.Iface newEMRSchedulerService(String url, int socketTimeout, int connTimeout,
                                                          boolean isRetry, int maxRetry) {
    return newEMRSchedulerService(url, socketTimeout, connTimeout, isRetry, maxRetry, false);
  }

  public EMRSchedulerService.Iface newEMRSchedulerService(
      String url,
      int socketTimeout,
      int connTimeout,
      boolean isRetry,
      int maxRetry,
      boolean supportAccountKey) {
    return createClient(EMRSchedulerService.Iface.class, EMRSchedulerService.Client.class,
        url, socketTimeout, connTimeout, isRetry, maxRetry, supportAccountKey);
  }

  public EMRMasterService.Iface newEMRMasterService(String endpoint) {
    return newEMRMasterService(endpoint, false, ErrorsConstants.MAX_RETRY);
  }

  public EMRMasterService.Iface newEMRMasterService(String endpoint, boolean isRetry, int maxRetry) {
    String url = endpoint + EMRServiceConstants.MASTER_SERVICE_PATH;
    return newEMRMasterService(url,
        (int)CommonConstants.DEFAULT_CLIENT_TIMEOUT,
        (int)CommonConstants.DEFAULT_CLIENT_CONN_TIMEOUT,
        isRetry,
        maxRetry,
        false
    );
  }

  public EMRMasterService.Iface newEMRMasterService(
      String url,
      int socketTimeout,
      int connTimeout,
      boolean isRetry,
      int maxRetry,
      boolean supportAccountKey) {
    return createClient(EMRMasterService.Iface.class, EMRMasterService.Client.class,
        url, socketTimeout, connTimeout, isRetry, maxRetry, supportAccountKey);
  }

  public EMRAdminService.Iface newEMRAdminService(String endpoint) {
    return newEMRAdminService(endpoint, false, ErrorsConstants.MAX_RETRY);
  }

  public EMRAdminService.Iface newEMRAdminService(String endpoint, boolean isRetry, int maxRetry) {
    String url = endpoint + "/v1/api/metrics";
    return newEMRAdminService(url,
        (int)CommonConstants.DEFAULT_CLIENT_TIMEOUT,
        (int)CommonConstants.DEFAULT_CLIENT_CONN_TIMEOUT,
        isRetry,
        maxRetry,
        false);
  }

  private EMRAdminService.Iface newEMRAdminService(String url,
      int socketTimeout, int connTimeout, boolean isRetry, int maxRetry, boolean supportAccountKey) {
    return createClient(EMRAdminService.Iface.class, EMRAdminService.Client.class,
        url, socketTimeout, connTimeout, isRetry, maxRetry, supportAccountKey);
  }
}
