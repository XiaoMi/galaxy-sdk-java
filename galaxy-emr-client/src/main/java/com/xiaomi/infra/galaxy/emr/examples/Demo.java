package com.xiaomi.infra.galaxy.emr.examples;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xiaomi.infra.galaxy.emr.client.EMRClientFactory;
import com.xiaomi.infra.galaxy.emr.thrift.AddInstanceGroupRequest;
import com.xiaomi.infra.galaxy.emr.thrift.AddSSHPublicKeysRequest;
import com.xiaomi.infra.galaxy.emr.thrift.ApplicationInfo;
import com.xiaomi.infra.galaxy.emr.thrift.ApplicationSuite;
import com.xiaomi.infra.galaxy.emr.thrift.ClusterDetail;
import com.xiaomi.infra.galaxy.emr.thrift.CreateClusterRequest;
import com.xiaomi.infra.galaxy.emr.thrift.CreateClusterResponse;
import com.xiaomi.infra.galaxy.emr.thrift.DeleteSSHPublicKeysRequest;
import com.xiaomi.infra.galaxy.emr.thrift.EMRSchedulerService;
import com.xiaomi.infra.galaxy.emr.thrift.EMRServiceConstants;
import com.xiaomi.infra.galaxy.emr.thrift.GetEMRBasicConfigResponse;
import com.xiaomi.infra.galaxy.emr.thrift.GetHardwareConfigResponse;
import com.xiaomi.infra.galaxy.emr.thrift.GetSSHPublicKeysRequest;
import com.xiaomi.infra.galaxy.emr.thrift.GetSSHPublicKeysResponse;
import com.xiaomi.infra.galaxy.emr.thrift.GetSoftwareConfigResponse;
import com.xiaomi.infra.galaxy.emr.thrift.InstanceGroupRole;
import com.xiaomi.infra.galaxy.emr.thrift.SSHPublicKey;
import com.xiaomi.infra.galaxy.emr.thrift.StateCode;
import com.xiaomi.infra.galaxy.emr.thrift.TerminateClusterRequest;
import com.xiaomi.infra.galaxy.rpc.thrift.Credential;
import com.xiaomi.infra.galaxy.rpc.thrift.UserType;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

/**
 * Copyright 2016, Xiaomi.
 * All rights reserved.
 * Author: liupengcheng@xiaomi.com
 */
public class Demo {

  public static void createCluster() throws Exception {
    if (LOG.isDebugEnabled())
      LOG.debug("test createCluster.");
    String clusterName = "cluster1";
    GetSoftwareConfigResponse getSoftConfigResp = client.getSoftwareConfig();
    GetHardwareConfigResponse getHardConfigResp = client.getHardwareConfig();
    GetEMRBasicConfigResponse getBasicConfigResp = client.getEMRBasicConfig();
    if (LOG.isDebugEnabled()) {
      LOG.debug("getSoftwareConfig response: " + getSoftConfigResp);
      LOG.debug("getHardwareConfig response: " + getHardConfigResp);
      LOG.debug("getEMRBasicConfig response: " + getBasicConfigResp);
    }

    CreateClusterRequest createClusterRequest = new CreateClusterRequest(clusterName);
    createClusterRequest.setAutoTerminate(false).setTerminationProtected(true)
        .setPurpose("emr").setRegion("ec2.cn-north-1").setKeyPair("keypair1");
    List<ApplicationInfo> coreAppInfos = new ArrayList<ApplicationInfo>();
    List<ApplicationInfo> auxAppInfos = new ArrayList<ApplicationInfo>();
    coreAppInfos.add(new ApplicationInfo().setName("Zookeeper").setVersion("3.4.4"));
    coreAppInfos.add(new ApplicationInfo().setName("Hdfs").setVersion("2.4.0"));
    coreAppInfos.add(new ApplicationInfo().setName("Yarn").setVersion("2.4.0"));
    createClusterRequest.setSoftConfig(new ApplicationSuite()
        .setName("MDH")
        .setVersion("emr-mdh1.1")
        .setCoreApplications(coreAppInfos)
        .setAuxApplications(auxAppInfos));
    AddInstanceGroupRequest addMasterGroupRequest = new AddInstanceGroupRequest("masterInstanceGroup")
        .setRole(InstanceGroupRole.MASTER)
        .setInstanceType("master.normal")
        .setRequestedInstanceCount(1);
    AddInstanceGroupRequest addControlGroupRequest = new AddInstanceGroupRequest("controlInstanceGroup")
        .setRole(InstanceGroupRole.CONTROL)
        .setInstanceType("core.normal")
        .setRequestedInstanceCount(3);
    AddInstanceGroupRequest addCoreGroupRequest = new AddInstanceGroupRequest("coreInstanceGroup")
        .setRole(InstanceGroupRole.CORE)
        .setInstanceType("core.normal")
        .setRequestedInstanceCount(1);
    createClusterRequest.addToAddInstanceGroupRequests(addMasterGroupRequest);
    createClusterRequest.addToAddInstanceGroupRequests(addControlGroupRequest);
    createClusterRequest.addToAddInstanceGroupRequests(addCoreGroupRequest);

    CreateClusterResponse createClusterResponse = client.createCluster(createClusterRequest);
    String clusterId = createClusterResponse.getClusterId();
    LOG.info("clusterId:" + clusterId);

    assertNotNull(clusterId);
    assertEquals(createClusterRequest.getName(), createClusterResponse.getName());

    int MAX_TIMEOUT = 6 * 60;
    long pollingStart = System.currentTimeMillis() / 1000;
    while (true) {
      Thread.sleep(5 * 1000);
      ClusterDetail clusterDetail = client.describeCluster(clusterId);
      if (LOG.isDebugEnabled())
        LOG.debug("Cluster status: " + clusterDetail.getClusterStatus().getState());
      if (clusterDetail.getClusterStatus().getState() == StateCode.C_RUNNING) {
        LOG.info("cluster detail: " + clusterDetail);
        break;
      }
      if (System.currentTimeMillis() / 1000 > pollingStart + MAX_TIMEOUT) {
        String errMsg = "Create cluster polling error: polling exceeded max timeout: " +
            +MAX_TIMEOUT + " seconds";
        LOG.error(errMsg);
        throw new Exception(errMsg);
      }
    }
  }

  public static void describeCluster() throws Exception {
    if (LOG.isDebugEnabled())
      LOG.debug("test describeCluster.");
    // describe cluster detail and check
    // you must first know cluster id
    String clusterId = "";
    ClusterDetail clusterDetail = client.describeCluster(clusterId);
    LOG.info("clusterDetail:", clusterDetail);
  }

  public static void listClusters() throws Exception {
    if (LOG.isDebugEnabled())
      LOG.debug("test listClusters.");
    int listStartTime = (int) (System.currentTimeMillis() / 1000 - 8 * 60);
    int listStopTime = (int) (System.currentTimeMillis() / 1000);
    List<ClusterDetail> clusterDetails = client.listClusters(listStartTime, listStopTime);
    // make sure from 8 minutes ago util now there are more than 1 cluster is created
    // 8 minutes is a approximate value
    for (ClusterDetail detail : clusterDetails) {
      LOG.info("clusterDetail: " + detail);
    }
  }

  public static void addAndDeleteSSHPublicKeys() throws Exception {
    String clusterId = ""; // your cluster id
    // add ssh key
    AddSSHPublicKeysRequest add = new AddSSHPublicKeysRequest(clusterId);
    String keyContent = "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQDJnehrgiDwftegCw" +
        "j6JPt8IWrs+FI8LbMxjMMlJw3+91/KECOUi4Hcue/hiaxe2bzGOuZbql" +
        "OS4KdIu3US+QN8FUvfXkBx1Db0DibXTW5dUL+QCmmaRdpw/ATV3LwU0C" +
        "lRIHPnqL+/cIyngU0MGCrbmqkiK2fdeFvvHKRmBqZ+7NHjA4VXj6UPIyLi" +
        "SAX5Y7F4sIi+2jBwmnjR+tR5eNBDv8a3zwaSwOSry2V099qbAlhIUDMH" +
        "oUotFCPiH9KfaAGC6L4PhfevQYIhs9K90k92iiWAtSbGI+oj4F4KQBm" +
        "V2vaAzy3AjWwyW13KjV65aLS6sRlabr+8cN6i0wikfxiD test@xiaomi.com";
    SSHPublicKey sshKey = new SSHPublicKey().setTitle("title1").setContent(keyContent);
    add.addToPublicKeys(sshKey);
    client.addSSHPublicKeys(add);
    // get ssh key
    GetSSHPublicKeysRequest get = new GetSSHPublicKeysRequest(clusterId);
    GetSSHPublicKeysResponse getResp = client.getSSHPublicKeys(get);
    assertEquals("title1", getResp.getPublicKeys().get(0).getTitle());
    assertEquals(keyContent, getResp.getPublicKeys().get(0).getContent());
    assertNotNull(getResp.getPublicKeys().get(0).getAddTime());
    assertNotNull(getResp.getPublicKeys().get(0).getFingerprint());
    assertEquals("2d:97:6e:16:0c:1a:d5:5c:f3:b5:f6:94:ff:86:7f:aa",
        getResp.getPublicKeys().get(0).getFingerprint());
    // delete ssh key
    DeleteSSHPublicKeysRequest delete = new DeleteSSHPublicKeysRequest(clusterId);
    delete.setPublicKeys(getResp.getPublicKeys());
    client.deleteSSHPublicKeys(delete);
  }


  public static void terminateCluster() throws Exception {
    String clusterId = ""; // your cluster id
    if (LOG.isDebugEnabled())
      LOG.debug("terminate cluster:" + clusterId);
    TerminateClusterRequest terminateClusterRequest = new TerminateClusterRequest(clusterId);
    client.terminateCluster(terminateClusterRequest);

    int MAX_TIMEOUT = 6 * 60;
    long terminateStart = System.currentTimeMillis() / 1000;
    while (true) {
      Thread.sleep(5 * 1000);
      ClusterDetail clusterDetail = client.describeCluster(clusterId);
      if (LOG.isDebugEnabled())
        LOG.debug("Cluster status: " + clusterDetail.getClusterStatus().getState());
      if (clusterDetail.getClusterStatus().getState() == StateCode.C_TERMINATED)
        break;
      if (System.currentTimeMillis() / 1000 > terminateStart + MAX_TIMEOUT) {
        String errMsg = "Terminate cluster polling exceeded max timeout:" + MAX_TIMEOUT + " seconds";
        LOG.info(errMsg);
        throw new Exception(errMsg);
      }
    }
  }

  public static void init() {
    Credential credential = new Credential().setType(UserType.APP_SECRET)
        .setSecretKeyId(secretKeyId)
        .setSecretKey(secretKey);
    EMRClientFactory factory = new EMRClientFactory(credential);
    client = factory.newEMRSchedulerService(endpoint
        + EMRServiceConstants.SCHEDULER_SERVICE_PATH);
  }

  private static final Logger LOG = LoggerFactory.getLogger(Demo.class);
  private static String secretKeyId = ""; // your secret key id
  private static String secretKey = ""; // your secret key
  private static String endpoint = "https://awsbj0.emr.api.xiaomi.com";
  private static EMRSchedulerService.Iface client;

  public static void main(String[] args) throws Exception {
    init();
    // test create cluster
//    createCluster();

    // describe cluster
    describeCluster();

    // list clusters of user
//    listClusters();

    // add ssh key in order to ssh login, and you can delete it when not use
//    addAndDeleteSSHPublicKeys();

    // terminate cluster
//    terminateCluster();
  }
}
