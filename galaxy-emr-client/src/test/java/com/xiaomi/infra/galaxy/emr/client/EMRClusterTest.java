package com.xiaomi.infra.galaxy.emr.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import com.fasterxml.uuid.Generators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

import com.xiaomi.infra.galaxy.emr.thrift.AddInstanceGroupRequest;
import com.xiaomi.infra.galaxy.emr.thrift.AddInstanceGroupResponse;
import com.xiaomi.infra.galaxy.emr.thrift.AddSSHPublicKeysRequest;
import com.xiaomi.infra.galaxy.emr.thrift.ApplicationInfo;
import com.xiaomi.infra.galaxy.emr.thrift.ApplicationSuite;
import com.xiaomi.infra.galaxy.emr.thrift.ClusterDetail;
import com.xiaomi.infra.galaxy.emr.thrift.CreateClusterRequest;
import com.xiaomi.infra.galaxy.emr.thrift.CreateClusterResponse;
import com.xiaomi.infra.galaxy.emr.thrift.DeleteClusterRequest;
import com.xiaomi.infra.galaxy.emr.thrift.DeleteClusterResponse;
import com.xiaomi.infra.galaxy.emr.thrift.DeleteSSHPublicKeysRequest;
import com.xiaomi.infra.galaxy.emr.thrift.EMRSchedulerService;
import com.xiaomi.infra.galaxy.emr.thrift.EMRServiceConstants;
import com.xiaomi.infra.galaxy.emr.thrift.GetEMRBasicConfigResponse;
import com.xiaomi.infra.galaxy.emr.thrift.GetHardwareConfigResponse;
import com.xiaomi.infra.galaxy.emr.thrift.GetSSHPublicKeysRequest;
import com.xiaomi.infra.galaxy.emr.thrift.GetSSHPublicKeysResponse;
import com.xiaomi.infra.galaxy.emr.thrift.GetSoftwareConfigResponse;
import com.xiaomi.infra.galaxy.emr.thrift.InstanceDetail;
import com.xiaomi.infra.galaxy.emr.thrift.InstanceGroupDetail;
import com.xiaomi.infra.galaxy.emr.thrift.InstanceGroupRole;
import com.xiaomi.infra.galaxy.emr.thrift.JobDetail;
import com.xiaomi.infra.galaxy.emr.thrift.SSHPublicKey;
import com.xiaomi.infra.galaxy.emr.thrift.StateCode;
import com.xiaomi.infra.galaxy.emr.thrift.SubmitJobRequest;
import com.xiaomi.infra.galaxy.emr.thrift.TerminateClusterRequest;
import com.xiaomi.infra.galaxy.rpc.thrift.Credential;
import com.xiaomi.infra.galaxy.rpc.thrift.UserType;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: liupengcheng@xiaomi.com
 */
public class EMRClusterTest {
  private static final Logger LOG = LoggerFactory.getLogger(EMRClusterTest.class);
  private static String endpoint;
  private static EMRClientFactory factory;
  private static EMRSchedulerService.Iface service;
  private static TestContext context;
  private static String secretId = ""; // your secretId
  private static String secretKey = ""; // your secretKey

  private final static String CLUSTER_NAME_PREFIX = "cname-";

  private class TestContext {
    private String clusterId;
    private CreateClusterRequest createClusterRequest;
    private CreateClusterResponse createClusterResponse;

    public String getClusterId() {
      return clusterId;
    }

    public void setClusterId(String clusterId) {
      this.clusterId = clusterId;
    }

    public CreateClusterRequest getCreateClusterRequest() {
      return createClusterRequest;
    }

    public void setCreateClusterRequest(CreateClusterRequest createClusterRequest) {
      this.createClusterRequest = createClusterRequest;
    }

    public CreateClusterResponse getCreateClusterResponse() {
      return createClusterResponse;
    }

    public void setCreateClusterResponse(CreateClusterResponse createClusterResponse) {
      this.createClusterResponse = createClusterResponse;
    }
  }

  @Factory(dataProvider = "endpointProvider")
  public EMRClusterTest(String endpoint) throws Exception {
    this.endpoint = endpoint;
    Credential credential = new Credential().setType(UserType.APP_SECRET)
        .setSecretKeyId(secretId).setSecretKey(secretKey);
    factory = new EMRClientFactory(credential);

    service = factory.newEMRSchedulerService(
        this.endpoint + EMRServiceConstants.SCHEDULER_SERVICE_PATH);
    context = new TestContext();
  }

  @DataProvider(name = "endpointProvider")
  public static Object[][] data() throws IOException {
    Properties properties = new Properties();
    properties.load(EMRClusterTest.class.getClassLoader()
        .getResourceAsStream("endpoint.properties"));
    List<Object[]> items = new ArrayList<Object[]>();
    for (Map.Entry<Object, Object> e : properties.entrySet()) {
      items.add(new String[]{(String) e.getValue()});
    }
    return items.toArray(new Object[0][0]);
  }

  @Test
  public void testCreateCluster() throws Exception {
    if (LOG.isDebugEnabled())
      LOG.debug("test createCluster.");
    String clusterName = genClusterName();
    GetSoftwareConfigResponse getSoftConfigResp = service.getSoftwareConfig();
    GetHardwareConfigResponse getHardConfigResp = service.getHardwareConfig();
    GetEMRBasicConfigResponse getBasicConfigResp = service.getEMRBasicConfig();
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
    context.setCreateClusterRequest(createClusterRequest);

    CreateClusterResponse createClusterResponse = service.createCluster(createClusterRequest);
    String clusterId = createClusterResponse.getClusterId();
    LOG.info("clusterId:" + clusterId);
    context.setClusterId(clusterId);
    context.setCreateClusterResponse(createClusterResponse);

    assertNotNull(clusterId);
    assertEquals(createClusterRequest.getName(), createClusterResponse.getName());

    int MAX_TIMEOUT = 6 * 60;
    long pollingStart = System.currentTimeMillis() / 1000;
    while (true) {
      Thread.sleep(5 * 1000);
      ClusterDetail clusterDetail = service.describeCluster(context.getClusterId());
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

  private String genClusterName() {
    UUID uuid = Generators.timeBasedGenerator().generate();
    return CLUSTER_NAME_PREFIX + uuid.toString();
  }

  @Test(dependsOnMethods = {"testCreateCluster"})
  public void testDescribeCluster() throws Exception {
    if (LOG.isDebugEnabled())
      LOG.debug("test describeCluster.");
    // describe cluster detail and check
    ClusterDetail clusterDetail = service.describeCluster(context.getClusterId());
    assertEquals(context.getCreateClusterRequest().getName(), clusterDetail.getName());
    assertEquals(context.getCreateClusterRequest().isAutoTerminate(),
        clusterDetail.isAutoTerminate());
    assertEquals(context.getCreateClusterRequest().getPurpose(),
        clusterDetail.getPurpose());
    assertEquals(context.getCreateClusterRequest().getKeyPair(),
        clusterDetail.getKeyPair());
    assertEquals(context.getCreateClusterRequest().isTerminationProtected(),
        clusterDetail.isTerminationProtected());
  }

  @Test(dependsOnMethods = {"testCreateCluster"})
  public void testDescribeInstanceGroup() throws Exception {
    if (LOG.isDebugEnabled())
      LOG.debug("test describeInstanceGroup.");
    List<AddInstanceGroupResponse> addInstanceGroupResponses =
        context.getCreateClusterResponse().getAddInstanceGroupResponses();
    List<AddInstanceGroupRequest> addInstanceGroupRequests =
        context.getCreateClusterRequest().getAddInstanceGroupRequests();
    Collections.sort(addInstanceGroupRequests, new Comparator<AddInstanceGroupRequest>() {
      public int compare(AddInstanceGroupRequest o1, AddInstanceGroupRequest o2) {
        return o1.getName().compareTo(o2.getName());
      }
    });
    Collections.sort(addInstanceGroupResponses, new Comparator<AddInstanceGroupResponse>() {
      public int compare(AddInstanceGroupResponse o1, AddInstanceGroupResponse o2) {
        return o1.getName().compareTo(o2.getName());
      }
    });
    assertEquals(addInstanceGroupRequests.size(), addInstanceGroupResponses.size());
    for (int i = 0; i < addInstanceGroupResponses.size(); i++) {
      String instanceGroupId = addInstanceGroupResponses.get(i).getInstanceGroupId();
      InstanceGroupDetail instanceGroupDetail = service.describeInstanceGroup(instanceGroupId);
      validateInstanceGroupDetail(addInstanceGroupRequests.get(i),
          instanceGroupDetail);
    }
  }

  @Test(dependsOnMethods = {"testCreateCluster"})
  public void testListClusters() throws Exception {
    if (LOG.isDebugEnabled())
      LOG.debug("test listClusters.");
    int listStartTime = (int) (System.currentTimeMillis() / 1000 - 8 * 60);
    int listStopTime = (int) (System.currentTimeMillis() / 1000);
    List<ClusterDetail> clusterDetails = service.listClusters(listStartTime, listStopTime);
    // make sure from 8 minutes ago util now there are more than 1 cluster is created
    // 8 minutes is a approximate value
    assertTrue(clusterDetails.size() >= 1);
    for (ClusterDetail detail : clusterDetails) {
      if (detail != null && detail.getClusterId().equals(context.getClusterId())) {
        validateClusterDetail(context.getCreateClusterRequest(), detail);
        return;
      }
    }
  }

  @Test(dependsOnMethods = {"testCreateCluster"})
  public void testListInstanceGroups() throws Exception {
    if (LOG.isDebugEnabled())
      LOG.debug("tset listInstanceGroups.");
    String clusterId = context.getClusterId();
    List<InstanceGroupDetail> instanceGroupDetails = service.listInstanceGroups(clusterId);
    List<AddInstanceGroupRequest> addInstanceGroupRequests
        = context.getCreateClusterRequest().getAddInstanceGroupRequests();
    Collections.sort(addInstanceGroupRequests, new Comparator<AddInstanceGroupRequest>() {
      public int compare(AddInstanceGroupRequest o1, AddInstanceGroupRequest o2) {
        return o1.getName().compareTo(o2.getName());
      }
    });
    Collections.sort(instanceGroupDetails, new Comparator<InstanceGroupDetail>() {
      public int compare(InstanceGroupDetail o1, InstanceGroupDetail o2) {
        return o1.getName().compareTo(o2.getName());
      }
    });
    assertEquals(addInstanceGroupRequests.size(), instanceGroupDetails.size());
    for (int i = 0; i < addInstanceGroupRequests.size(); i++) {
      validateInstanceGroupDetail(addInstanceGroupRequests.get(i), instanceGroupDetails.get(i));
    }
  }

  @Test(dependsOnMethods = {"testCreateCluster"})
  public void testDescribeJob() throws Exception {

  }

  @Test(dependsOnMethods = {"testCreateCluster"})
  public void testListJobs() throws Exception {

  }

  @Test(dependsOnMethods = {"testCreateCluster"})
  public void testListInstancesInGroup() throws Exception {
    if (LOG.isDebugEnabled())
      LOG.debug("test llistIntancesInGroup");
    List<AddInstanceGroupResponse> addInstanceGroupResponses =
        context.getCreateClusterResponse().getAddInstanceGroupResponses();
    List<AddInstanceGroupRequest> addInstanceGroupRequests =
        context.getCreateClusterRequest().getAddInstanceGroupRequests();
    assertEquals(addInstanceGroupRequests.size(), addInstanceGroupResponses.size());
    // requests order consistent with responses
    for (int i = 0; i < addInstanceGroupResponses.size(); i++) {
      String groupId = addInstanceGroupResponses.get(i).getInstanceGroupId();
      List<InstanceDetail> instanceDetails = service.listInstancesInGroup(
          context.getClusterId(),
          groupId,
          addInstanceGroupRequests.get(i).getRole()
      );
      validateInstanceDetailsInGroup(instanceDetails, addInstanceGroupRequests.get(i).getInstanceType());
    }
  }

  private void validateInstanceDetailsInGroup(List<InstanceDetail> instanceDetails, String instanceType) {
    for (InstanceDetail instanceDetail : instanceDetails) {
      validateInstanceDetailInGroup(instanceDetail, instanceType);
    }
  }

  private void validateInstanceDetailInGroup(InstanceDetail instanceDetail, String instanceType) {
    assertNotNull(instanceDetail.getInstanceId());
    assertEquals(instanceType, instanceDetail.getInstanceType());
    assertEquals(context.getClusterId(), instanceDetail.getName());
    assertNotNull(instanceDetail.getOsInstanceId());
    assertNotNull(instanceDetail.getPrivateIpAddress());
    assertNotNull(instanceDetail.getPublicIpAddress());
    assertEquals(StateCode.I_RUNNING, instanceDetail.getInstanceStatus().getState());
  }

  @Test(dependsOnMethods = {"testCreateCluster"})
  public void testListInstancesInCluster() throws Exception {
    if (LOG.isDebugEnabled())
      LOG.debug("test listInstanceInCluster.");
    List<InstanceDetail> instanceDetails = service.listInstancesInCluster(context.getClusterId());
    for (InstanceDetail instanceDetail : instanceDetails) {
      validateInstanceDetailInCluster(instanceDetail);
    }
  }

  private void validateInstanceDetailInCluster(InstanceDetail instanceDetail) {
    assertNotNull(instanceDetail.getInstanceId());
    assertNotNull(instanceDetail.getInstanceType());
    assertEquals(context.getClusterId(), instanceDetail.getName());
    assertNotNull(instanceDetail.getOsInstanceId());
    assertNotNull(instanceDetail.getPrivateIpAddress());
    assertNotNull(instanceDetail.getPublicIpAddress());
    assertEquals(StateCode.I_RUNNING, instanceDetail.getInstanceStatus().getState());
  }

  @Test(dependsOnMethods = {"testCreateCluster", "testDescribeCluster",
      "testDescribeInstanceGroup", "testListClusters", "testListInstanceGroups",
      "testDescribeJob", "testListJobs", "testListInstancesInGroup", "testListInstancesInCluster"})
  public void testSSHPublicKeysOperation() throws Exception {
    String clusterId = context.getClusterId();
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
    service.addSSHPublicKeys(add);
    // get ssh key
    GetSSHPublicKeysRequest get = new GetSSHPublicKeysRequest(clusterId);
    GetSSHPublicKeysResponse getResp = service.getSSHPublicKeys(get);
    assertEquals("title1", getResp.getPublicKeys().get(0).getTitle());
    assertEquals(keyContent, getResp.getPublicKeys().get(0).getContent());
    assertNotNull(getResp.getPublicKeys().get(0).getAddTime());
    assertNotNull(getResp.getPublicKeys().get(0).getFingerprint());
    assertEquals("2d:97:6e:16:0c:1a:d5:5c:f3:b5:f6:94:ff:86:7f:aa",
        getResp.getPublicKeys().get(0).getFingerprint());
    // delete ssh key
    DeleteSSHPublicKeysRequest delete = new DeleteSSHPublicKeysRequest(clusterId);
    delete.setPublicKeys(getResp.getPublicKeys());
    service.deleteSSHPublicKeys(delete);
  }

  @Test(dependsOnMethods = {"testSSHPublicKeysOperation"})
  public void testTerminateCluster() throws Exception {
    String clusterId = context.getClusterId();
    if (LOG.isDebugEnabled())
      LOG.debug("terminate cluster:" + clusterId);
    TerminateClusterRequest terminateClusterRequest = new TerminateClusterRequest(clusterId);
    service.terminateCluster(terminateClusterRequest);

    int MAX_TIMEOUT = 6 * 60;
    long terminateStart = System.currentTimeMillis() / 1000;
    while (true) {
      Thread.sleep(5 * 1000);
      ClusterDetail clusterDetail = service.describeCluster(context.getClusterId());
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

  @Test(dependsOnMethods = {"testTerminateCluster"})
  public void testDeleteCluster() throws Exception {
    String clusterId = context.getClusterId();
    if (LOG.isDebugEnabled())
      LOG.debug("delete cluster:" + clusterId);
    DeleteClusterRequest request = new DeleteClusterRequest(clusterId);
    DeleteClusterResponse response = service.deleteCluster(request);
    assertEquals(true, response.isSucceed());
    if (LOG.isDebugEnabled())
      LOG.debug("delete cluster succeed.");
  }

  private void validateJobDetail(SubmitJobRequest getJobReq, JobDetail describeJobDetail) {
    assertNotNull(describeJobDetail);
    assertEquals(getJobReq.getName(), describeJobDetail.getName());
    assertEquals(getJobReq.getJar(), describeJobDetail.getJar());
    assertEquals(getJobReq.getJarArgs(), describeJobDetail.getJarArgs());
    assertEquals(getJobReq.getJarMainClass(), describeJobDetail.getJarMainClass());
    assertEquals(getJobReq.getJarProperties(), describeJobDetail.getJarProperties());
  }

  private void validateJobDetails(CreateClusterRequest req, List<JobDetail> jobDetails) {
    assertNotNull(jobDetails);
    if (req.getSubmitJobRequests().size() != jobDetails.size())
      Assert.fail("submit job number not match request:\n" +
          " expected:" + req.getSubmitJobRequests().size() + ", actual:" + jobDetails.size());
    Collections.sort(req.getSubmitJobRequests(), new Comparator<SubmitJobRequest>() {
      @Override
      public int compare(SubmitJobRequest o1, SubmitJobRequest o2) {
        return o1.getName().compareTo(o2.getName());
      }
    });
    Collections.sort(jobDetails, new Comparator<JobDetail>() {
      @Override
      public int compare(JobDetail o1, JobDetail o2) {
        return o1.getName().compareTo(o2.getName());
      }
    });
    Iterator<SubmitJobRequest> jobIter = req.getSubmitJobRequestsIterator();
    Iterator<JobDetail> jobDetailIter = jobDetails.iterator();

    while (jobIter.hasNext() && jobDetailIter.hasNext()) {
      SubmitJobRequest jobRequest = jobIter.next();
      JobDetail jobDetail = jobDetailIter.next();
      assertEquals(jobRequest.getName(), jobDetail.getName());
      assertEquals(jobRequest.getJar(), jobDetail.getJar());
      assertEquals(jobRequest.getJarArgs(), jobDetail.getJarArgs());
      assertEquals(jobRequest.getJarMainClass(), jobDetail.getJarMainClass());
      assertEquals(jobRequest.getJarProperties(), jobDetail.getJarProperties());
    }
  }

  private void validateInstanceGroupDetail(AddInstanceGroupRequest instanceGroupRequest,
      InstanceGroupDetail instanceGroupDetail) {
    assertNotNull(instanceGroupDetail);
    assertEquals(instanceGroupRequest.getName(), instanceGroupDetail.getName());
    assertEquals(instanceGroupRequest.getRole(), instanceGroupDetail.getRole());
    assertEquals(instanceGroupRequest.getInstanceType(), instanceGroupDetail.getInstanceType());
    assertEquals(instanceGroupRequest.getRequestedInstanceCount(),
        instanceGroupDetail.getRequestedInstanceCount());
    assertEquals(instanceGroupRequest.getRequestedInstanceCount(),
        instanceGroupDetail.getRunningInstanceCount());
  }

  private void validateClusterDetail(CreateClusterRequest req, ClusterDetail clusterDetail) {
    assertEquals(req.getName(), clusterDetail.getName());
    assertEquals(req.isAutoTerminate(), clusterDetail.isAutoTerminate());
    assertEquals(req.isTerminationProtected(), clusterDetail.isTerminationProtected());
    assertEquals(req.getPurpose(), clusterDetail.getPurpose());
    assertEquals(req.getKeyPair(), clusterDetail.getKeyPair());
    assertEquals(req.getRegion(), clusterDetail.getRegion());
  }
}