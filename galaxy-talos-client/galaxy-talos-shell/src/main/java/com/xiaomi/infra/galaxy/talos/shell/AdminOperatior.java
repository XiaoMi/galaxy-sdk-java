package com.xiaomi.infra.galaxy.talos.shell;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

import libthrift091.TException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xiaomi.infra.galaxy.rpc.thrift.Credential;
import com.xiaomi.infra.galaxy.rpc.thrift.UserType;
import com.xiaomi.infra.galaxy.talos.admin.TalosAdmin;
import com.xiaomi.infra.galaxy.talos.client.TalosClientConfig;
import com.xiaomi.infra.galaxy.talos.thrift.ApplyQuotaRequest;
import com.xiaomi.infra.galaxy.talos.thrift.ApproveQuotaRequest;
import com.xiaomi.infra.galaxy.talos.thrift.CreateTopicRequest;
import com.xiaomi.infra.galaxy.talos.thrift.DeleteTopicRequest;
import com.xiaomi.infra.galaxy.talos.thrift.DescribeTopicRequest;
import com.xiaomi.infra.galaxy.talos.thrift.GalaxyTalosException;
import com.xiaomi.infra.galaxy.talos.thrift.GetDescribeInfoRequest;
import com.xiaomi.infra.galaxy.talos.thrift.GetDescribeInfoResponse;
import com.xiaomi.infra.galaxy.talos.thrift.GetPartitionOffsetRequest;
import com.xiaomi.infra.galaxy.talos.thrift.GetScheduleInfoRequest;
import com.xiaomi.infra.galaxy.talos.thrift.GetTopicOffsetRequest;
import com.xiaomi.infra.galaxy.talos.thrift.GetWorkerIdRequest;
import com.xiaomi.infra.galaxy.talos.thrift.GetWorkerIdResponse;
import com.xiaomi.infra.galaxy.talos.thrift.ListPendingQuotaResponse;
import com.xiaomi.infra.galaxy.talos.thrift.ListQuotaResponse;
import com.xiaomi.infra.galaxy.talos.thrift.OffsetInfo;
import com.xiaomi.infra.galaxy.talos.thrift.PartitionQuotaInfo;
import com.xiaomi.infra.galaxy.talos.thrift.RevokeQuotaRequest;
import com.xiaomi.infra.galaxy.talos.thrift.Topic;
import com.xiaomi.infra.galaxy.talos.thrift.TopicAndPartition;
import com.xiaomi.infra.galaxy.talos.thrift.TopicAttribute;
import com.xiaomi.infra.galaxy.talos.thrift.TopicInfo;
import com.xiaomi.infra.galaxy.talos.thrift.TopicTalosResourceName;

public class AdminOperatior {
  static String line = "------------------------------------------------------------------------";
  private static final Logger LOG = LoggerFactory.getLogger(TalosAdminShell.class);
  private static final String pathname = System.getProperty("talos.shell.dist.path");
  private static final Integer RETENTION_TIME_BASE = 60 * 60 * 24;
  private static final Integer RETENTION_TIME_BASE_DAY = 1000 * 60 * 60 * 24;
  private TalosAdmin talosAdmin;
  private Credential credential;
  private String clusterName;
  private Utils utils;

  public AdminOperatior() {
    String conf = System.getProperty("talos.shell.conf");
    Properties properties = TalosClientConfig.loadProperties(conf);
    String accessKey = properties.getProperty("galaxy.talos.access.key");
    String accessSecret = properties.getProperty("galaxy.talos.access.secret");
    TalosClientConfig clientConfig = new TalosClientConfig(properties);
    credential = new Credential();
    credential.setSecretKeyId(accessKey)
        .setSecretKey(accessSecret)
        .setType(UserType.DEV_XIAOMI);
    talosAdmin = new TalosAdmin(clientConfig, credential);
    clusterName = properties.getProperty("galaxy.talos.cluster.name");
    utils = new Utils();
  }

  public void createTopic(String topicName, String orgId, int partitionNumber)
      throws TException {
    TopicAttribute topicAttribute = new TopicAttribute()
        .setPartitionNumber(partitionNumber);
    String cloudTopicName = orgId + "/" + topicName;
    CreateTopicRequest request = new CreateTopicRequest()
        .setTopicName(cloudTopicName)
        .setTopicAttribute(topicAttribute);
    try {
      talosAdmin.createTopic(request);
      System.out.println("createTopic OK!");
    } catch (GalaxyTalosException e) {
      LOG.error("Exception in createTopic:", e);
      System.out.println("input error: " + e.getDetails());
      return;
    } catch (Exception e) {
      System.out.println("An error occurred, please check if the configuration" +
          " file is correct");
      return;
    }
  }

  public void deleteTopic(String topicName) throws TException {
    Topic topic;
    try {
      topic = talosAdmin.describeTopic(new DescribeTopicRequest(topicName));
      System.out.println("deleteTopic OK!");
    } catch (GalaxyTalosException e) {
      LOG.error("Exception in describeTopic:", e);
      System.out.println("input error: " + e.getDetails());
      return;
    } catch (Exception e) {
      System.out.println("An error occurred, please check if the configuration" +
          " file is correct");
      return;
    }

    TopicTalosResourceName resourceName = topic.getTopicInfo()
        .getTopicTalosResourceName();
    DeleteTopicRequest request = new DeleteTopicRequest()
        .setTopicTalosResourceName(resourceName);
    talosAdmin.deleteTopic(request);
  }

  public void describeTopic(String topicName) throws TException {
    Topic topic;
    try {
      topic = talosAdmin.describeTopic(new DescribeTopicRequest(topicName));
    } catch (GalaxyTalosException e) {
      LOG.error("Exception in describeTopic:", e);
      System.out.println("input error: " + e.getDetails());
      return;
    } catch (Exception e) {
      System.out.println("An error occurred, please check if the configuration" +
          " file is correct");
      return;
    }
    String str = "OrgId:         " + topic.getTopicInfo().getOwnerId() + "\n"
        + "PartitionNum:  " + Integer.toString(topic.getTopicAttribute()
        .getPartitionNumber()) + "\n"
        + "RetentionDay:  " + Integer.toString(topic.getTopicAttribute()
        .getMessageRetentionSecs() / RETENTION_TIME_BASE) + "\n"
        + "CreateDate:    " + Long.toString(topic.getTopicState()
        .getCreateTimestamp() / RETENTION_TIME_BASE_DAY) + "\n"
        + "MsgNum:        " + Long.toString(topic.getTopicState()
        .getMessageNumber()) + "\n"
        + "MsgBytes:      " + Long.toString(topic.getTopicState()
        .getMessageBytes()) + "\n"
        + "TotalMsgNum:   " + Long.toString(topic.getTopicState()
        .getTotalMessageNumber()) + "\n"
        + "TotalMsgBytes: " + Long.toString(topic.getTopicState()
        .getTotalMessageBytes()) + "\n";
    System.out.println(str);
    Map<String, Integer> topicAcl = topic.getTopicAcl();
    AdminOperatior adminOperatior = new AdminOperatior();
    adminOperatior.getAclMap(topicAcl);
  }

  public static void getAclMap(Map<String, Integer> map) {
    String str = StringUtils.rightPad("TeamID", 15, ' ') +
        StringUtils.rightPad("Permission", 30, ' ') + '\n';
    Utils utils = new Utils();
    for (String teamId : map.keySet()) {
      str += StringUtils.rightPad(teamId, 15, ' ') +
          StringUtils.rightPad(utils.idToPermission(map.get(teamId)),
              30, ' ') + '\n';
    }
    System.out.println(str);
  }

  public void listTopic() throws Exception {
    List<TopicInfo> topicInfoList;
    try {
      topicInfoList = talosAdmin.listTopic();
    } catch (Exception e) {
      System.out.println("An error occurred, please check if the configuration" +
          " file is correct");
      return;
    }
    String fileName = utils.getFileName(pathname, clusterName, "TopicInfo");
    File outputFileName = new File(fileName);
    outputFileName.createNewFile();
    BufferedWriter bufferWriter = new BufferedWriter(new FileWriter(outputFileName));
    String str = StringUtils.rightPad("topicTalosResourceName", 100, ' ') +
        StringUtils.rightPad("orgId", 10, ' ') + "\n";
    for (TopicInfo list : topicInfoList) {
      str += StringUtils.rightPad(list.getTopicTalosResourceName()
          .getTopicTalosResourceName(), 100, ' ') +
          StringUtils.rightPad(list.getOwnerId(), 10, ' ') + "\n";
    }
    System.out.println(str);
    bufferWriter.write(str);
    bufferWriter.flush();
    bufferWriter.close();
    System.out.println("Please go to the corresponding file to view:  " + fileName);
  }

  public void listTopicsinfo() throws Exception {
    List<Topic> topicList;
    try {
      topicList = talosAdmin.listTopicsinfo();
    } catch (Exception e) {
      System.out.println("An error occurred, please check if the configuration" +
          " file is correct");
      return;
    }
    String fileName = utils.getFileName(pathname, clusterName, "Topic");
    File outputFileName = new File(fileName);
    outputFileName.createNewFile();
    BufferedWriter bufferWriter = new BufferedWriter(new FileWriter(outputFileName));
    String str = StringUtils.rightPad("TopicName", 30, ' ') +
        StringUtils.rightPad("OrgId", 10, ' ') +
        StringUtils.rightPad("PartitionNum", 15, ' ') +
        StringUtils.rightPad("RetentionDay", 15, ' ') +
        StringUtils.rightPad("CreateDate", 15, ' ') +
        StringUtils.rightPad("MsgNum", 10, ' ') +
        StringUtils.rightPad("MsgBytes", 10, ' ') +
        StringUtils.rightPad("TotalMsgNum", 15, ' ') +
        StringUtils.rightPad("TotalMsgBytes", 15, ' ') +
        "\n";
    for (Topic list : topicList) {
      if (list.getTopicInfo().getTopicName().length() < 30) {
        str += StringUtils.rightPad(list.getTopicInfo().getTopicName(), 30, ' ') +
            StringUtils.rightPad(list.getTopicInfo().getOwnerId(), 10, ' ') +
            StringUtils.rightPad(Integer.toString(list.getTopicAttribute()
                .getPartitionNumber()), 15, ' ') +
            StringUtils.rightPad(Integer.toString(list.getTopicAttribute()
                .getMessageRetentionSecs() / RETENTION_TIME_BASE), 15, ' ') +
            StringUtils.rightPad(Long.toString(list.getTopicState()
                .getCreateTimestamp() / RETENTION_TIME_BASE_DAY), 15, ' ') +
            StringUtils.rightPad(Long.toString(list.getTopicState()
                .getMessageNumber()), 15, ' ') +
            StringUtils.rightPad(Long.toString(list.getTopicState()
                .getMessageBytes()), 15, ' ') +
            StringUtils.rightPad(Long.toString(list.getTopicState()
                .getTotalMessageNumber()), 15, ' ') +
            StringUtils.rightPad(Long.toString(list.getTopicState()
                .getTotalMessageBytes()), 15, ' ') +
            "\n";
      } else {
        str += StringUtils.rightPad(list.getTopicInfo()
            .getTopicName(), 30, ' ') + "  " +
            StringUtils.rightPad(list.getTopicInfo().getOwnerId(), 10, ' ') + "  " +
            StringUtils.rightPad(Integer.toString(list.getTopicAttribute()
                .getPartitionNumber()), 15, ' ') + "  " +
            StringUtils.rightPad(Integer.toString(list.getTopicAttribute()
                .getMessageRetentionSecs() / RETENTION_TIME_BASE), 15, ' ') + "  " +
            StringUtils.rightPad(Long.toString(list.getTopicState()
                .getCreateTimestamp() / RETENTION_TIME_BASE_DAY), 15, ' ') + "  " +
            StringUtils.rightPad(Long.toString(list.getTopicState()
                .getMessageNumber()), 15, ' ') + "  " +
            StringUtils.rightPad(Long.toString(list.getTopicState()
                .getMessageBytes()), 15, ' ') + "  " +
            StringUtils.rightPad(Long.toString(list.getTopicState()
                .getTotalMessageNumber()), 15, ' ') + "  " +
            StringUtils.rightPad(Long.toString(list.getTopicState()
                .getTotalMessageBytes()), 15, ' ') + "  " +
            "\n";
      }
    }
    System.out.println(str);
    bufferWriter.write(str);
    bufferWriter.flush();
    bufferWriter.close();
    System.out.println("Please go to the corresponding file to view:  " + fileName);
  }

  public void getTopicOffset(String topicName) throws Exception {
    Topic topic;
    try {
      topic = talosAdmin.describeTopic(new DescribeTopicRequest(topicName));
    } catch (GalaxyTalosException e) {
      LOG.error("Exception in describeTopic:", e);
      System.out.println("input error: " + e.getDetails());
      return;
    } catch (Exception e) {
      System.out.println("An error occurred, please check if the configuration" +
          " file is correct");
      return;
    }
    TopicTalosResourceName resourceName = topic.getTopicInfo()
        .getTopicTalosResourceName();
    GetTopicOffsetRequest request = new GetTopicOffsetRequest()
        .setTopicTalosResourceName(resourceName);
    List<OffsetInfo> offsetInfoList = talosAdmin.getTopicOffset(request);
    File outputFileName = new File(pathname + "dist.file");
    outputFileName.createNewFile();
    BufferedWriter bufferWriter = new BufferedWriter(new FileWriter(outputFileName));
    TreeMap<Integer, OffsetInfo> map = new TreeMap<Integer, OffsetInfo>();
    for (OffsetInfo list : offsetInfoList) {
      map.put(list.getPartitionId(), list);
    }
    String str = "cluster: " + clusterName + "\n" +
        StringUtils.rightPad("PartitionId", 15, ' ') +
        StringUtils.rightPad("StartOffset", 15, ' ') +
        StringUtils.rightPad("EndOffset", 15, ' ') + "\n";
    for (Integer partitionid : map.keySet()) {
      str += StringUtils.rightPad(Long.toString(partitionid), 15, ' ') +
          StringUtils.rightPad(Long.toString(map.get(partitionid).getStartOffset()),
              15, ' ') +
          StringUtils.rightPad(Long.toString(map.get(partitionid).getEndOffset()),
              15, ' ') + "\n";
    }
    System.out.println(str);
    bufferWriter.write(str);
    bufferWriter.flush();
    bufferWriter.close();
    System.out.println("Please go to the corresponding file to view:  " +
        pathname + "dist.file");
  }

  public void getPartitionOffset(String topicName, int partitionId)
      throws TException {
    Topic topic;
    try {
      topic = talosAdmin.describeTopic(new DescribeTopicRequest(topicName));
    } catch (GalaxyTalosException e) {
      LOG.error("Exception in describeTopic:", e);
      System.out.println("input error: " + e.getDetails());
      return;
    } catch (Exception e) {
      System.out.println("An error occurred, please check if the configuration" +
          " file is correct");
      return;
    }
    TopicTalosResourceName resourceName = topic.getTopicInfo()
        .getTopicTalosResourceName();
    TopicAndPartition topicAndPartition = new TopicAndPartition()
        .setTopicName(topicName)
        .setPartitionId(partitionId)
        .setTopicTalosResourceName(resourceName);
    GetPartitionOffsetRequest request = new GetPartitionOffsetRequest()
        .setTopicAndPartition(topicAndPartition);
    OffsetInfo offsetInfo = talosAdmin.getPartitionOffset(request);
    if (offsetInfo.getErrorMsg() != null) {
      System.out.println("input error, please check the parameter: i");
      return;
    }
    System.out.println("PartitionId: " + offsetInfo.getPartitionId() + "   " +
        "StartOffset: " + offsetInfo.getStartOffset() + "   " +
        "EndOffset: " + offsetInfo.getEndOffset());
  }

  public void getScheduleInfo(String topicName) throws Exception {
    Topic topic;
    try {
      topic = talosAdmin.describeTopic(new DescribeTopicRequest(topicName));
    } catch (GalaxyTalosException e) {
      LOG.error("Exception in describeTopic:", e);
      System.out.println("input error: " + e.getDetails());
      return;
    } catch (Exception e) {
      System.out.println("An error occurred, please check if the configuration" +
          " file is correct");
      return;
    }
    TopicTalosResourceName resourceName = topic.getTopicInfo()
        .getTopicTalosResourceName();
    GetScheduleInfoRequest request = new GetScheduleInfoRequest()
        .setTopicTalosResourceName(resourceName);
    Map<TopicAndPartition, String> topicAndPartitionMap = talosAdmin
        .getScheduleInfo(request);
    File outputFileName = new File(pathname + "dist.file");
    outputFileName.createNewFile();
    BufferedWriter bufferWriter = new BufferedWriter(new FileWriter(outputFileName));
    List<Map.Entry<TopicAndPartition, String>> list =
        new ArrayList<Map.Entry<TopicAndPartition, String>>();
    list.addAll(topicAndPartitionMap.entrySet());
    Collections.sort(list, new Comparator<Map.Entry<TopicAndPartition, String>>() {
      public int compare(Map.Entry<TopicAndPartition, String> o1,
                         Map.Entry<TopicAndPartition, String> o2) {
        return o1.getKey().partitionId - o2.getKey().partitionId;
      }
    });
    String str = StringUtils.rightPad("TopicName", 50, ' ') +
        StringUtils.rightPad("OrgId", 20, ' ') +
        StringUtils.rightPad("PartitionId", 20, ' ') +
        StringUtils.rightPad("Info", 20, ' ') + "\n";
    for (Iterator<Map.Entry<TopicAndPartition, String>> it = list.iterator();
         it.hasNext(); ) {
      TopicAndPartition topicAndPartition = it.next().getKey();
      String[] topicTalosResourceName = topicAndPartition.getTopicTalosResourceName()
          .getTopicTalosResourceName().split("#");
      str += StringUtils.rightPad(topicAndPartition.getTopicName(), 50, ' ') +
          StringUtils.rightPad(topicTalosResourceName[0], 20, ' ') +
          StringUtils.rightPad(Integer.toString(topicAndPartition.getPartitionId()),
              20, ' ') +
          StringUtils.rightPad(topicAndPartitionMap.get(topicAndPartition),
              20, ' ') + "\n";
    }
    System.out.println(str);
    bufferWriter.write(str);
    bufferWriter.flush();
    bufferWriter.close();
    System.out.println("Please go to the corresponding file to view:  " +
        pathname + "dist.file");
  }

  public void applyQuota(String orgId, int totalQuota) throws Exception {
    PartitionQuotaInfo quotaInfo = new PartitionQuotaInfo().setOrgId(orgId)
        .setTotalQuota(totalQuota);
    ApplyQuotaRequest request = new ApplyQuotaRequest().setQuotaInfo(quotaInfo);
    try {
      talosAdmin.applyQuota(request);
      System.out.println("applyQuota OK!");
    } catch (GalaxyTalosException e) {
      LOG.error("Exception in applyQuota:", e);
      System.out.println("input error: " + e.getDetails());
    } catch (Exception e) {
      System.out.println("An error occurred, please check if the configuration" +
          " file is correct"+ e);
      LOG.error("Exception in applyQuota:", e);
      return;
    }
  }

  public void approveQuota(String orgId, int totalQuota) throws Exception {
    PartitionQuotaInfo quotaInfo = new PartitionQuotaInfo().setOrgId(orgId)
        .setTotalQuota(totalQuota);
    ApproveQuotaRequest request = new ApproveQuotaRequest().setQuotaInfo(quotaInfo);
    try {
      talosAdmin.approveQuota(request);
      System.out.println("approveQuota OK!");
    } catch (GalaxyTalosException e) {
      LOG.error("Exception in createTopic:", e);
      System.out.println("input error: " + e.getDetails());
    } catch (Exception e) {
      System.out.println("An error occurred, please check if the configuration" +
          " file is correct");
      return;
    }
  }

  public void setQuota(String orgId, int totalQuota) throws Exception {
    applyQuota(orgId, totalQuota);
    approveQuota(orgId, totalQuota);
  }

  public void revokeQuota(String orgId, int totalQuota) throws Exception {
    PartitionQuotaInfo quotaInfo = new PartitionQuotaInfo().setOrgId(orgId)
        .setTotalQuota(totalQuota);
    RevokeQuotaRequest request = new RevokeQuotaRequest().setQuotaInfo(quotaInfo);
    try {
      talosAdmin.revokeQuota(request);
      System.out.println("revokeQuota OK!");
    } catch (GalaxyTalosException e) {
      LOG.error("Exception in createTopic:", e);
      System.out.println("input error: " + e.getDetails());
    } catch (Exception e) {
      System.out.println("An error occurred, please check if the configuration" +
          " file is correct");
      return;
    }
  }

  public void listQuota() throws Exception {
    ListQuotaResponse response;
    try {
      response = talosAdmin.listQuota();
    } catch (Exception e) {
      System.out.println("An error occurred, please check if the configuration" +
          " file is correct");
      return;
    }
    String fileName = utils.getFileName(pathname, clusterName, "Quota");
    File outputFileName = new File(fileName);
    outputFileName.createNewFile();
    BufferedWriter bufferWriter = new BufferedWriter(new FileWriter(outputFileName));
    String str = "ApprovedTotalQuota: " + response.getApprovedTotalQuota() + '\n';
    str += StringUtils.rightPad("OrgId", 20, ' ') +
        StringUtils.rightPad("TotalQuota", 20, ' ') +
        StringUtils.rightPad("UsedOuota", 20, ' ') + '\n';
    List<PartitionQuotaInfo> quotaList = response.getQuotaList();
    for (PartitionQuotaInfo quotaInfo : quotaList) {
      str += StringUtils.rightPad(quotaInfo.getOrgId(), 20, ' ') +
          StringUtils.rightPad(Integer.toString(quotaInfo.getTotalQuota()),
              20, ' ') +
          StringUtils.rightPad(Integer.toString(quotaInfo.getUsedOuota()),
              20, ' ') + '\n';
    }
    System.out.println(str);
    bufferWriter.write(str);
    bufferWriter.flush();
    bufferWriter.close();
    System.out.println("Please go to the corresponding file to view:  " + fileName);
  }

  public void listPendingQuota() throws Exception {
    ListPendingQuotaResponse response;
    try {
      response = talosAdmin.listPendingQuota();
    } catch (Exception e) {
      System.out.println("An error occurred, please check if the configuration" +
          " file is correct");
      return;
    }
    String fileName = utils.getFileName(pathname, clusterName, "PendQuota");
    File outputFileName = new File(fileName);
    outputFileName.createNewFile();
    BufferedWriter bufferWriter = new BufferedWriter(new FileWriter(outputFileName));
    boolean talosAdmin = response.talosAdmin;
    String str = "TalosAdmin: " + talosAdmin + '\n';
    str += StringUtils.rightPad("OrgId", 20, ' ') +
        StringUtils.rightPad("TotalQuota", 20, ' ') +
        StringUtils.rightPad("UsedOuota", 20, ' ') + '\n';
    List<PartitionQuotaInfo> pendingQuotaList = response.getPendingQuotaList();
    for (PartitionQuotaInfo quotaInfo : pendingQuotaList) {
      String OrgId = quotaInfo.getOrgId();
      Integer TotalQuota = quotaInfo.getTotalQuota();
      Integer UsedOuota = quotaInfo.getUsedOuota();
      str += StringUtils.rightPad(OrgId, 20, ' ') +
          StringUtils.rightPad(Integer.toString(TotalQuota), 20, ' ') +
          StringUtils.rightPad(Integer.toString(UsedOuota), 20, ' ') + '\n';
    }
    System.out.println(str);
    bufferWriter.write(str);
    bufferWriter.flush();
    bufferWriter.close();
    System.out.println("Please go to the corresponding file to view:  " + fileName);
  }

  Map<String, List<String>> hostMap = new HashMap<String, List<String>>();

  public void getTopicPartitionSet() throws Exception {
    List<TopicInfo> topicInfoList;
    try {
      topicInfoList = talosAdmin.listTopic();
    } catch (Exception e) {
      System.out.println("An error occurred, please check if the configuration" +
          " file is correct");
      return;
    }
    for (TopicInfo list : topicInfoList) {
      TopicTalosResourceName resourceName = list.getTopicTalosResourceName();
      GetScheduleInfoRequest request = new GetScheduleInfoRequest()
          .setTopicTalosResourceName(resourceName);
      try {
        Map<TopicAndPartition, String> topicAndPartitionMap = talosAdmin
            .getScheduleInfo(request);
        getTopicInfoToHostMap(topicAndPartitionMap);
      } catch (GalaxyTalosException e) {
        LOG.error("GalaxyTalosException", e);
      }
    }
    getHostMapInfo();
    System.out.println("Please go to the corresponding file to view:  " +
        pathname + "dist.file");
  }

  public void getTopicInfoToHostMap(Map<TopicAndPartition, String> map) {
    for (TopicAndPartition key : map.keySet()) {
      String Host = map.get(key);
      String TopicName = key.getTopicName();
      String PartitionId = Integer.toString(key.getPartitionId());
      if (hostMap.get(Host) == null) {
        List<String> topicAndPartitionList = new ArrayList<String>();
        topicAndPartitionList.add(TopicName + "-" + PartitionId);
        hostMap.put(Host, topicAndPartitionList);
      } else {
        List<String> topicAndPartitionList = hostMap.get(Host);
        topicAndPartitionList.add(TopicName + "-" + PartitionId);
        hostMap.put(Host, topicAndPartitionList);
      }
    }
  }

  public void getHostMapInfo() throws Exception {
    File outputFileName = new File(pathname + "dist.file");
    outputFileName.createNewFile();
    BufferedWriter bufferWriter = new BufferedWriter(new FileWriter(outputFileName));
    String str = StringUtils.rightPad("Host", 30, ' ') +
        StringUtils.rightPad("TopicAndPartitionId", 10, ' ') + "\n";
    for (String Host : hostMap.keySet()) {
      List<String> TopicAndPartitionIdList = hostMap.get(Host);
      for (String TopicAndPartitionId : TopicAndPartitionIdList) {
        str += StringUtils.rightPad(Host, 30, ' ') +
            StringUtils.rightPad(TopicAndPartitionId, 10, ' ') + "\n";
      }
      str += line + "\n";
    }
    bufferWriter.write(str);
    bufferWriter.flush();
    bufferWriter.close();
    System.out.println(str);
  }

  public void getWorkerId(String topicName, int partitionId, String consumerGroupName) throws Exception {
    GetDescribeInfoResponse response = talosAdmin.getDescribeInfo(new GetDescribeInfoRequest(topicName));
    TopicTalosResourceName resouceName = response.getTopicTalosResourceName();
    TopicAndPartition topicAndPartition = new TopicAndPartition(topicName, resouceName, partitionId);
    GetWorkerIdRequest request = new GetWorkerIdRequest(topicAndPartition, consumerGroupName);
    String workerId = talosAdmin.getWorkerId(request);
    if (workerId == null || workerId.equals("")) {
      System.out.println("Not exist consumer client currently.");
      return;
    }
    System.out.println("ConsumerGroup: " + consumerGroupName + ", Topic: " +
        topicName + ", partitionId: " + partitionId + ", has consumer client: " + workerId);
  }
}