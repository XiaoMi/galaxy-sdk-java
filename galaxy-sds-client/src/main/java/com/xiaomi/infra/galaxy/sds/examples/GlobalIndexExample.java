package com.xiaomi.infra.galaxy.sds.examples;

import com.xiaomi.infra.galaxy.sds.client.ClientFactory;
import com.xiaomi.infra.galaxy.sds.thrift.AdminService;
import com.xiaomi.infra.galaxy.sds.thrift.CannedAcl;
import com.xiaomi.infra.galaxy.sds.thrift.CommonConstants;
import com.xiaomi.infra.galaxy.sds.thrift.Credential;
import com.xiaomi.infra.galaxy.sds.thrift.DataType;
import com.xiaomi.infra.galaxy.sds.thrift.Datum;
import com.xiaomi.infra.galaxy.sds.thrift.DatumUtil;
import com.xiaomi.infra.galaxy.sds.thrift.EntityGroupSpec;
import com.xiaomi.infra.galaxy.sds.thrift.GlobalSecondaryIndexSpec;
import com.xiaomi.infra.galaxy.sds.thrift.KeySpec;
import com.xiaomi.infra.galaxy.sds.thrift.ProvisionThroughput;
import com.xiaomi.infra.galaxy.sds.thrift.PutRequest;
import com.xiaomi.infra.galaxy.sds.thrift.ScanRequest;
import com.xiaomi.infra.galaxy.sds.thrift.ScanResult;
import com.xiaomi.infra.galaxy.sds.thrift.TableMetadata;
import com.xiaomi.infra.galaxy.sds.thrift.TableQuota;
import com.xiaomi.infra.galaxy.sds.thrift.TableSchema;
import com.xiaomi.infra.galaxy.sds.thrift.TableService;
import com.xiaomi.infra.galaxy.sds.thrift.TableSpec;
import com.xiaomi.infra.galaxy.sds.thrift.UserType;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: linshangquan@xiaomi.com
 */
public class GlobalIndexExample {
  private static AdminService.Iface adminClient;
  private static TableService.Iface tableClient;
  private static String appId = ""; // Your AppId
  private static String appKey = ""; // Your AppKey
  private static String appSecret = ""; // Your AppSecret
  private static String accountKey = ""; // Your AccountKey
  private static String accountSecret = ""; // Your AccountSecret
  private static String endpoint = "http://awsbj0.sds.api.xiaomi.com";
  private static boolean isInit = false;
  private static String tableName = "java-test-global-index";

  private static Credential getCredential(String secretKeyId, String secretKey, UserType userType) {
    return new Credential().setSecretKeyId(secretKeyId).setSecretKey(secretKey)
        .setType(userType);
  }

  public static AdminService.Iface createAdminClient(String host) {
    Credential credential = getCredential(accountKey, accountSecret, UserType.DEV_XIAOMI);
    ClientFactory clientFactory = new ClientFactory().setCredential(credential);
    return clientFactory.newAdminClient(host + CommonConstants.ADMIN_SERVICE_PATH, 50000, 3000);
  }

  public static TableService.Iface createTableClient(String host) {
    Credential credential = getCredential(appKey, appSecret, UserType.APP_SECRET);
    ClientFactory clientFactory = new ClientFactory().setCredential(credential);
    return clientFactory.newTableClient(host + CommonConstants.TABLE_SERVICE_PATH, 10000, 3000,
        true, 5);
  }

  private static TableSpec tableSpec() {
    EntityGroupSpec entityGroupSpec = new EntityGroupSpec().setAttributes(
        Arrays.asList(new KeySpec().setAttribute("userId"))).setEnableHash(
        true);
    List<KeySpec> primaryKey = Arrays.asList(new KeySpec().setAttribute("time"),
        new KeySpec().setAttribute("deviceId"));
    Map<String, GlobalSecondaryIndexSpec> indexes = new HashMap<String, GlobalSecondaryIndexSpec>();
    GlobalSecondaryIndexSpec didIndex = new GlobalSecondaryIndexSpec();
    didIndex.setIndexEntityGroup(
        new EntityGroupSpec().setAttributes(Arrays.asList(new KeySpec().setAttribute("deviceId")))
            .setEnableHash(true));
    didIndex.setIndexPrimaryKey(Arrays.asList(new KeySpec().setAttribute("time").setAsc(false)));
    didIndex.setProjections(Arrays.asList("value"));
    didIndex.setThroughput(new ProvisionThroughput().setReadCapacity(20).setWriteCapacity(20));

    GlobalSecondaryIndexSpec timeIndex = new GlobalSecondaryIndexSpec();
    timeIndex.setIndexPrimaryKey(Arrays.asList(new KeySpec().setAttribute("time").setAsc(false)));
    timeIndex.setProjections(Arrays.asList("value"));
    timeIndex.setThroughput(new ProvisionThroughput().setReadCapacity(20).setWriteCapacity(20));
    indexes.put("didIndex", didIndex);
    indexes.put("timeIndex", timeIndex);

    Map<String, DataType> attributes = new HashMap<String, DataType>();
    attributes.put("userId", DataType.STRING);
    attributes.put("deviceId", DataType.STRING);
    attributes.put("time", DataType.INT64);
    attributes.put("value", DataType.STRING);
    TableSchema tableSchema = new TableSchema();
    tableSchema.setEntityGroup(entityGroupSpec)
        .setPrimaryIndex(primaryKey)
        .setGlobalSecondaryIndexes(indexes)
        .setAttributes(attributes)
        .setTtl(-1);

    TableMetadata tableMetadata = new TableMetadata();
    tableMetadata
        .setQuota(new TableQuota().setSize(100 * 1024 * 1024))
        .setAppAcl(cannedAclGrant(appId, CannedAcl.APP_SECRET_READ, CannedAcl.APP_SECRET_WRITE))
        .setThroughput(new ProvisionThroughput().setReadCapacity(20).setWriteCapacity(20));

    return new TableSpec().setSchema(tableSchema)
        .setMetadata(tableMetadata);
  }

  public static Map<String, List<CannedAcl>> cannedAclGrant(String appId, CannedAcl... cannedAcls) {
    Map<String, List<CannedAcl>> appGrant = new HashMap<String, List<CannedAcl>>();
    appGrant.put(appId, Arrays.asList(cannedAcls));
    return appGrant;
  }

  private static void init() {
    adminClient = createAdminClient(endpoint);
    tableClient = createTableClient(endpoint);
    isInit = true;
  }

  private static void printResult(Map<String, Datum> resultToPrint) {
    for (Map.Entry<String, Datum> e : resultToPrint.entrySet()) {
      System.out.println(
          String.format("[%s] => %s", e.getKey(), DatumUtil.fromDatum(e.getValue()).toString()));
    }
  }

  public static void main(String[] args) throws Exception {
    init();
    TableSpec tableSpec = tableSpec();
    adminClient.createTable(tableName, tableSpec);
    try {
      // put data
      System.out.println("================= put data ====================");
      for (int i = 0; i < 5; i++) {
        for (int j = 0; j < 5; j++) {
          PutRequest putRequest = new PutRequest();
          putRequest.setTableName(tableName);
          putRequest.putToRecord("userId", DatumUtil.toDatum("user" + i));
          putRequest.putToRecord("deviceId", DatumUtil.toDatum("device" + i));
          putRequest.putToRecord("time", DatumUtil.toDatum((long) j));
          putRequest.putToRecord("value", DatumUtil.toDatum("value " + i + "-" + j));
          tableClient.put(putRequest);
        }
      }


      //scan by didIndex to get data that time between 2 ~ 4 and deviceId = device1
      System.out.println("================= scan by didIndex ====================");
      ScanRequest scanRequest = new ScanRequest();
      HashMap<String, Datum> startKey = new HashMap<String, Datum>();
      HashMap<String, Datum> stopKey = new HashMap<String, Datum>();
      scanRequest.setTableName(tableName);
      scanRequest.setIndexName("didIndex");
      startKey.clear();
      startKey.put("deviceId", DatumUtil.toDatum("device1"));
      startKey.put("time", DatumUtil.toDatum(4l));
      stopKey.clear();
      stopKey.put("deviceId", DatumUtil.toDatum("device1"));
      stopKey.put("time", DatumUtil.toDatum(2l));
      scanRequest.setStartKey(startKey);
      scanRequest.setStopKey(stopKey);
      scanRequest.setAttributes(Arrays.asList("userId", "value", "time"));
      scanRequest.setLimit(10);
      ScanResult scanResult = tableClient.scan(scanRequest);
      List<Map<String, Datum>>kvsList = scanResult.getRecords();
      for (Map<String, Datum> kvs : kvsList) {
        printResult(kvs);
      }

      // scan by timeIndex to get all data between 1 ~ 3
      System.out.println("================= scan by timeIndex ====================");
      scanRequest.setIndexName("timeIndex");
      startKey.clear();
      startKey.put("time", DatumUtil.toDatum(3l));
      stopKey.clear();
      stopKey.put("time", DatumUtil.toDatum(1l));
      scanRequest.setStartKey(startKey);
      scanRequest.setStopKey(stopKey);
      scanRequest.setAttributes(Arrays.asList("userId", "value", "time"));
      scanRequest.setLimit(10);
      scanResult = tableClient.scan(scanRequest);
      kvsList = scanResult.getRecords();
      for (Map<String, Datum> kvs : kvsList) {
        printResult(kvs);
      }
    } finally {
      adminClient.disableTable(tableName);
      adminClient.dropTable(tableName);
    }
  }
}
