package com.xiaomi.infra.galaxy.sds.client.examples;

import com.xiaomi.infra.galaxy.sds.client.ClientFactory;
import com.xiaomi.infra.galaxy.sds.client.TableScanner;
import com.xiaomi.infra.galaxy.sds.thrift.AdminService;
import com.xiaomi.infra.galaxy.sds.thrift.CommonConstants;
import com.xiaomi.infra.galaxy.sds.thrift.Credential;
import com.xiaomi.infra.galaxy.sds.thrift.DataType;
import com.xiaomi.infra.galaxy.sds.thrift.Datum;
import com.xiaomi.infra.galaxy.sds.thrift.DatumUtil;
import com.xiaomi.infra.galaxy.sds.thrift.KeySpec;
import com.xiaomi.infra.galaxy.sds.thrift.ProvisionThroughput;
import com.xiaomi.infra.galaxy.sds.thrift.PutRequest;
import com.xiaomi.infra.galaxy.sds.thrift.RemoveRequest;
import com.xiaomi.infra.galaxy.sds.thrift.Request;
import com.xiaomi.infra.galaxy.sds.thrift.ScanAction;
import com.xiaomi.infra.galaxy.sds.thrift.ScanOp;
import com.xiaomi.infra.galaxy.sds.thrift.ScanRequest;
import com.xiaomi.infra.galaxy.sds.thrift.TableMetadata;
import com.xiaomi.infra.galaxy.sds.thrift.TableQuota;
import com.xiaomi.infra.galaxy.sds.thrift.TableSchema;
import com.xiaomi.infra.galaxy.sds.thrift.TableService;
import com.xiaomi.infra.galaxy.sds.thrift.TableSpec;
import com.xiaomi.infra.galaxy.sds.thrift.UserType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class ScanWithActionExample {
  private static ClientFactory clientFactory;
  private static AdminService.Iface adminClient;
  private static TableService.Iface tableClient;
  private static String secretKeyId = ""; // Your AppKey
  private static String secretKey = ""; // Your AppSecret
  private static UserType userType = UserType.APP_SECRET;
  private static String endpoint = "http://cnbj-s0.sds.api.xiaomi.com";
  private static boolean isInit = false;
  private static String tableName = "java-test-weather1";
  private static int numCity = 26;
  private static String[] cities = { "Beihai", "Dalian", "Dandong", "Fuzhou", "Guangzhou",
      "Haikou", "Hankou", "Huangpu", "Jiujiang", "Lianyungang", "Nanjing", "Nantong", "Ningbo",
      "Qingdao", "Qinhuangdao", "Rizhao", "Sanya", "Shanghai", "Shantou", "Shenzhen",
      "Tianjin", "Weihai", "Wenzhou", "Xiamen", "Yangzhou", "Yantai" };

  private static void init() {
    Credential credential = new Credential().setSecretKey(secretKey).setSecretKeyId(secretKeyId)
        .setType(userType);
    clientFactory = new ClientFactory(credential);
    // socket timeout 10000 ms and connection timeout 3000
    adminClient = clientFactory
        .newAdminClient(endpoint + CommonConstants.ADMIN_SERVICE_PATH, 50000, 3000);
    // 5 retries at most
    tableClient = clientFactory
        .newTableClient(endpoint + CommonConstants.TABLE_SERVICE_PATH, 10000, 3000, true, 5);
    isInit = true;
  }

  private static TableSpec tableSpec() {
    List<KeySpec> primaryKey = Arrays
        .asList(new KeySpec[] { new KeySpec().setAttribute("cityId"), new KeySpec().setAttribute(
            "timestamp").setAsc(false) });
    Map<String, DataType> attributes = new HashMap<String, DataType>();
    attributes.put("cityId", DataType.STRING);
    attributes.put("timestamp", DataType.INT64);
    attributes.put("score", DataType.DOUBLE);
    attributes.put("pm25", DataType.INT64);
    TableSchema tableSchema = new TableSchema();
    tableSchema.setPrimaryIndex(primaryKey)
        .setAttributes(attributes)
        .setTtl(-1);

    TableMetadata tableMetadata = new TableMetadata();
    tableMetadata
        .setQuota(new TableQuota().setSize(100 * 1024 * 1024))
        .setThroughput(new ProvisionThroughput().setReadCapacity(200).setWriteCapacity(200));

    return new TableSpec().setSchema(tableSchema)
        .setMetadata(tableMetadata);
  }

  private static void printResult(Map<String, Datum> resultToPrint) {
    if (resultToPrint != null) {
      for (Map.Entry<String, Datum> e : resultToPrint.entrySet()) {
        System.out.print(
            String
                .format("[%s] => %s\t", e.getKey(), DatumUtil.fromDatum(e.getValue()).toString())
        );
      }
      System.out.println();
    }
  }

  private static void displayTable() throws Exception {
    ScanRequest scanRequest = new ScanRequest();
    scanRequest.setTableName(tableName);
    scanRequest.setLimit(numCity);
    TableScanner scanner = new TableScanner(tableClient, scanRequest);
    Iterator<Map<String, Datum>> iterator = scanner.iterator();
    while (iterator.hasNext()) {
      printResult(iterator.next());
    }
  }

  private static int scanWithAction(ScanRequest scanRequest) throws Exception {
    int count = 0;
    TableScanner scanner = new TableScanner(tableClient, scanRequest);
    Iterator<Map<String, Datum>> iterator = scanner.iterator();
    while (iterator.hasNext()) {
      count += (Integer) DatumUtil.fromDatum(iterator.next().get(CommonConstants.SCAN_COUNT));
    }
    return count;
  }

  public static void main(String[] args) throws Exception {
    init();
    TableSpec tableSpec = tableSpec();
    adminClient.createTable(tableName, tableSpec);
    try {
      Date now = new Date();
      PutRequest putRequest = new PutRequest();
      System.out.println("========== put data ==========");
      for (int i = 0; i < numCity; i++) {
        putRequest.clear();
        putRequest.setTableName(tableName);
        putRequest.putToRecord("cityId", DatumUtil.toDatum(cities[i]));
        putRequest.putToRecord("timestamp", DatumUtil.toDatum(now.getTime()));
        putRequest.putToRecord("score", DatumUtil.toDatum((double) new Random().nextInt(100)));
        putRequest.putToRecord("pm25", DatumUtil.toDatum((long) (new Random().nextInt(500))));
        tableClient.put(putRequest);
        System.out.println("put record #" + i);
      }
      System.out.println("========== records in table ==========");
      displayTable();
      ScanRequest scanRequest = new ScanRequest();
      scanRequest.setTableName(tableName);
      scanRequest.setLimit(numCity);
      int count;
      System.out.println("========== scan with action COUNT ==========");
      scanRequest.setAction(new ScanAction().setAction(ScanOp.COUNT));
      count = scanWithAction(scanRequest);
      System.out.println("There are totally " + count + " cities");

      Map<String, Datum> startKey = new HashMap<String, Datum>();
      Map<String, Datum> stopKey = new HashMap<String, Datum>();
      startKey.put("cityId", DatumUtil.toDatum("Guangzhou"));
      stopKey.put("cityId", DatumUtil.toDatum("Qingdao"));
      scanRequest.setStartKey(startKey);
      scanRequest.setStopKey(stopKey);
      count = scanWithAction(scanRequest);
      System.out.println("There are " + count + " cities between  Guangzhou and Qingdao");

      System.out.println("========== scan with action UPDATE==========");
      // set all cities' score 60.0
      Map<String, Datum> record = new HashMap<String, Datum>();
      record.put("score", DatumUtil.toDatum(60.0));
      scanRequest.setAction(new ScanAction().setAction(ScanOp.UPDATE).setRequest(
          Request.putRequest(new PutRequest().setRecord(record))));
      scanRequest.setStartKey(null);
      scanRequest.setStopKey(null);
      count = scanWithAction(scanRequest);
      System.out.println("There are " + count + " cities' score are set to 60.0, after update");
      displayTable();
      // set the score of cities between Dandong and Sanya  to 90.0
      record.put("score", DatumUtil.toDatum(90.0));
      scanRequest.setAction(new ScanAction().setAction(ScanOp.UPDATE).setRequest(
          Request.putRequest(new PutRequest().setRecord(record))));
      startKey.put("cityId", DatumUtil.toDatum("Dandong"));
      stopKey.put("cityId", DatumUtil.toDatum("Sanya"));
      scanRequest.setStartKey(startKey);
      scanRequest.setStopKey(stopKey);
      count = scanWithAction(scanRequest);
      System.out.println("There are " + count
          + " cities between Dandong and Sanya and their scores are set to 90.0, after update");
      displayTable();

      System.out.println("========== scan with action DELETE==========");
      // delete pm25 of all cities
      List<String> deletedAttribute = new ArrayList<String>();
      deletedAttribute.add("pm25");
      scanRequest.setAction(new ScanAction().setAction(ScanOp.DELETE).setRequest(
          Request.removeRequest(new RemoveRequest().setAttributes(deletedAttribute))));
      scanRequest.setStartKey(null);
      scanRequest.setStopKey(null);
      count = scanWithAction(scanRequest);
      System.out.println("There are " + count + " cities' pm25 are deleted, after delete");
      displayTable();
      // delete score between Ningbo and Tianjin
      deletedAttribute.clear();
      deletedAttribute.add("score");
      scanRequest.setAction(new ScanAction().setAction(ScanOp.DELETE).setRequest(
          Request.removeRequest(new RemoveRequest().setAttributes(deletedAttribute))));
      startKey.put("cityId", DatumUtil.toDatum("Ningbo"));
      stopKey.put("cityId", DatumUtil.toDatum("Tianjin"));
      scanRequest.setStartKey(startKey);
      scanRequest.setStopKey(stopKey);
      count = scanWithAction(scanRequest);
      System.out.println("There are " + count
          + " cities between Ningbo and Tianjin and their score are deleted, after delete");
      displayTable();

      // delete cities between Rizhao and Xiamen
      startKey.put("cityId", DatumUtil.toDatum("Rizhao"));
      stopKey.put("cityId", DatumUtil.toDatum("Xiamen"));
      scanRequest.setStartKey(startKey);
      scanRequest.setStopKey(stopKey);
      scanRequest.setAction(new ScanAction().setAction(ScanOp.DELETE));
      count = scanWithAction(scanRequest);
      System.out.println("There are " + count
          + " cities between Rizhao and Xiamen and they are deleted, after delete");
      displayTable();
    } finally {
      adminClient.disableTable(tableName);
      adminClient.dropTable(tableName);
    }
  }
}
