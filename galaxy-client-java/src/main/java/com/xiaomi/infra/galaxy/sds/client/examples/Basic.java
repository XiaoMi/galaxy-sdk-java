package com.xiaomi.infra.galaxy.sds.client.examples;

import com.xiaomi.infra.galaxy.sds.client.ClientFactory;
import com.xiaomi.infra.galaxy.sds.client.TableScanner;
import com.xiaomi.infra.galaxy.sds.thrift.AdminService;
import com.xiaomi.infra.galaxy.sds.thrift.CommonConstants;
import com.xiaomi.infra.galaxy.sds.thrift.Credential;
import com.xiaomi.infra.galaxy.sds.thrift.DataType;
import com.xiaomi.infra.galaxy.sds.thrift.Datum;
import com.xiaomi.infra.galaxy.sds.thrift.DatumUtil;
import com.xiaomi.infra.galaxy.sds.thrift.GetRequest;
import com.xiaomi.infra.galaxy.sds.thrift.GetResult;
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
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class Basic {
  private static ClientFactory clientFactory;
  private static AdminService.Iface adminClient;
  private static TableService.Iface tableClient;
  private static String secretKeyId = ""; // Your AppId
  private static String secretKey = ""; // Your AppSecret
  private static UserType userType = UserType.APP_SECRET;
  private static String endpoint = "http://sds.api.xiaomi.com";
  private static boolean isInit = false;
  private static String tableName = "java-test-weather";
  private static String[] cities = { "北京", "Beihai", "Dalian", "Dandong", "Fuzhou", "Guangzhou",
      "Haikou", "Hankou", "Huangpu", "Jiujiang", "Lianyungang", "Nanjing", "Nantong", "Ningbo",
      "Qingdao", "Qinhuangdao", "Rizhao", "Sanya", "Shanghai", "Shantou", "Shenzhen",
      "Tianjin", "Weihai", "Wenzhou", "Xiamen", "Yangzhou", "Yantai" };

  private static void init() {
    Credential credential = new Credential().setSecretKey(secretKey).setSecretKeyId(secretKeyId)
        .setType(userType);
    clientFactory = new ClientFactory(credential);
    // socket timeout 10000 ms and connection timeout 3000
    adminClient = clientFactory.newAdminClient(endpoint + CommonConstants.ADMIN_SERVICE_PATH, 10000, 3000);
    // 5 retries at most
    tableClient = clientFactory.newTableClient(endpoint + CommonConstants.TABLE_SERVICE_PATH, true, 5);
    isInit = true;
  }

  private static TableSpec tableSpec() {
    List<KeySpec> primaryKey = Arrays
        .asList(new KeySpec[] { new KeySpec("cityId"), new KeySpec("timestamp").setAsc(false) });
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
        .setQuota(new TableQuota(100 * 1024 * 1024))
        .setThroughput(new ProvisionThroughput(20, 20));

    return new TableSpec().setSchema(tableSchema)
        .setMetadata(tableMetadata);
  }

  private static void printResult(Map<String, Datum> resultToPrint) {
    if (resultToPrint != null) {
      for (Map.Entry<String, Datum> e : resultToPrint.entrySet()) {
        System.out.println(
            String.format("[%s] => %s", e.getKey(), DatumUtil.fromDatum(e.getValue()).toString()));
      }
    }
  }

  public static void main(String[] args) throws Exception {
    init();
    TableSpec tableSpec = tableSpec();
    adminClient.createTable(tableName, tableSpec);
    try {
      System.out.println("table " + tableName + ":");
      System.out.println(adminClient.describeTable(tableName));
      System.out.println("All tables belong to you:");
      System.out.println(adminClient.findAllTables());
      // put data
      Date now = new Date();
      PutRequest putRequest = new PutRequest();
      for (int i = 0; i < 10; i++) {
        putRequest.clear();
        putRequest.setTableName(tableName);
        putRequest.putToRecord("cityId", DatumUtil.toDatum(cities[i]));
        putRequest.putToRecord("timestamp", DatumUtil.toDatum(now.getTime()));
        putRequest.putToRecord("score", DatumUtil.toDatum((double) new Random().nextInt(100)));
        putRequest.putToRecord("pm25", DatumUtil.toDatum((long) (new Random().nextInt(500))));
        tableClient.put(putRequest);
        System.out.println("put record #" + i);
      }
      // get data
      GetRequest getRequest = new GetRequest();
      getRequest.setTableName(tableName);
      getRequest.putToKeys("cityId", DatumUtil.toDatum(cities[0]));
      getRequest.putToKeys("timestamp", DatumUtil.toDatum(now.getTime()));
      getRequest.addToAttributes("pm25");
      GetResult getResult = tableClient.get(getRequest);
      printResult(getResult.getItem());

      // scan data
      ScanRequest scanRequest = new ScanRequest();
      scanRequest.setTableName(tableName);
      scanRequest.addToAttributes("cityId");
      scanRequest.addToAttributes("score");
      scanRequest.setCondition("score > 50");
      scanRequest.setLimit(10);
      ScanResult scanResult = tableClient.scan(scanRequest);
      List<Map<String, Datum>> kvsList = scanResult.getRecords();
      for (Map<String, Datum> kvs : kvsList) {
        printResult(kvs);
      }

      // scan the whole table with retry
      scanRequest.clear();
      scanRequest.setTableName(tableName);
      TableScanner scanner = new TableScanner(tableClient, scanRequest);
      Iterator<Map<String, Datum>> iterator = scanner.iterator();
      while (iterator.hasNext()) {
        printResult(iterator.next());
      }
    } finally {
      adminClient.disableTable(tableName);
      adminClient.dropTable(tableName);
    }
  }
}
