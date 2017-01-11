package com.xiaomi.infra.galaxy.sds.examples;

import com.xiaomi.infra.galaxy.sds.client.ClientFactory;
import com.xiaomi.infra.galaxy.sds.client.TableScanner;
import com.xiaomi.infra.galaxy.sds.thrift.AdminService;
import com.xiaomi.infra.galaxy.sds.thrift.BatchOp;
import com.xiaomi.infra.galaxy.sds.thrift.BatchRequest;
import com.xiaomi.infra.galaxy.sds.thrift.BatchRequestItem;
import com.xiaomi.infra.galaxy.sds.thrift.BatchResult;
import com.xiaomi.infra.galaxy.sds.thrift.CannedAcl;
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
import com.xiaomi.infra.galaxy.sds.thrift.Request;
import com.xiaomi.infra.galaxy.sds.thrift.ScanRequest;
import com.xiaomi.infra.galaxy.sds.thrift.ScanResult;
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

public class Basic {
  private static AdminService.Iface adminClient;
  private static TableService.Iface tableClient;
  private static String accountKey = ""; // Your AccountKey
  private static String accountSecret = ""; // Your AccountSecret
  private static String endpoint = "http://cnbj-s0.sds.api.xiaomi.com";
  private static boolean isInit = false;
  private static String tableName = "java-test-weather";
  private static String[] cities = { "北京", "Beihai", "Dalian", "Dandong", "Fuzhou", "Guangzhou",
      "Haikou", "Hankou", "Huangpu", "Jiujiang", "Lianyungang", "Nanjing", "Nantong", "Ningbo",
      "Qingdao", "Qinhuangdao", "Rizhao", "Sanya", "Shanghai", "Shantou", "Shenzhen",
      "Tianjin", "Weihai", "Wenzhou", "Xiamen", "Yangzhou", "Yantai" };

  private static Credential getCredential(String secretKeyId, String secretKey, UserType userType) {
    return new Credential().setSecretKeyId(secretKeyId).setSecretKey(secretKey)
        .setType(userType);
  }

  public static AdminService.Iface createAdminClient(String host) {
    Credential credential = getCredential(accountKey, accountSecret, UserType.DEV_XIAOMI);
    ClientFactory clientFactory = new ClientFactory().setCredential(credential);
    return clientFactory.newAdminClient(host + CommonConstants.ADMIN_SERVICE_PATH,
        50000, 3000);
  }

  public static TableService.Iface createTableClient(String host) {
    Credential credential = getCredential(accountKey, accountSecret, UserType.APP_SECRET);
    // based on JSON transport protocol
    // clientFactory = new ClientFactory().setCredential(credential).setProtocol(ThriftProtocol.TJSON);

    // based on Compact Binary transport protocol
    // clientFactory = new ClientFactory().setCredential(credential).setProtocol(ThriftProtocol.TCOMPACT);

    // based on default Binary transport protocol
    ClientFactory clientFactory = new ClientFactory().setCredential(credential);
    return clientFactory.newTableClient(host + CommonConstants.TABLE_SERVICE_PATH,
        10000, 3000, true, 5);
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
        .setThroughput(new ProvisionThroughput().setReadCapacity(20).setWriteCapacity(20));

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

      // batch put by partialAllowedBatch
      List<BatchRequestItem> batch = new ArrayList<BatchRequestItem>();
      for (int i = 11; i < 20; i++) {
        BatchRequestItem item = new BatchRequestItem().setAction(BatchOp.PUT);
        Map<String, Datum> record = new HashMap<String, Datum>();
        record.put("cityId", DatumUtil.toDatum(cities[i]));
        putRequest.putToRecord("timestamp", DatumUtil.toDatum(now.getTime()));
        putRequest.putToRecord("score", DatumUtil.toDatum((double) new Random().nextInt(100)));
        putRequest.putToRecord("pm25", DatumUtil.toDatum((long) (new Random().nextInt(500))));
        item.setRequest(Request.putRequest(new PutRequest().setTableName(tableName).setRecord(record)));
        batch.add(item);
      }

      while (true) {
        BatchResult br = tableClient.partialAllowedBatch(new BatchRequest().setItems(batch));
        List<BatchRequestItem> request = new ArrayList<BatchRequestItem>();
        for (int i = 0; i < br.getItems().size(); i++) {
          //At current only quota exceeded will get  isSuccess() == false
          if (!br.getItems().get(i).isSuccess()) {
            request.add(batch.get(i));
          }
        }
        if (request.isEmpty()) {
          break;
        }
        Thread.sleep(100); // wait a while to retry.
        batch = request;
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
