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
import com.xiaomi.infra.galaxy.sds.thrift.Permission;
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

public class AtomicBatch {
  private static AdminService.Iface adminClient;
  private static TableService.Iface tableClient;
  private static String accountKey = ""; // Your AccountKey
  private static String accountSecret = ""; // Your AccountSecret
  private static String endpoint = "http://awsbj0.sds.api.xiaomi.com";
  private static String teamId = "";  //start with CI
  private static boolean isInit = false;
  private static String tableName1 = "java-test-atomicbatch";
  private static String tableName2 = "java-test-atomicbatch-s0";
  private static String[] cities = { "北京", "Beihai", "Dalian", "Dandong", "Fuzhou", "Guangzhou",
      "Haikou", "Hankou", "Huangpu", "Jiujiang", "Lianyungang", "Nanjing", "Nantong", "Ningbo",
      "Qingdao", "Qinhuangdao", "Rizhao", "Sanya", "Shanghai", "Shantou", "Shenzhen", "Tianjin",
      "Weihai", "Wenzhou", "Xiamen", "Yangzhou", "Yantai" };

  private static Credential getCredential(String secretKeyId, String secretKey, UserType userType) {
    return new Credential().setSecretKeyId(secretKeyId).setSecretKey(secretKey).setType(userType);
  }

  public static AdminService.Iface createAdminClient(String host) {
    Credential credential = getCredential(accountKey, accountSecret, UserType.DEV_XIAOMI);
    ClientFactory clientFactory = new ClientFactory().setCredential(credential);
    return clientFactory.newAdminClient(host + CommonConstants.ADMIN_SERVICE_PATH, 50000, 3000);
  }

  public static TableService.Iface createTableClient(String host) {
    Credential credential = getCredential(accountKey, accountSecret, UserType.DEV_XIAOMI);
    // based on JSON transport protocol
    // clientFactory = new ClientFactory().setCredential(credential).setProtocol(ThriftProtocol.TJSON);

    // based on Compact Binary transport protocol
    // clientFactory = new ClientFactory().setCredential(credential).setProtocol(ThriftProtocol.TCOMPACT);

    // based on default Binary transport protocol
    ClientFactory clientFactory = new ClientFactory().setCredential(credential);
    return clientFactory
        .newTableClient(host + CommonConstants.TABLE_SERVICE_PATH, 10000, 3000, true, 5);
  }

  public static Map<String, List<Permission>> cannedAclGrant(String acountkey,
      Permission... permissions) {
    Map<String, List<Permission>> aclGrant = new HashMap<String, List<Permission>>();
    aclGrant.put(acountkey, Arrays.asList(permissions));
    return aclGrant;
  }

  private static void init() {
    adminClient = createAdminClient(endpoint);
    tableClient = createTableClient(endpoint);
    isInit = true;
  }

  private static TableSpec tableSpec() {
    List<KeySpec> primaryKey = Arrays.asList(new KeySpec[] { new KeySpec().setAttribute("cityId"),
        new KeySpec().setAttribute("timestamp").setAsc(false) });
    Map<String, DataType> attributes = new HashMap<String, DataType>();
    attributes.put("cityId", DataType.STRING);
    attributes.put("timestamp", DataType.INT64);
    attributes.put("score", DataType.DOUBLE);
    attributes.put("pm25", DataType.INT64);
    TableSchema tableSchema = new TableSchema();
    tableSchema.setPrimaryIndex(primaryKey).setAttributes(attributes).setTtl(-1)
        //enable transaction for atomicBatch
        .setEnableTransaction(true);

    TableMetadata tableMetadata = new TableMetadata();
    tableMetadata.setAcl(
        cannedAclGrant(teamId, Permission.READ, Permission.WRITE, Permission.ADMIN,
            Permission.QUOTA_ADMIN, Permission.DESCRIBE_ADMIN))
        .setQuota(new TableQuota().setSize(100 * 1024 * 1024))
        .setThroughput(new ProvisionThroughput().setReadCapacity(500).setWriteCapacity(500));

    return new TableSpec().setSchema(tableSchema).setMetadata(tableMetadata);
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
    adminClient.createTable(tableName1, tableSpec);
    adminClient.createTable(tableName2, tableSpec);
    try {
      System.out.println("table " + tableName1 + ":");
      System.out.println(adminClient.describeTable(tableName1));
      System.out.println("table " + tableName2 + ":");
      System.out.println(adminClient.describeTable(tableName2));
      System.out.println("All tables belong to you:");
      System.out.println(adminClient.findAllTables());
      Date now = new Date();

      List<BatchRequestItem> puts1 = new ArrayList<BatchRequestItem>();
      for (int i = 0; i < 10; i++) {
        BatchRequestItem item = new BatchRequestItem().setAction(BatchOp.PUT);
        Map<String, Datum> record = new HashMap<String, Datum>();
        record.put("cityId", DatumUtil.toDatum(cities[i]));
        record.put("timestamp", DatumUtil.toDatum(now.getTime()));
        record.put("score", DatumUtil.toDatum((double) new Random().nextInt(100)));
        record.put("pm25", DatumUtil.toDatum((long) (new Random().nextInt(500))));
        item.setRequest(
            Request.putRequest(new PutRequest().setTableName(tableName1).setRecord(record)));
        puts1.add(item);
      }

      List<BatchRequestItem> puts2 = new ArrayList<BatchRequestItem>();
      for (int i = 0; i < 10; i++) {
        BatchRequestItem item = new BatchRequestItem().setAction(BatchOp.PUT);
        Map<String, Datum> record = new HashMap<String, Datum>();
        record.put("cityId", DatumUtil.toDatum(cities[i]));
        record.put("timestamp", DatumUtil.toDatum(now.getTime()));
        record.put("score", DatumUtil.toDatum((double) new Random().nextInt(100)));
        record.put("pm25", DatumUtil.toDatum((long) (new Random().nextInt(500))));
        item.setRequest(
            Request.putRequest(new PutRequest().setTableName(tableName2).setRecord(record)));
        puts2.add(item);
      }

      List<BatchRequestItem> puts = new ArrayList<BatchRequestItem>();
      puts.addAll(puts1);
      puts.addAll(puts2);

      tableClient.atomicBatch(new BatchRequest().setItems(puts));

      // scan data
      ScanRequest scanRequest = new ScanRequest();
      scanRequest.setTableName(tableName1);
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
      scanRequest.setTableName(tableName2);
      TableScanner scanner = new TableScanner(tableClient, scanRequest);
      Iterator<Map<String, Datum>> iterator = scanner.iterator();
      while (iterator.hasNext()) {
        printResult(iterator.next());
      }
    } finally {
      adminClient.disableTable(tableName1);
      adminClient.dropTable(tableName1);
      adminClient.disableTable(tableName2);
      adminClient.dropTable(tableName2);
    }
  }
}
