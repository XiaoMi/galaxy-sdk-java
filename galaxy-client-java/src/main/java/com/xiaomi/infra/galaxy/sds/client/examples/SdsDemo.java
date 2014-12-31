package com.xiaomi.infra.galaxy.sds.client.examples;

import com.xiaomi.infra.galaxy.sds.client.ClientFactory;
import com.xiaomi.infra.galaxy.sds.thrift.AdminService;
import com.xiaomi.infra.galaxy.sds.thrift.CommonConstants;
import com.xiaomi.infra.galaxy.sds.thrift.Credential;
import com.xiaomi.infra.galaxy.sds.thrift.DataType;
import com.xiaomi.infra.galaxy.sds.thrift.Datum;
import com.xiaomi.infra.galaxy.sds.thrift.DatumUtil;
import com.xiaomi.infra.galaxy.sds.thrift.EntityGroupSpec;
import com.xiaomi.infra.galaxy.sds.thrift.GetRequest;
import com.xiaomi.infra.galaxy.sds.thrift.GetResult;
import com.xiaomi.infra.galaxy.sds.thrift.KeySpec;
import com.xiaomi.infra.galaxy.sds.thrift.LocalSecondaryIndexSpec;
import com.xiaomi.infra.galaxy.sds.thrift.OperatorType;
import com.xiaomi.infra.galaxy.sds.thrift.ProvisionThroughput;
import com.xiaomi.infra.galaxy.sds.thrift.PutRequest;
import com.xiaomi.infra.galaxy.sds.thrift.PutResult;
import com.xiaomi.infra.galaxy.sds.thrift.ScanRequest;
import com.xiaomi.infra.galaxy.sds.thrift.ScanResult;
import com.xiaomi.infra.galaxy.sds.thrift.SecondaryIndexConsistencyMode;
import com.xiaomi.infra.galaxy.sds.thrift.SimpleCondition;
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
import java.util.Random;

public class SdsDemo {
  private static ClientFactory clientFactory;
  private static AdminService.Iface adminClient;
  private static TableService.Iface tableClient;
  private static String secretKeyId = ""; // Your AppKey
  private static String secretKey = ""; // Your AppSecret
  private static UserType userType = UserType.APP_SECRET;
  private static String endpoint = "http://cnbj-s0.sds.api.xiaomi.com";
  private static boolean isInit = false;
  private static String tableName = "java-test-note";
  private static String[] categories = { "work", "travel", "food" };

  private static void init() {
    Credential credential = new Credential().setSecretKey(secretKey).setSecretKeyId(secretKeyId)
        .setType(userType);
    clientFactory = new ClientFactory(credential);
    adminClient = clientFactory
        .newAdminClient(endpoint + CommonConstants.ADMIN_SERVICE_PATH, 50000, 3000);
    tableClient = clientFactory
        .newTableClient(endpoint + CommonConstants.TABLE_SERVICE_PATH, 10000, 3000, true, 3);
    isInit = true;
  }

  private static TableSpec tableSpec() {
    EntityGroupSpec entityGroupSpec = new EntityGroupSpec().setAttributes(Arrays.asList(
        new KeySpec[] { new KeySpec().setAttribute("userId").setAsc(false) })).setEnableHash(true);
    List<KeySpec> primaryKey = Arrays.asList(new KeySpec[] { new KeySpec().setAttribute(
        "noteId").setAsc(false) });
    Map<String, LocalSecondaryIndexSpec> secondaryIndexSpecMap = new HashMap<String, LocalSecondaryIndexSpec>();
    LocalSecondaryIndexSpec mtimeIndex = new LocalSecondaryIndexSpec();
    mtimeIndex.setIndexSchema(Arrays.asList(new KeySpec[] { new KeySpec().setAttribute(
        "mtime").setAsc(false) }));
    mtimeIndex.setProjections(Arrays.asList("title", "noteId"));
    mtimeIndex.setConsistencyMode(SecondaryIndexConsistencyMode.EAGER);
    secondaryIndexSpecMap.put("mtime", mtimeIndex);
    LocalSecondaryIndexSpec catIndex = new LocalSecondaryIndexSpec();
    catIndex
        .setIndexSchema(Arrays.asList(new KeySpec[] { new KeySpec().setAttribute("category") }));
    catIndex.setConsistencyMode(SecondaryIndexConsistencyMode.LAZY);
    secondaryIndexSpecMap.put("cat", catIndex);
    Map<String, DataType> attributes = new HashMap<String, DataType>();
    attributes.put("userId", DataType.STRING);
    attributes.put("noteId", DataType.INT64);
    attributes.put("title", DataType.STRING);
    attributes.put("content", DataType.STRING);
    attributes.put("version", DataType.INT64);
    attributes.put("mtime", DataType.INT64);
    attributes.put("category", DataType.STRING_SET);
    TableSchema tableSchema = new TableSchema();
    tableSchema.setEntityGroup(entityGroupSpec)
        .setPrimaryIndex(primaryKey)
        .setSecondaryIndexes(secondaryIndexSpecMap)
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
      System.out.println("================= insert and update notes ====================");
      for (int i = 0; i < 20; i++) {
        int version = 0;
        PutRequest putRequest = new PutRequest();
        putRequest.setTableName(tableName);
        putRequest.putToRecord("userId", DatumUtil.toDatum("user1"));
        putRequest.putToRecord("noteId", DatumUtil.toDatum((long) i));
        putRequest.putToRecord("title", DatumUtil.toDatum("Title " + i));
        putRequest.putToRecord("content", DatumUtil.toDatum("note " + i));
        putRequest.putToRecord("version", DatumUtil.toDatum((long) version));
        putRequest.putToRecord("mtime", DatumUtil.toDatum((long) (i * i % 10)));
        putRequest.putToRecord("category", DatumUtil.toDatum(Arrays
                .asList(categories[i % categories.length], categories[(i + 1) % categories.length]),
            DataType.STRING
        ));
        tableClient.put(putRequest);
        PutRequest newPutRequest = putRequest;
        newPutRequest.putToRecord("version", DatumUtil.toDatum((long) (version + 1)));
        newPutRequest.putToRecord("content", DatumUtil.toDatum("new content " + i));
        newPutRequest.putToRecord("mtime", DatumUtil.toDatum((long) (i * i % 10 + 1)));
        SimpleCondition simpleCondition = new SimpleCondition();
        simpleCondition.setField("version");
        simpleCondition.setValue(DatumUtil.toDatum((long) version));
        simpleCondition.setOperator(OperatorType.EQUAL);
        newPutRequest.setCondition(simpleCondition);
        PutResult putResult = tableClient.put(putRequest);
        if (putResult.isSuccess()) {
          System.out.println("update note without conflict");
        }
      }

      // random access
      System.out.println("================= get note by id ====================");
      GetRequest getRequest = new GetRequest();
      getRequest.setTableName(tableName);
      getRequest.putToKeys("userId", DatumUtil.toDatum("user1"));
      getRequest.putToKeys("noteId", DatumUtil.toDatum((long) (new Random().nextInt(10))));
      GetResult getResult = tableClient.get(getRequest);
      printResult(getResult.getItem());

      // get noteId which contain category food
      System.out
          .println("================= get notes which contain category food ====================");
      ScanRequest scanRequest = new ScanRequest();
      scanRequest.setTableName(tableName);
      scanRequest.setIndexName("cat");
      Map<String, Datum> startKey = new HashMap<String, Datum>();
      startKey.put("userId", DatumUtil.toDatum("user1"));
      startKey.put("category", DatumUtil.toDatum("food"));
      Map<String, Datum> stopKey = new HashMap<String, Datum>();
      stopKey.put("userId", DatumUtil.toDatum("user1"));
      stopKey.put("category", DatumUtil.toDatum("food"));
      scanRequest.setStartKey(startKey);
      scanRequest.setStopKey(stopKey);
      scanRequest.setAttributes(Arrays.asList("noteId", "category"));
      scanRequest.setLimit(100);
      ScanResult scanResult = tableClient.scan(scanRequest);
      List<Map<String, Datum>> kvsList = scanResult.getRecords();
      for (Map<String, Datum> kvs : kvsList) {
        printResult(kvs);
      }

      //scan by last modify time
      System.out.println("================= scan by last modify time ====================");
      scanRequest.clear();
      scanRequest.setTableName(tableName);
      scanRequest.setIndexName("mtime");
      startKey.clear();
      startKey.put("userId", DatumUtil.toDatum("user1"));
      stopKey.clear();
      stopKey.put("userId", DatumUtil.toDatum("user1"));
      scanRequest.setStartKey(startKey);
      scanRequest.setStopKey(stopKey);
      scanRequest.setCondition("title REGEXP '.*[0-5]' AND noteId > 5");
      scanRequest.setAttributes(Arrays.asList("noteId", "title", "mtime"));
      scanRequest.setLimit(100);
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
