package com.xiaomi.infra.galaxy.sds.examples;

import static com.xiaomi.infra.galaxy.sds.thrift.ErrorCode.RESOURCE_NOT_FOUND;
import com.xiaomi.infra.galaxy.sds.client.ClientFactory;
import com.xiaomi.infra.galaxy.sds.thrift.AdminService;
import com.xiaomi.infra.galaxy.sds.thrift.BatchOp;
import com.xiaomi.infra.galaxy.sds.thrift.BatchRequest;
import com.xiaomi.infra.galaxy.sds.thrift.BatchRequestItem;
import com.xiaomi.infra.galaxy.sds.thrift.CannedAcl;
import com.xiaomi.infra.galaxy.sds.thrift.CommonConstants;
import com.xiaomi.infra.galaxy.sds.thrift.Credential;
import com.xiaomi.infra.galaxy.sds.thrift.DataType;
import com.xiaomi.infra.galaxy.sds.thrift.Datum;
import com.xiaomi.infra.galaxy.sds.thrift.DatumUtil;
import com.xiaomi.infra.galaxy.sds.thrift.GetRequest;
import com.xiaomi.infra.galaxy.sds.thrift.GetResult;
import com.xiaomi.infra.galaxy.sds.thrift.IncrementRequest;
import com.xiaomi.infra.galaxy.sds.thrift.IncrementResult;
import com.xiaomi.infra.galaxy.sds.thrift.KeySpec;
import com.xiaomi.infra.galaxy.sds.thrift.ProvisionThroughput;
import com.xiaomi.infra.galaxy.sds.thrift.PutRequest;
import com.xiaomi.infra.galaxy.sds.thrift.RemoveRequest;
import com.xiaomi.infra.galaxy.sds.thrift.RemoveResult;
import com.xiaomi.infra.galaxy.sds.thrift.Request;
import com.xiaomi.infra.galaxy.sds.thrift.ScanRequest;
import com.xiaomi.infra.galaxy.sds.thrift.ScanResult;
import com.xiaomi.infra.galaxy.sds.thrift.ServiceException;
import com.xiaomi.infra.galaxy.sds.thrift.TableInfo;
import com.xiaomi.infra.galaxy.sds.thrift.TableMetadata;
import com.xiaomi.infra.galaxy.sds.thrift.TableQuota;
import com.xiaomi.infra.galaxy.sds.thrift.TableSchema;
import com.xiaomi.infra.galaxy.sds.thrift.TableService;
import com.xiaomi.infra.galaxy.sds.thrift.TableSpec;
import com.xiaomi.infra.galaxy.sds.thrift.TableState;
import com.xiaomi.infra.galaxy.sds.thrift.UserType;
import libthrift091.TException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class LocalBasicExample {
  // In order to run this example, follow the instrucations below
  // 1.you should visit https://cloud-platform.d.xiaomi.net
  // 2.aquire your personal AccountKey and AccountSecret of SDS of 北京3武清测试
  // 3.complete code of String accountKey and accountSecret below
  private static AdminService.Iface adminClient;
  private static TableService.Iface tableClient;
  private static String accountKey = ""; // Your AccountKey
  private static String accountSecret = ""; // Your AccountSecret
  private static String endpoint = "https://staging-cnbj2-fusion-sds.api.xiaomi.net";
  private static String tableName = "sds-local-example";
  private static String[] cities = { "北京", "Beihai", "Dalian", "Dandong", "Fuzhou", "Guangzhou",
      "Haikou", "Sanya", "Jiujiang", "Lianyungang", "Nanjing", "Nantong", "Ningbo", "Qingdao",
      "Qinhuangdao", "Rizhao", "Shanghai", "Shantou", "Shenzhen", "Tianjin", "Weihai", "Wenzhou",
      "Xiamen", "Yangzhou", "Yantai" };
  private static String[] province = { "Beijing", "Guangxi", "Liaoning", "Liaoning", "FuJian",
      "Guangdong", "Hainan", "Hainan", "Jiangxi", "Jiangsu", "Jiangsu", "Jiangsu", "Zhejiang",
      "Shandong", "Hebei", "Shandong", "Shanghai", "Guangdong", "Guangdong", "Tianjin", "Shandong",
      "Zhejiang", "Fujian", "Jiangsu", "Shandong" };

  private static Credential getCredential(String secretKeyId, String secretKey, UserType userType) {
    return new Credential().setSecretKeyId(secretKeyId).setSecretKey(secretKey).setType(userType);
  }

  public static AdminService.Iface createAdminClient(String host) {
    Credential credential = getCredential(accountKey, accountSecret, UserType.DEV_XIAOMI);
    ClientFactory clientFactory = new ClientFactory().setCredential(credential);
    return clientFactory.newAdminClient(host + CommonConstants.ADMIN_SERVICE_PATH, 50000, 3000);
  }

  public static TableService.Iface createTableClient(String host) {
    Credential credential = getCredential(accountKey, accountSecret, UserType.APP_SECRET);
    ClientFactory clientFactory = new ClientFactory().setCredential(credential);
    return clientFactory.newTableClient(host + CommonConstants.TABLE_SERVICE_PATH, 10000, 3000,
      true, 5);
  }

  public static Map<String, List<CannedAcl>> cannedAclGrant(String appId, CannedAcl... cannedAcls) {
    Map<String, List<CannedAcl>> appGrant = new HashMap<String, List<CannedAcl>>();
    appGrant.put(appId, Arrays.asList(cannedAcls));
    return appGrant;
  }

  private static void init() {
    adminClient = createAdminClient(endpoint);
    tableClient = createTableClient(endpoint);
  }

  // To create a SDS table ,you should give tablename in string and TableSpec
  private static void createTable(String tableName) throws TException {
    if (isTableExist(tableName)) {
      System.out.println("table " + tableName + " exists,drop table");
      dropTable(tableName);
    }
    TableSpec spec = createTableSpec();
    adminClient.createTable(tableName, spec);
    System.out.println(String.format("created table %s", tableName));
    displayTableSpec(spec);
  }

  private static void displayTableSpec(TableSpec spec) {
    System.out.println("primaryIndex:" + spec.getSchema().getPrimaryIndex());
    System.out.println("attributes:" + spec.getSchema().getAttributes());
    System.out.println("quota:" + spec.getMetadata().getQuota());
    System.out.println("throughput:" + spec.getMetadata().getThroughput());
  }

  private static boolean isTableExist(String tableName) throws TException {
    try {
      adminClient.describeTable(tableName);
    } catch (ServiceException e) {

      if (e.errorCode == RESOURCE_NOT_FOUND) return false;
      else throw e;
    }
    System.out.println("table " + tableName + " exists");
    return true;
  }

  private static TableSpec createTableSpec() {
    // primaryKey was defined as a list of one or more keys
    // defined cityId and province as primaryKey index
    List<KeySpec> primaryKey = Arrays.asList(new KeySpec[] { new KeySpec().setAttribute("cityId"),
        new KeySpec().setAttribute("province") });

    // build a map to defined attributes
    Map<String, DataType> attributes = new HashMap<String, DataType>();
    attributes.put("cityId", DataType.STRING);
    attributes.put("province", DataType.STRING);
    attributes.put("timestamp", DataType.INT64);
    attributes.put("score", DataType.DOUBLE);
    attributes.put("pm25", DataType.INT64);

    // put primaryKey and attributes to defined a table
    TableSchema tableSchema = new TableSchema();
    tableSchema.setPrimaryIndex(primaryKey).setAttributes(attributes);

    // set table quota
    TableMetadata tableMetadata = new TableMetadata();
    tableMetadata.setQuota(new TableQuota().setSize(100 * 1024 * 1024))// the unit size of quota is
                                                                       // bytes
        .setThroughput(new ProvisionThroughput().setReadCapacity(80).setWriteCapacity(80));

    // build tableSpec then return
    return new TableSpec().setSchema(tableSchema).setMetadata(tableMetadata);
  }

  private static void alterTable(String tableName) throws TException {
    TableSpec spec = adminClient.describeTable(tableName);
    TableMetadata metadata = spec.getMetadata();
    // alter table quota
    metadata.setThroughput(new ProvisionThroughput().setReadCapacity(20).setWriteCapacity(20));
    spec.setMetadata(metadata);
    adminClient.alterTable(tableName, spec);
    System.out.println(String.format("altered table %s", tableName));
    displayTableSpec(spec);
  }

  // Warning!!!! Do not use this method unless you clear the consequnse
  private static void cloneTable(String tableName, String cloneTableName) throws TException {
    System.out.println(String.format("clone %s to %s", tableName, cloneTableName));
    if (isTableExist(cloneTableName)) {
      dropTable(cloneTableName);
    }
    adminClient.cloneTable(tableName, cloneTableName, true);
    TableSpec spec = adminClient.describeTable(cloneTableName);
    System.out.println(String.format("spec of new clone table:%s", spec));
  }

  // Warning!!!! Do not use this method unless you clear the consequnse
  private static void disableTable(String tableName) throws TException {
    System.out.println("disabling table %s" + tableName);

    TableState state = adminClient.getTableState(tableName);
    if (TableState.ENABLED == state) {
      adminClient.disableTable(tableName);
      System.out.println("success disable " + tableName);
    } else {
      System.out.println(tableName + "is in the state :" + state + "d isabling failed!");
    }
  }

  // Warning!!!! Do not use this method unless you clear the consequnse
  private static void enableTable(String tableName) throws TException {
    System.out.println("enabling table %s" + tableName);
    TableState state = adminClient.getTableState(tableName);
    if (TableState.DISABLED == state) {
      adminClient.enableTable(tableName);
      System.out.println("success enable " + tableName);
    } else {
      System.out.println(tableName + "is in the state :" + state + " enabling failed!");
    }
  }

  // Warning!!!! Do not use this method unless you clear the consequnse
  private static void dropTable(String tableName) throws TException {

    adminClient.disableTable(tableName);
    adminClient.dropTable(tableName);
    System.out.println("droped table " + tableName);
  }

  private static void listAllTables() throws TException {
    System.out.println("list All Tables!!!");
    List<TableInfo> tableInfoList = adminClient.findAllTables();
    for (TableInfo tb : tableInfoList) {
      System.out.println(tb.toString());
    }
  }

  private static void putExample() throws TException {
    Date now = new Date();
    PutRequest putRequest = new PutRequest();
    for (int i = 0; i < 10; i++) {
      putRequest.clear();
      putRequest.setTableName(tableName);
      putRequest.putToRecord("cityId", DatumUtil.toDatum(cities[i]));
      putRequest.putToRecord("province", DatumUtil.toDatum(province[i]));
      putRequest.putToRecord("timestamp", DatumUtil.toDatum(now.getTime()));
      putRequest.putToRecord("score", DatumUtil.toDatum((double) new Random().nextInt(100)));
      putRequest.putToRecord("pm25", DatumUtil.toDatum((long) (new Random().nextInt(500))));
      tableClient.put(putRequest);
      System.out.println("put record #" + i + ": " + putRequest.toString());
    }

  }

  private static void getExample() throws TException {
    // get data
    GetRequest getRequest = new GetRequest();
    getRequest.setTableName(tableName);
    getRequest.putToKeys("cityId", DatumUtil.toDatum(cities[0]));
    getRequest.putToKeys("province", DatumUtil.toDatum(province[0]));
    getRequest.addToAttributes("pm25");
    getRequest.addToAttributes("score");
    GetResult getResult = tableClient.get(getRequest);
    System.out.println(String.format("get record %s+%s(cityId+province)", cities[0], province[0]));
    Map<String, Datum> item = getResult.getItem();
    System.out.println(String.format("pm25:" + item.get("pm25")));
    System.out.println(String.format("score:" + item.get("score")));
    // System.out.println(String.format("timestamp:"+item.get("timestamp")));

  }

  private static void removeExample() throws TException {
    System.out.println(String.format("remove record %s+%s", cities[1], province[1]));
    RemoveRequest rq = new RemoveRequest();
    rq.setTableName(tableName);
    rq.putToKeys("cityId", DatumUtil.toDatum(cities[1]));
    rq.putToKeys("province", DatumUtil.toDatum(province[1]));
    RemoveResult ret = tableClient.remove(rq);
  }

  private static void incrementExample() throws TException {
    // check the value before increment
    GetRequest getRequest = new GetRequest();
    getRequest.setTableName(tableName);
    getRequest.putToKeys("cityId", DatumUtil.toDatum(cities[0]));
    getRequest.putToKeys("province", DatumUtil.toDatum(province[0]));
    GetResult ret = tableClient.get(getRequest);
    Map<String, Datum> item = ret.getItem();
    System.out.println("pm25 before increment: " + item.get("pm25"));

    // do increment
    IncrementRequest rq = new IncrementRequest();
    rq.setTableName(tableName);
    rq.putToKeys("cityId", DatumUtil.toDatum(cities[0]));
    rq.putToKeys("province", DatumUtil.toDatum(province[0]));
    // increse 2
    rq.putToAmounts("pm25", DatumUtil.toDatum(2));
    IncrementResult increRet = tableClient.increment(rq);
    System.out.println("pm25 after increse 2 : " + increRet.getAmounts().get("pm25"));
  }

  private static void batchExample() throws TException {
    System.out.println("batching...");
    Date now = new Date();
    BatchRequest batchRequest = new BatchRequest();
    List<BatchRequestItem> batch = new ArrayList<BatchRequestItem>();
    for (int i = 11; i < 20; i++) {
      BatchRequestItem item = new BatchRequestItem().setAction(BatchOp.PUT);
      Map<String, Datum> record = new HashMap<String, Datum>();
      record.put("cityId", DatumUtil.toDatum(cities[i]));
      record.put("province", DatumUtil.toDatum(province[i]));
      record.put("timestamp", DatumUtil.toDatum(now.getTime()));
      record.put("score", DatumUtil.toDatum((double) new Random().nextInt(100)));
      record.put("pm25", DatumUtil.toDatum((long) (new Random().nextInt(500))));
      item.setRequest(
        Request.putRequest(new PutRequest().setTableName(tableName).setRecord(record)));
      batchRequest.addToItems(item);
      System.out.println("record#" + (i - 10) + record.toString());
    }
    tableClient.batch(batchRequest);
    System.out.println("batch done!");
  }

  private static void scanExample() throws TException {
    // scan data
    ScanRequest scanRequest = new ScanRequest();
    scanRequest.setTableName(tableName);
    scanRequest.addToAttributes("cityId");
    scanRequest.addToAttributes("province");
    scanRequest.addToAttributes("score");
    scanRequest.addToAttributes("pm25");
    scanRequest.addToAttributes("timestamp");
    // scanRequest.setCondition("score > 50");
    scanRequest.setLimit(10);
    ScanResult scanResult = tableClient.scan(scanRequest);
    List<Map<String, Datum>> kvsList = scanResult.getRecords();
    for (Map<String, Datum> kvs : kvsList) {
      printResult(kvs);
    }
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
    System.out.println("==========init=========");
    init();

    System.out.println("==========create example=========");
    createTable(tableName);

    System.out.println("==========alter table example=========");
    alterTable(tableName);

    System.out.println("==========disable table example=========");
    disableTable(tableName);

    System.out.println("==========list table example=========");
    listAllTables();

    System.out.println("==========enable table example=========");
    enableTable(tableName);

    System.out.println("==========put example=========");
    putExample();

    System.out.println("==========batch example=========");
    batchExample();

    System.out.println("==========get example=========");
    getExample();

    System.out.println("==========increment example=========");
    incrementExample();

    System.out.println("==========scan example=========");
    scanExample();

    System.out.println("==========remove example=========");
    removeExample();

    System.out.println("==========drop table example=========");
    dropTable(tableName);

  }

}
