package com.xiaomi.infra.galaxy.sds.examples;

import com.xiaomi.infra.galaxy.sds.client.ClientFactory;
import com.xiaomi.infra.galaxy.sds.client.TableScanner;
import com.xiaomi.infra.galaxy.sds.thrift.AdminService;
import com.xiaomi.infra.galaxy.sds.thrift.BatchOp;
import com.xiaomi.infra.galaxy.sds.thrift.BatchRequest;
import com.xiaomi.infra.galaxy.sds.thrift.BatchRequestItem;
import com.xiaomi.infra.galaxy.sds.thrift.BatchResult;
import com.xiaomi.infra.galaxy.sds.thrift.CannedAcl;
import com.xiaomi.infra.galaxy.sds.thrift.ColdStandBy;
import com.xiaomi.infra.galaxy.sds.thrift.ColdStandByCycle;
import com.xiaomi.infra.galaxy.sds.thrift.CommonConstants;
import com.xiaomi.infra.galaxy.sds.thrift.Credential;
import com.xiaomi.infra.galaxy.sds.thrift.DataType;
import com.xiaomi.infra.galaxy.sds.thrift.Datum;
import com.xiaomi.infra.galaxy.sds.thrift.DatumUtil;
import com.xiaomi.infra.galaxy.sds.thrift.EntityGroupSpec;
import com.xiaomi.infra.galaxy.sds.thrift.GetRequest;
import com.xiaomi.infra.galaxy.sds.thrift.GetResult;
import com.xiaomi.infra.galaxy.sds.thrift.KeySpec;
import com.xiaomi.infra.galaxy.sds.thrift.Permission;
import com.xiaomi.infra.galaxy.sds.thrift.ProvisionThroughput;
import com.xiaomi.infra.galaxy.sds.thrift.PutRequest;
import com.xiaomi.infra.galaxy.sds.thrift.Request;
import com.xiaomi.infra.galaxy.sds.thrift.ScanRequest;
import com.xiaomi.infra.galaxy.sds.thrift.ScanResult;
import com.xiaomi.infra.galaxy.sds.thrift.SnapshotType;
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



public class music {
    private static AdminService.Iface adminClient;
    private static TableService.Iface tableClient;
    private static AdminService.Iface appClient;
    private static String accountKey = "AKP3I74ZK3I4I53XLI"; // Your AccountKey
    private static String accountSecret = "gbCDj8H3SwuWQiy31prYm6/vKRkrV85Ap8E6n1ym"; // Your AccountSecret
    private static String endpoint = "http://cnbj1-sds.api.xiaomi.net";
    // private static String endpoint = "http://awsusor0.sds.api.xiaomi.com";

    //  private static String appId = "CId7233d0a-c558-4c78-85b3-a2f805a0d9d0"; // Your AppId
// private static String accountKey = "EAKWFRCNFYKLM"; // Your AppKey
//  private static String accountSecret = "SK3HKypQeL7xXZxm1ZIR6VgKDIlJXJ"; // Your AppSecret
//
    private static String appId = "2882303761517597197"; // Your AppId
    private static String appKey = "5331759752197"; // Your AppKey
    private static String appSecret = "ksicxJoWBeaDHIz1Vp1KWg=="; // Your AppSecret

//  private static String appSecret = "ksicxJoWBeaDHIz1Vp1KWg=="; // Your AppSecret

//  private static String accountKey = "AKP3I74ZK3I4I53XLI"; // Your AccountKey
//  private static String accountSecret = "gbCDj8H3SwuWQiy31prYm6/vKRkrV85Ap8E6n1ym"; // Your AccountSecret
//  private static String endpoint = "http://cnbj1-sds.api.xiaomi.net";


    private static boolean isInit = false;
    private static String tableName = "CL5860/music-comment-DEV";
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

    public static AdminService.Iface createAppAdminClient(String host) {
        Credential credential = getCredential(appKey, appSecret, UserType.APP_SECRET);
        ClientFactory clientFactory = new ClientFactory().setCredential(credential);
        return clientFactory.newAdminClient(host + CommonConstants.ADMIN_SERVICE_PATH, 50000, 3000);
    }

    public static Map<String, List<Permission>> cannedAclPermission(String appId, Permission... permissions) {
        Map<String, List<Permission>> appGrant = new HashMap<String, List<Permission>>();
        appGrant.put(appId, Arrays.asList(permissions));
        return appGrant;
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
        appClient = createAppAdminClient(endpoint);
        isInit = true;
    }

    private static TableSpec tableSpec() {
//    EntityGroupSpec entityGroupSpec = new EntityGroupSpec().setAttributes(
//            Arrays.asList(new KeySpec().setAttribute("score"))).setEnableHash(
//            true);
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
                .setThroughput(new ProvisionThroughput().setReadCapacity(200).setWriteCapacity(200))
                .setColdStandBy(new ColdStandBy().
                        setColdStandByCycle(ColdStandByCycle.WEEK).
                        setColdStandBySize(15).setEnableColdStandBy(true));
        //.setAcl(cannedAclPermission("A:"+appId, Permission.WRITE));



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
        //  adminClient.createTable(tableName, tableSpec);
        try {
            System.out.println("table " + tableName + ":");
            // System.out.println(adminClient.describeTable(tableName));
            System.out.println("All tables belong to you:");
            System.out.println(adminClient.findAllTables());

            //  System.out.println(adminClient.listSnapshots(tableName));

//      adminClient.restoreSnapshot(tableName,"sds_c4tst_4n79_____java-test-weather_snapshot1515134777815",
//              tableName, S);

            // put data
//            Date now = new Date();
//            PutRequest putRequest = new PutRequest();
//            for (int i = 0; i < 10; i++) {
//                putRequest.clear();
//                putRequest.setTableName(tableName);
//                putRequest.putToRecord("cityId", DatumUtil.toDatum(cities[i]));
//                putRequest.putToRecord("timestamp", DatumUtil.toDatum(now.getTime()));
//                putRequest.putToRecord("score", DatumUtil.toDatum((double) new Random().nextInt(100)));
//                putRequest.putToRecord("pm25", DatumUtil.toDatum((long) (new Random().nextInt(500))));
//                tableClient.put(putRequest);
//                System.out.println("put record #" + i);
//            }

            // batch put by partialAllowedBatch
//      List<BatchRequestItem> batch = new ArrayList<BatchRequestItem>();
//      for (int i = 11; i < 20; i++) {
//        BatchRequestItem item = new BatchRequestItem().setAction(BatchOp.PUT);
//        Map<String, Datum> record = new HashMap<String, Datum>();
//        record.put("cityId", DatumUtil.toDatum(cities[i]));
//        putRequest.putToRecord("timestamp", DatumUtil.toDatum(now.getTime()));
//        putRequest.putToRecord("score", DatumUtil.toDatum((double) new Random().nextInt(100)));
//        putRequest.putToRecord("pm25", DatumUtil.toDatum((long) (new Random().nextInt(500))));
//        item.setRequest(Request.putRequest(new PutRequest().setTableName(tableName).setRecord(record)));
//        batch.add(item);
//      }
//
//      while (true) {
//        BatchResult br = tableClient.partialAllowedBatch(new BatchRequest().setItems(batch));
//        List<BatchRequestItem> request = new ArrayList<BatchRequestItem>();
//        for (int i = 0; i < br.getItems().size(); i++) {
//          //At current only quota exceeded will get  isSuccess() == false
//          if (!br.getItems().get(i).isSuccess()) {
//            request.add(batch.get(i));
//          }
//        }
//        if (request.isEmpty()) {
//          break;
//        }
//        Thread.sleep(100); // wait a while to retry.
//        batch = request;
//      }

            // get data
//      GetRequest getRequest = new GetRequest();
//      getRequest.setTableName(tableName);
//      getRequest.putToKeys("cityId", DatumUtil.toDatum(cities[0]));
//      getRequest.putToKeys("timestamp", DatumUtil.toDatum(now.getTime()));
//      getRequest.addToAttributes("pm25");
//      GetResult getResult = tableClient.get(getRequest);
            //   printResult(getResult.getItem());

            // scan data
            ScanRequest scanRequest = new ScanRequest();

            HashMap<String, Datum> startKey = new HashMap<String, Datum>();
            HashMap<String, Datum> stopKey = new HashMap<String, Datum>();

            startKey.put("categoryID", DatumUtil.toDatum("special-915"));
            stopKey.put("categoryID", DatumUtil.toDatum("special-915"));
            // scan the whole table with retry
            scanRequest.clear();
            scanRequest.setTableName(tableName);
            scanRequest.setStartKey(startKey);
            scanRequest.setStopKey(stopKey);
            scanRequest.setLimit(1000);
            scanRequest.setCondition("createTime%2 == 0");


//            scanRequest.setTableName(tableName);
//            scanRequest.addToAttributes("cityId");
//            scanRequest.addToAttributes("score");
      //  scanRequest.setCondition("status <= 1");
//      scanRequest.setLimit(10);
//      ScanResult scanResult = tableClient.scan(scanRequest);
//      List<Map<String, Datum>> kvsList = scanResult.getRecords();
//      for (Map<String, Datum> kvs : kvsList) {
//        printResult(kvs);
//      }
        int count =0;
            TableScanner scanner = new TableScanner(tableClient, scanRequest);
            Iterator<Map<String, Datum>> iterator = scanner.iterator();
            while (iterator.hasNext()) {
                count++;
                printResult(iterator.next());
            }

            System.out.println("count:"+count);
        } finally {
            //  adminClient.disableTable(tableName);
            //  adminClient.dropTable(tableName);
        }
    }
}
