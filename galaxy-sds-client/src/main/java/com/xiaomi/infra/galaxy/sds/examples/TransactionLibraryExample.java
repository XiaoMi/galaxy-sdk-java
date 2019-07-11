package com.xiaomi.infra.galaxy.sds.examples;

import com.xiaomi.infra.galaxy.sds.client.ClientFactory;
import com.xiaomi.infra.galaxy.sds.thrift.AdminService;
import com.xiaomi.infra.galaxy.sds.thrift.BatchOp;
import com.xiaomi.infra.galaxy.sds.thrift.BatchRequest;
import com.xiaomi.infra.galaxy.sds.thrift.BatchRequestItem;
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
import com.xiaomi.infra.galaxy.sds.thrift.TableMetadata;
import com.xiaomi.infra.galaxy.sds.thrift.TableQuota;
import com.xiaomi.infra.galaxy.sds.thrift.TableSchema;
import com.xiaomi.infra.galaxy.sds.thrift.TableService;
import com.xiaomi.infra.galaxy.sds.thrift.TableSpec;
import com.xiaomi.infra.galaxy.sds.thrift.Transaction;
import com.xiaomi.infra.galaxy.sds.thrift.UserType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class TransactionLibraryExample {
  private static AdminService.Iface adminClient;
  private static TableService.Iface tableClient;
  private static String accountKey = ""; // Your AccountKey
  private static String accountSecret = ""; // Your AccountSecret
  private static String endpoint = "http://cnbj-s0.sds.api.xiaomi.com";
  private static boolean isInit = false;
  private static String transferTableName = "java-transfer-table";
  private static String usersTableName = "java-users-table";

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

  private static TableSpec tableSpecTransferTable() {
    List<KeySpec> primaryKey = Arrays.asList(new KeySpec[] { new KeySpec().setAttribute("fromUserId"), new KeySpec().setAttribute(
        "toUserId"),  new KeySpec().setAttribute("transferId")});
    Map<String, DataType> attributes = new HashMap<String, DataType>();
    attributes.put("fromUserId", DataType.STRING);
    attributes.put("toUserId", DataType.STRING);
    attributes.put("amount", DataType.INT64);
    attributes.put("time", DataType.STRING);
    attributes.put("amount", DataType.INT64);
    //use timestamp as transferId
    attributes.put("transferId", DataType.INT64);
    TableSchema tableSchema = new TableSchema();
    tableSchema.setPrimaryIndex(primaryKey).setAttributes(attributes).setTtl(-1)
        //enable transaction for TransactionLibraryExample
        .setEnableTransaction(true);

    TableMetadata tableMetadata = new TableMetadata();
    tableMetadata
        .setQuota(new TableQuota().setSize(10 * 1024 * 1024))
        .setThroughput(new ProvisionThroughput().setReadCapacity(100).setWriteCapacity(100));
    return new TableSpec().setSchema(tableSchema).setMetadata(tableMetadata);
  }

  private static TableSpec tableSpecUsersTable() {
    List<KeySpec> primaryKey = Arrays.asList(new KeySpec[] { new KeySpec().setAttribute("userId") });
    Map<String, DataType> attributes = new HashMap<String, DataType>();
    attributes.put("userId", DataType.STRING);
    attributes.put("balance", DataType.INT64);
    TableSchema tableSchema = new TableSchema();
    tableSchema.setPrimaryIndex(primaryKey).setAttributes(attributes).setTtl(-1)
        //enable transaction for TransactionLibraryExample
        .setEnableTransaction(true);

    TableMetadata tableMetadata = new TableMetadata();
    tableMetadata
        .setQuota(new TableQuota().setSize(10 * 1024 * 1024))
        .setThroughput(new ProvisionThroughput().setReadCapacity(100).setWriteCapacity(100));
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
    TableSpec transferTableSpec = tableSpecTransferTable();
    TableSpec usersTableSpec = tableSpecUsersTable();

    adminClient.createTable(transferTableName, transferTableSpec);
    adminClient.createTable(usersTableName, usersTableSpec);
    try {
      System.out.println("table " + transferTableName + ":");
      System.out.println(adminClient.describeTable(transferTableName));
      System.out.println("table " + usersTableName + ":");
      System.out.println(adminClient.describeTable(usersTableName));
      System.out.println("All tables belong to you:");
      System.out.println(adminClient.findAllTables());
      Date now = new Date();
      long transferId = now.getTime();
      // new transaction
      Transaction transaction = tableClient.newTransaction();
      GetRequest getRequest= new GetRequest();
      getRequest.setTableName(transferTableName);
      Map<String, Datum> record = new HashMap<String, Datum>();
      record.put("fromUserId", DatumUtil.toDatum("Tom"));
      record.put("toUserId", DatumUtil.toDatum("Bob"));
      record.put("transferId", DatumUtil.toDatum(transferId));
      getRequest.setKeys(record);
      // transactionGet
      GetResult getResult = tableClient.transactionGet(transaction, getRequest);
      if(getResult.getItem() == null){
        // Tom hasn't transferred to Bob yet.
        List<BatchRequestItem> batchRequestItems = new ArrayList<BatchRequestItem>();

        //put record to TransferTable
        BatchRequestItem transferItem = new BatchRequestItem().setAction(BatchOp.PUT);
        Map<String, Datum> transferRecord = new HashMap<String, Datum>();
        transferRecord.put("fromUserId", DatumUtil.toDatum("Tom"));
        transferRecord.put("toUserId", DatumUtil.toDatum("Bob"));
        transferRecord.put("amount", DatumUtil.toDatum(20L));
        transferRecord.put("time", DatumUtil.toDatum("2018-10-10"));
        transferRecord.put("transferId",DatumUtil.toDatum(transferId));
        transferItem.setRequest(
              Request.putRequest(new PutRequest().setTableName(transferTableName).setRecord(transferRecord)));

        //update Tom balance
        BatchRequestItem uesrTomItem = new BatchRequestItem().setAction(BatchOp.PUT);
        Map<String, Datum> userTomRecord = new HashMap<String, Datum>();
        userTomRecord.put("userId", DatumUtil.toDatum("Tom"));
        userTomRecord.put("balance", DatumUtil.toDatum(80L));
        uesrTomItem.setRequest(Request.putRequest(
            new PutRequest().setTableName(usersTableName).setRecord(userTomRecord)));

        //update Bob balance
        BatchRequestItem uesrBobItem = new BatchRequestItem().setAction(BatchOp.PUT);
        Map<String, Datum> userBobRecord = new HashMap<String, Datum>();
        userBobRecord.put("userId", DatumUtil.toDatum("Bob"));
        userBobRecord.put("balance", DatumUtil.toDatum(120L));
        uesrBobItem.setRequest(Request.putRequest(
            new PutRequest().setTableName(usersTableName).setRecord(userBobRecord)));

        batchRequestItems.add(transferItem);
        batchRequestItems.add(uesrTomItem);
        batchRequestItems.add(uesrBobItem);

        tableClient.transactionCommit(transaction,new BatchRequest().setItems(batchRequestItems));
      }

    } finally {
      adminClient.disableTable(transferTableName);
      adminClient.dropTable(transferTableName);
      adminClient.disableTable(usersTableName);
      adminClient.dropTable(usersTableName);
    }
  }
}
