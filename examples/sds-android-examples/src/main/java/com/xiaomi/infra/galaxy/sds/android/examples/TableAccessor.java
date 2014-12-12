package com.xiaomi.infra.galaxy.sds.android.examples;

import com.xiaomi.infra.galaxy.sds.client.ClientFactory;
import com.xiaomi.infra.galaxy.sds.thrift.CommonConstants;
import com.xiaomi.infra.galaxy.sds.thrift.Credential;
import com.xiaomi.infra.galaxy.sds.thrift.Datum;
import com.xiaomi.infra.galaxy.sds.thrift.DatumUtil;
import com.xiaomi.infra.galaxy.sds.thrift.GetRequest;
import com.xiaomi.infra.galaxy.sds.thrift.GetResult;
import com.xiaomi.infra.galaxy.sds.thrift.PutRequest;
import com.xiaomi.infra.galaxy.sds.thrift.ScanRequest;
import com.xiaomi.infra.galaxy.sds.thrift.ScanResult;
import com.xiaomi.infra.galaxy.sds.thrift.TableService;
import com.xiaomi.infra.galaxy.sds.thrift.UserType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class TableAccessor {
  private static ClientFactory clientFactory;
  private static TableService.Iface tableClient;
  private static String secretKeyId = ""; // Set your AppKey
  private static String serviceToken = ""; // Set your ServiceToken
  private static UserType userType = UserType.APP_XIAOMI_SSO;
  private static final Logger LOG = LoggerFactory.getLogger(TableAccessor.class);
  private static String[] categories = { "work", "travel", "food" };
  private static int M = 10;
  private boolean isInit = false;
  private String endpoint;
  private String tableName;

  private void init() {
    Credential credential = new Credential().setSecretKey(serviceToken).setSecretKeyId(secretKeyId)
        .setType(userType);
    clientFactory = new ClientFactory(credential);
    tableClient = clientFactory.newTableClient(endpoint + CommonConstants.TABLE_SERVICE_PATH);
    isInit = true;
  }

  public TableAccessor(String tableName, String endpoint) {
    this.tableName = tableName;
    this.endpoint = endpoint;
  }

  private void printResult(Map<String, Datum> resultToPrint) {
    for (Map.Entry<String, Datum> e : resultToPrint.entrySet()) {
      LOG.info(
          String.format("[%s] => %s", e.getKey(), DatumUtil.fromDatum(e.getValue()).toString()));
    }
  }

  //When access data, user need not specify the entity group
  public void putData() throws Exception {
    if (!isInit) {
      init();
    }
    Date now = new Date();
    for (int i = 0; i < M; i++) {
      PutRequest putRequest = new PutRequest();
      putRequest.setTableName(tableName);
      putRequest.putToRecord("noteId", DatumUtil.toDatum((long) i));
      putRequest.putToRecord("title", DatumUtil.toDatum("Title " + i));
      putRequest.putToRecord("content", DatumUtil.toDatum("note " + i));
      putRequest.putToRecord("version", DatumUtil.toDatum((long) i));
      putRequest.putToRecord("mtime", DatumUtil.toDatum(now.getTime()));
      putRequest.putToRecord("category", DatumUtil.toDatum(categories[i % categories.length]));
      tableClient.put(putRequest);
      LOG.info("Put record # {}", i);
    }
  }

  public void getData() throws Exception {
    if (!isInit) {
      init();
    }
    GetRequest getRequest = new GetRequest();
    getRequest.setTableName(tableName);
    getRequest.putToKeys("noteId", DatumUtil.toDatum((long) (new Random().nextInt(M))));
    GetResult getResult = tableClient.get(getRequest);
    printResult(getResult.getItem());
  }

  public void scanData() throws Exception {
    if (!isInit) {
      init();
    }
    Map<String, Datum> startKey = new HashMap<String, Datum>();
    startKey.put("noteId", DatumUtil.toDatum((long) 1));
    Map<String, Datum> stopKey = new HashMap<String, Datum>();
    stopKey.put("noteId", DatumUtil.toDatum((long) 5));
    List<String> attributes = new ArrayList<String>();
    attributes.add("title");
    ScanRequest scanRequest = new ScanRequest()
        .setTableName(tableName)
        .setStartKey(startKey)
        .setStopKey(stopKey)
        .setLimit(100)
        .setAttributes(attributes);
    ScanResult scanResult = tableClient.scan(scanRequest);
    List<Map<String, Datum>> kvsList = scanResult.getRecords();
    for (Map<String, Datum> kvs : kvsList) {
      for (Map.Entry<String, Datum> e : kvs.entrySet()) {
        LOG.info(e.getKey() + "\t" + DatumUtil.fromDatum(e.getValue()));
      }
    }
  }
}
