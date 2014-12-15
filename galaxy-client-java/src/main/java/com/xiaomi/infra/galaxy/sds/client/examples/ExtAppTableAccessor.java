package com.xiaomi.infra.galaxy.sds.client.examples;

import com.xiaomi.infra.galaxy.sds.client.ClientFactory;
import com.xiaomi.infra.galaxy.sds.thrift.AppUserAuthProvider;
import com.xiaomi.infra.galaxy.sds.thrift.AuthService;
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
import libthrift091.TException;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class ExtAppTableAccessor {
  private static TableService.Iface tableClient;
  private String appId;
  private static String[] categories = { "work", "travel", "food" };
  private static int M = 10;
  private String endpoint;
  private String tableName;
  private AppUserAuthProvider appUserAuthProvider;
  private String accessToken;

  public ExtAppTableAccessor(String appId, String tableName, String endpoint,
      AppUserAuthProvider appUserAuthProvider, String accessToken) throws TException {
    this.appId = appId;
    this.tableName = tableName;
    this.endpoint = endpoint;
    this.appUserAuthProvider = appUserAuthProvider;
    this.accessToken = accessToken;
    init();
  }

  private void init() throws TException {
    ClientFactory clientFactory = new ClientFactory();
    AuthService.Iface authClient = clientFactory
        .newAuthClient(endpoint + CommonConstants.AUTH_SERVICE_PATH);
    Credential credential = authClient
        .createCredential(appId, appUserAuthProvider, accessToken);
    clientFactory = new ClientFactory(credential);
    tableClient = clientFactory
        .newTableClient(endpoint + CommonConstants.TABLE_SERVICE_PATH, 10000, 3000, true, 3);
  }

  private void printResult(Map<String, Datum> resultToPrint) {
    for (Map.Entry<String, Datum> e : resultToPrint.entrySet()) {
      System.out.println(
          String.format("[%s] => %s", e.getKey(), DatumUtil.fromDatum(e.getValue()).toString()));
    }
  }

  //When access data, user need not specify the entity group
  public void putData() throws Exception {
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
      System.out.println("Put record # " + i);
    }
  }

  public void getData() throws Exception {
    GetRequest getRequest = new GetRequest();
    getRequest.setTableName(tableName);
    getRequest.putToKeys("noteId", DatumUtil.toDatum((long) (new Random().nextInt(M))));
    GetResult getResult = tableClient.get(getRequest);
    printResult(getResult.getItem());
  }

  public void scanData() throws Exception {
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
        System.out.println(e.getKey() + "\t" + DatumUtil.fromDatum(e.getValue()));
      }
    }
  }

  public void accessData() throws Exception {
    putData();
    getData();
    scanData();
  }
}
