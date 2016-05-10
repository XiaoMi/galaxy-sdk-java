package com.xiaomi.infra.galaxy.sds.client;

import com.xiaomi.infra.galaxy.sds.thrift.BatchRequest;
import com.xiaomi.infra.galaxy.sds.thrift.BatchRequestItem;
import com.xiaomi.infra.galaxy.sds.thrift.GetRequest;
import com.xiaomi.infra.galaxy.sds.thrift.IncrementRequest;
import com.xiaomi.infra.galaxy.sds.thrift.PutRequest;
import com.xiaomi.infra.galaxy.sds.thrift.RemoveRequest;
import com.xiaomi.infra.galaxy.sds.thrift.ScanRequest;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: linshangquan@xiaomi.com
 */
public class SdsRequestUtils {
  private final static Set<String> methods;
  static {
    methods = new HashSet<String>();
    methods.addAll(
        Arrays.asList("get", "put", "increment", "remove", "scan", "batch", "putToRebuildIndex"));
  }

  public static String getQuery(Method method,  Object[] args) {
    if (methods.contains(method.getName()) && args.length == 1) {
      return getQuery(method.getName(), args[0]);
    } else {
      return new StringBuilder("type=".length() + method.getName().length())
          .append("type=")
          .append(method.getName())
          .toString();
    }
  }


  private static String getTableName(Object request) {
    if (request instanceof PutRequest) {
      return ((PutRequest) request).getTableName();
    } else if (request instanceof IncrementRequest) {
      return ((IncrementRequest) request).getTableName();
    } else if (request instanceof GetRequest) {
      return ((GetRequest) request).getTableName();
    } else if (request instanceof RemoveRequest) {
      return ((RemoveRequest) request).getTableName();
    } else if (request instanceof ScanRequest) {
      return ((ScanRequest) request).getTableName();
    } else if (request instanceof BatchRequest) {
      BatchRequestItem batchRequestItem = ((BatchRequest) request).getItems().get(0);
      switch (batchRequestItem.getAction()) {
      case PUT:
        return batchRequestItem.getRequest().getPutRequest().getTableName();
      case GET:
        return batchRequestItem.getRequest().getGetRequest().getTableName();
      case INCREMENT:
        return batchRequestItem.getRequest().getIncrementRequest().getTableName();
      case REMOVE:
        return batchRequestItem.getRequest().getRemoveRequest().getTableName();
      default:
        throw new RuntimeException("Unknown batch action " + batchRequestItem.getAction());
      }
    } else {
      throw new RuntimeException("Unknown request type " + request.getClass().toString());
    }
  }

  private static String getQuery(String methodName, Object request) {
    return new StringBuilder().append("type=")
        .append(methodName)
        .append("&name=")
        .append(getTableName(request)).toString();
  }
}
