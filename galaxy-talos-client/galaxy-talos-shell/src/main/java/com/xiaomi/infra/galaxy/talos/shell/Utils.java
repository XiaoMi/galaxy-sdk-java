package com.xiaomi.infra.galaxy.talos.shell;

public class Utils {
  public static String idToPermission(Integer id) {
    switch (id) {
      case 0:
        return "NONE";
      case 1:
        return "PUT_MESSAGE";
      case 2:
        return "GET_MESSAGE";
      case 3:
        return "FULL_MESSAGE_CONTROL";
      case 4:
        return "DESCRIBE_TOPIC";
      case 5:
        return "PUT_MESSAGE_AND_DESCRIBE_TOPIC";
      case 6:
        return "GET_MESSAGE_AND_DESCRIBE_TOPIC";
      case 7:
        return "TOPIC_READ_AND_MESSAGE_FULL_CONTROL";
      case 8:
        return "CHANGE_TOPIC";
      case 12:
        return "FULL_TOPIC_CONTROL";
      case 15:
        return "FULL_CONTROL";
      case 16:
        return "CHANGE_PERMISSION";
      case 31:
        return "ADMIN";
      default:
        System.out.println("An error occurred");
        return "";
    }
  }

  public String getFileNameFromCluster(String clusterName) {
    String[] strings = clusterName.split("-");
    Integer length = strings.length;
    if (length == 3) {
      return strings[0] + strings[1];
    } else {
      return strings[0];
    }
  }

  public String getFileName(String pathname, String clusterName, String functionName) {
    return pathname + getFileNameFromCluster(clusterName) + "." + functionName;
  }
}