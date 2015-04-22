package com.xiaomi.infra.galaxy.rpc.util;


import com.xiaomi.infra.galaxy.rpc.thrift.Version;

public class VersionUtil {
  public static String versionString(Version version) {
    return String.format("%d.%d.%s", version.major, version.minor, version.patch);
  }
}
