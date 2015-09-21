/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: linshangquan@xiaomi.com 
 */
package com.xiaomi.infra.galaxy.rpc.util;

import com.xiaomi.infra.galaxy.auth.authorization.Principal;
import com.xiaomi.infra.galaxy.auth.authorization.principal.AppRoot;
import com.xiaomi.infra.galaxy.auth.authorization.principal.Application;
import com.xiaomi.infra.galaxy.auth.authorization.principal.Developer;
import com.xiaomi.infra.galaxy.auth.authorization.principal.Guest;
import com.xiaomi.infra.galaxy.rpc.thrift.GrantType;
import com.xiaomi.infra.galaxy.rpc.thrift.Grantee;

public class GranteeUtil {
  public static Principal toPrincipal(Grantee grantee) {
    switch (grantee.getType()) {
    case DEVELOPER:
      return new Developer(grantee.getIdentifier());
    case APP_ROOT:
      return new AppRoot(grantee.getIdentifier());
    case APP_USER:
      return new Application(grantee.getIdentifier());
    case GUEST:
      return new Guest(grantee.getIdentifier());
    default:
      throw new IllegalArgumentException("Unsupported grantee type " + grantee.getType());
    }
  }

  public static Grantee toGrantee(Principal principal) {
    if (principal instanceof Developer) {
      return new Grantee().setType(GrantType.DEVELOPER).setIdentifier(
          ((Developer) principal).getDeveloperId());
    } else if (principal instanceof AppRoot) {
      return new Grantee().setType(GrantType.APP_ROOT).setIdentifier(
          ((AppRoot) principal).getAppId());
    } else if (principal instanceof Application) {
      return new Grantee().setType(GrantType.APP_USER).setIdentifier(
          ((Application) principal).getAppId());
    } else if (principal instanceof Guest) {
      return new Grantee().setType(GrantType.GUEST).setIdentifier(
          ((Guest) principal).getAppId());
    }
    throw new IllegalArgumentException("Unsupported principal " + principal);
  }
}
