/**
 * Copyright 2016, Xiaomi.
 * All rights reserved.
 * Author: xiajun@xiaomi.com
 */

package com.xiaomi.infra.galaxy.lcs.log.log4j.appender;

import org.apache.log4j.Logger;

import com.xiaomi.infra.galaxy.lcs.log.core.ILogger;
import com.xiaomi.infra.galaxy.lcs.log.core.appender.LCSAppender;
import com.xiaomi.infra.galaxy.lcs.log.core.appender.LCSThriftAppenderImpl;
import com.xiaomi.infra.galaxy.lcs.log.core.utils.ConfigureChecker;
import com.xiaomi.infra.galaxy.lcs.log.log4j.logger.AppenderLogger;

public class LCSThriftAppender extends LCSLog4jAppender {
  private static final Logger LOG = Logger.getLogger(LCSThriftAppender.class);
  private static final ILogger logger = new AppenderLogger(LOG);

  private String lcsAgentHostname;
  private int lcsAgentPort;

  private LCSThriftAppenderImpl lcsThriftAppender;

  public LCSThriftAppender() {
    super(logger, new LCSThriftAppenderImpl(logger));

    lcsThriftAppender = (LCSThriftAppenderImpl)lcsAppender;
  }

  public String getLcsAgentHostname() {
    return lcsAgentHostname;
  }


  public void setLcsAgentHostname(String lcsAgentHostname) {
    ConfigureChecker.checkConfigureBlank("lcsAgentHostname", lcsAgentHostname);

    this.lcsAgentHostname = lcsAgentHostname;
    lcsThriftAppender.setLcsAgentHost(lcsAgentHostname);
  }

  public int getLcsAgentPort() {
    return lcsAgentPort;
  }

  public void setLcsAgentPort(int lcsAgentPort) {
    ConfigureChecker.checkConfigureRange("lcsAgentPort", lcsAgentPort, 1, Integer.MAX_VALUE);

    this.lcsAgentPort = lcsAgentPort;
    lcsThriftAppender.setLcsAgentPort(lcsAgentPort);
  }
}
