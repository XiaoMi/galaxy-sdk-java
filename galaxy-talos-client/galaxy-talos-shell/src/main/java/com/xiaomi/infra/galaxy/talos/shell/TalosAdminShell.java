package com.xiaomi.infra.galaxy.talos.shell;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TalosAdminShell {
  static String line = "------------------------------------------";
  private static final Logger LOG = LoggerFactory.getLogger(TalosAdminShell.class);

  public static void main(String[] args) throws Exception {
    Options options = new Options();
    options.addOption("H", "help", false, "list help");
    options.addOption("createTopic", false, "Create a Topic using this interface," +
        "Need to add parameters: -n -p -o");
    options.addOption("deleteTopic", false, "Delete an existing Topic," +
        "Need to add parameters: -n");
    options.addOption("describeTopic", false, "Display information about an " +
        "existing Topic,Need to add parameters: -n");
    options.addOption("listTopic", false, "Show all Topic information");
    options.addOption("listTopicsInfo", false, "Display information about " +
        "all Info in Topic");
    options.addOption("getTopicOffset", false, "Get all Offset information " +
        "for a Topic,Need to add parameters: -n");
    options.addOption("getPartitionOffset", false, "Get all Offset information " +
        "for a Partition of a Topic,Need to add parameters: -n -i");
    options.addOption("getScheduleInfo", false, "Get all the Schedule " +
        "information for a Topic,Need to add parameters: -n");
    options.addOption("getTopicPartitionSet", false, "Get the Topic information " +
        "corresponding to each Host");
    options.addOption("applyQuota", false, "Apply for a Quota," +
        "Need to add parameters: -o -q");
    options.addOption("approveQuota", false, "Approve an already applied Quota," +
        "Need to add parameters: -o -q");
    options.addOption("setQuota", false, "Produce a usable Quota," +
        "Need to add parameters: -o -q");
    options.addOption("revokeQuota", false, "Reject a Quota application," +
        "Need to add parameters: -o -q");
    options.addOption("listQuota", false, "Get all visible Quota information");
    options.addOption("listPendingQuota", false, "Get all visible unallocated " +
        "Quota information");
    options.addOption("getWorkerId", false, "Get consumer client workerId, " +
        "Need to add parameters: -n -i -g");

    options.addOption("n", "topicName", true, "topic name");
    options.addOption("i", "partitionId", true, "partition id");
    options.addOption("p", "partitionNumber", true, "partition number");
    options.addOption("o", "orgId", true, "orgId");
    options.addOption("q", "totalQuota", true, "total quota");
    options.addOption("g", "consumerGroupName", true, "consumer group name");

    CommandLineParser parser = new DefaultParser();
    CommandLine cmd;
    try {
      cmd = parser.parse(options, args);
    } catch (ParseException e) {
      LOG.error("Exception in CommandLineParser:", e);
      System.out.println("input error: " + e.getMessage());
      return;
    }
    AdminOperatior adminOperator = new AdminOperatior();

    if (cmd.hasOption("H")) {
      String TalosShell = "TalosAdminShell";
      HelpFormatter helpFormatter = new HelpFormatter();
      helpFormatter.setOptionComparator(null);
      helpFormatter.printHelp(TalosShell, line, options, line);
      return;
    } else if (cmd.hasOption("createTopic")) {
      if ((cmd.hasOption("n")) && (cmd.hasOption("p")) && (cmd.hasOption("o"))) {
        String topicName = cmd.getOptionValue("n");
        int partitionNumber = Integer.parseInt(cmd.getOptionValue("p"));
        String orgId = cmd.getOptionValue("o");
        adminOperator.createTopic(topicName, orgId, partitionNumber);
        LOG.info("createTopic OK!");
      } else {
        System.out.println("createTopic must have parameters: -n(topicName)," +
            " -p(partitionNumber), -o(orgId)");
      }
    } else if (cmd.hasOption("deleteTopic")) {
      if (cmd.hasOption("n")) {
        String topicName = cmd.getOptionValue("n");
        adminOperator.deleteTopic(topicName);
        LOG.info("deleteTopic OK!");
      } else
        System.out.println("deleteTopic must have parameters: -n(topicName)");
    } else if (cmd.hasOption("describeTopic")) {
      if (cmd.hasOption("n")) {
        String topicName = cmd.getOptionValue("n");
        adminOperator.describeTopic(topicName);
      } else
        System.out.println("describeTopic must have parameters: -n(topicName)");
    } else if (cmd.hasOption("listTopic")) {
      adminOperator.listTopic();
    } else if (cmd.hasOption("listTopicsInfo")) {
      adminOperator.listTopicsinfo();
    } else if (cmd.hasOption("getTopicOffset")) {
      if (cmd.hasOption("n")) {
        String topicName = cmd.getOptionValue("n");
        adminOperator.getTopicOffset(topicName);
      } else
        System.out.println("getTopicOffset must have parameters: -n(topicName)");
    } else if (cmd.hasOption("getPartitionOffset")) {
      if ((cmd.hasOption("n")) && (cmd.hasOption("i"))) {
        String topicName = cmd.getOptionValue("n");
        int partitionId = Integer.parseInt(cmd.getOptionValue("i"));
        adminOperator.getPartitionOffset(topicName, partitionId);
      } else {
        System.out.println("getPartitionOffset must have parameters:" +
            " -n(topicName), -i(partitionId)");
      }
    } else if (cmd.hasOption("getScheduleInfo")) {
      if (cmd.hasOption("n")) {
        String topicName = cmd.getOptionValue("n");
        adminOperator.getScheduleInfo(topicName);
      } else
        System.out.println("getScheduleInfo must have parameters: -n(topicName)");

    } else if (cmd.hasOption("getTopicPartitionSet")) {
      adminOperator.getTopicPartitionSet();
    } else if (cmd.hasOption("applyQuota")) {
      if (((cmd.hasOption("o"))) && (cmd.hasOption("q"))) {
        String orgId = cmd.getOptionValue("o");
        int totalQuota = Integer.parseInt(cmd.getOptionValue("q"));
        adminOperator.applyQuota(orgId, totalQuota);
        LOG.info("applyQuota OK!");
      } else {
        System.out.println("applyQuota must have parameters:" +
            " -o(orgId), -q(totalQuota)");
      }
    } else if (cmd.hasOption("approveQuota")) {
      if (((cmd.hasOption("o"))) && (cmd.hasOption("q"))) {
        String orgId = cmd.getOptionValue("o");
        int totalQuota = Integer.parseInt(cmd.getOptionValue("q"));
        adminOperator.approveQuota(orgId, totalQuota);
        LOG.info("approveQuota OK!");
      } else {
        System.out.println("approveQuota must have parameters:" +
            " -o(orgId), -q(totalQuota)");
      }
    } else if (cmd.hasOption("setQuota")) {
      if (((cmd.hasOption("o"))) && (cmd.hasOption("q"))) {
        String orgId = cmd.getOptionValue("o");
        int totalQuota = Integer.parseInt(cmd.getOptionValue("q"));
        adminOperator.setQuota(orgId, totalQuota);
        LOG.info("setQuota OK!");
      } else {
        System.out.println("setQuota must have parameters:" +
            " -o(orgId), -q(totalQuota)");
      }
    } else if (cmd.hasOption("revokeQuota")) {
      if (((cmd.hasOption("o"))) && (cmd.hasOption("q"))) {
        String orgId = cmd.getOptionValue("o");
        int totalQuota = Integer.parseInt(cmd.getOptionValue("q"));
        adminOperator.revokeQuota(orgId, totalQuota);
        LOG.info("revokeQuota OK!");
      } else {
        System.out.println("revokeQuota must have parameters:" +
            " -o(orgId), -q(totalQuota)");
      }
    } else if (cmd.hasOption("listQuota")) {
      adminOperator.listQuota();
    } else if (cmd.hasOption("listPendingQuota")) {
      adminOperator.listPendingQuota();
    } else if (cmd.hasOption("getWorkerId")) {
      if (cmd.hasOption("n") && cmd.hasOption("i") && cmd.hasOption("g")) {
        String topicName = cmd.getOptionValue("n");
        int partitionId = Integer.parseInt(cmd.getOptionValue("i"));
        String consumerGroup = cmd.getOptionValue("g");
        adminOperator.getWorkerId(topicName, partitionId, consumerGroup);
      } else {
        System.out.println("getWorkerId must have parameters: " +
            "-n(topicName), -i(partitionId), -g(consumerGroupName)");
      }
    } else {
      System.out.println("input error, please check the input interface");
    }
  }
}