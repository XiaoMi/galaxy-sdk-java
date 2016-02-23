include "Common.thrift"
include "Queue.thrift"

namespace java com.xiaomi.infra.galaxy.emq.thrift
namespace php EMQ.Statistics
namespace py emq.statistics

/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: haxiaolin@xiaomi.com
 */
struct UserQuota {
  /**
  *The restriction of user throughput;
  **/
  1: optional Queue.Throughput throughput;

  /**
  *The number of queues owned by one user;
  **/
  2: optional i64 queueNumber;

}

struct SetUserQuotaRequest {
  /**
  *The developerId of an EMQ user;
  **/
  1: required string developerId;

  /**
  * The quota of a user;
  **/
  2: required UserQuota userQuota;

}

struct GetUserQuotaRequest {
  /**
  *The developerId of an EMQ user;
  **/
  1: optional string developerId;
}

struct GetUserQuotaResponse {
  /**
  *The developerId of an EMQ user;
  **/
  1: required string developerId;

  /**
  * The quota of a user;
  **/
  2: required UserQuota userQuota;

}

struct GetUserUsedQuotaRequest{
  /**
  *The developerId of an EMQ user;
  **/
  1: optional string developerId;
}

struct SetUserInfoRequest {
  /**
  *The developerId of an EMQ user;
  **/
  1: optional string developerId;

  /**
  *User name;
  **/
  2: optional string userName;

  /**
  *Email address;
  **/
  3: optional string email;

  /**
  *SMS number;
  **/
  4: optional string sms;

}

struct GetUserInfoRequest {
  /**
  *The developerId of an EMQ user, default is current user;
  **/
  1: optional string developerId;

}

struct GetUserInfoResponse {
  /**
  *The developerId of an EMQ user;
  **/
  1: required string developerId;

  /**
  *User name;
  **/
  2: optional string userName;

  /**
  *Email address;
  **/
  3: optional string email;

  /**
  *SMS number;
  **/
  4: optional string sms;

}

enum ALERT_TYPE{
  SEND_REQUEST,
  RECEIVE_REQUEST,
  CHANGE_REQUEST,
  DELETE_REQUEST,
  SINGLE_SEND_REQUEST,
  BATCH_SEND_REQUEST,
  SHORT_RECEIVE_REQUEST,
  LONG_RECEIVE_REQUEST,
  QUEUE_MESSAGE_NUMBER,
}

enum MEASUREMENT{
  LATENCY,
  LATENCY_P999,
  COUNT,
}

struct AlertPolicy{
  /**
  *The operation to be monitored;
  **/
  1: required ALERT_TYPE type;

  /**
  *The measurement to be monitored;
  **/
  2: required MEASUREMENT measurement;

  /**
  *If the monitored value >= threshold, it will send alert messages;
  *If measurement is LATENCY, then threshold unit is milliseconds.
  **/
  3: optional double threshold;

}

struct AddAlertPolicyRequest{
 /**
  *Queue name;
  **/
  1: required string queueName;

  /**
  *The alert policy;
  **/
  2: required AlertPolicy alertPolicy;

}

struct DeleteAlertPolicyRequest{
 /**
  *Queue name;
  **/
  1: required string queueName;

  /**
  *The alert policy;
  **/
  2: required AlertPolicy alertPolicy;

}

struct ListQueueAlertPoliciesRequest{
 /**
  *Queue name;
  **/
  1: required string queueName;
}

struct ListQueueAlertPoliciesResponse{
 /**
  *Queue name;
  **/
  1: required string queueName;

  /**
  *Alert policy list;
  **/
  2: required list<AlertPolicy> alertPolicies;

}

struct SetQueueDailyStatisticsStateRequest{
 /**
  *Queue name;
  **/
  1: required string queueName;

 /**
  *Enable or disable queue daily statistics
  **/
  2: required bool enabled;
}

struct GetQueueDailyStatisticsStateRequest{
 /**
  *Queue name;
  **/
  1: required string queueName;
}

struct GetQueueDailyStatisticsStateResponse{
 /**
  *Queue name;
  **/
  1: required string queueName;

 /**
  *Queue daily statistics state: enabled or disabled;
  **/
  2: required bool enabled;
}

service StatisticsService extends Common.EMQBaseService {
  /**
  * Set user quota, must be ADMIN user;
  **/
  void setUserQuota(1: SetUserQuotaRequest request) throws (1: Common.GalaxyEmqServiceException e);

  /**
  * Get user quota;
  **/
  GetUserQuotaResponse getUserQuota(1: GetUserQuotaRequest request) throws (1: Common.GalaxyEmqServiceException e);

  /**
  * Get user used quota;
  **/
  GetUserQuotaResponse getUserUsedQuota(1: GetUserUsedQuotaRequest request) throws (1: Common.GalaxyEmqServiceException e);

  /**
  * Set user info;
  **/
  void setUserInfo(1: SetUserInfoRequest request) throws (1: Common.GalaxyEmqServiceException e);

  /**
  * Get user info;
  **/
  GetUserInfoResponse getUserInfo(1: GetUserInfoRequest request) throws (1: Common.GalaxyEmqServiceException e);

  /**
  * Add an alert policy for queue;
  **/
  void addQueueAlertPolicy(1: AddAlertPolicyRequest request) throws (1: Common.GalaxyEmqServiceException e);

  /**
  * Delete an alert policy for queue;
  **/
  void deleteQueueAlertPolicy(1: DeleteAlertPolicyRequest request) throws (1: Common.GalaxyEmqServiceException e);

  /**
  * Clear alert policies for queue;
  **/
  ListQueueAlertPoliciesResponse listQueueAlertPolicies(1: ListQueueAlertPoliciesRequest request) throws (1: Common.GalaxyEmqServiceException e);

  /**
  * Set daily statistics state for queue;
  **/
  void setQueueDailyStatisticsState(1: SetQueueDailyStatisticsStateRequest request) throws (1: Common.GalaxyEmqServiceException e);

  /**
  * Get daily statistics state of queue;
  **/
  GetQueueDailyStatisticsStateResponse getQueueDailyStatisticsState(1: GetQueueDailyStatisticsStateRequest request) throws (1: Common.GalaxyEmqServiceException e);


}