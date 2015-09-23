include "Common.thrift"

namespace java com.xiaomi.infra.galaxy.emq.thrift
namespace php EMQ.Queue
namespace py emq.queue
namespace go emq.queue

/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: shenyuannan@xiaomi.com
 */

struct QueueAttribute {
  /**
  * Queue delay seconds, message send to this queue will invisible until after
  * delaySeconds, default 0s (0s ~ 15min)
  **/
  1: optional i32 delaySeconds;

  /**
  * Queue invisibility seconds, after message received form this queue, in
  * invisibilitySeconds this will not received through receiveMessage. When
  * after invisibilitySeconds if no deleteMessage called for this message, this
  * message will receive again, default 30s (2s ~ 12hour)
  **/
  2: optional i32 invisibilitySeconds;

  /**
  * The seconds wait when receiveMessage called, default 0s (0s ~ 20s)
  **/
  3: optional i32 receiveMessageWaitSeconds;

  /**
  * Maximum receive message number in this queue, default 100(1 ~ 100)
  **/
  4: optional i32 receiveMessageMaximumNumber;

  /**
  * message retention seconds in this queue, default 4days (60s ~ 14days)
  **/
  5: optional i32 messageRetentionSeconds;

  /**
  * Max message size in this queue, default 256K (1K ~ 256K)
  **/
  6: optional i32 messageMaximumBytes;

  /**
  * Partition number for this queue default 4 (1 ~ 255)
  **/
  7: optional i32 partitionNumber;

  /**
  * User-defined attributes;
  **/
  8: optional map<string, string> userAttributes;
}

struct QueueState {
  /**
  * Queue create timestamp;
  **/
  1: required i64 createTimestamp;

  /**
  * Queue last modified timestamp;
  **/
  2: required i64 lastModifiedTimestamp;

  /**
  * The approximate message number in this queue;
  **/
  3: required i64 approximateMessageNumber;

  /**
  * The available message number in this queue, this is for message that could
  * be get using receivedMessage
  **/
  4: required i64 approximateAvailableMessageNumber;

  /**
  * The invisibility message number in this queue, this is for received message
  * that in invisibilitySeconds and not deleted;
  **/
  5: required i64 approximateInvisibilityMessageNumber;
}

struct Throughput {
  /**
   * Queue read qps;
   **/
  1: optional i64 readQps;

  /**
   * Queue write qps;
   **/
  2: optional i64 writeQps;
}

struct SpaceQuota {
  /**
   * Queue read qps;
   **/
  1: optional i64 size;
}

struct QueueQuota {
  /**
   * Queue space quota;
   **/
  1: optional SpaceQuota spaceQuota;

  /**
   * Queue read and qps;
   **/
  2: optional Throughput throughput;
}

struct CreateQueueRequest {
  /**
  * The queue name;
  **/
  1: required string queueName;

  /**
  * The queue attribute;
  **/
  2: optional QueueAttribute queueAttribute;

  /**
   * The queue quota, including space quota, read qps, and write qps;
   **/
  3: optional QueueQuota queueQuota;
}

struct CreateQueueResponse {
  /**
  * The queue name;
  * The name returned here may be a little different from user set in request (with developerId as prefix).
  * So the user should use the name returned by this response for those following operations
  **/
  1: required string queueName;

  /**
  * The queue attribute;
  **/
  2: required QueueAttribute queueAttribute;

  /**
   * The queue quota;
   **/
  3: optional QueueQuota queueQuota;

}

struct DeleteQueueRequest {
  /**
  * The queue name;
  **/
  1: required string queueName;
}

struct PurgeQueueRequest {
  /**
  * The queue name;
  **/
  1: required string queueName;
}

struct SetQueueAttributesRequest {
  /**
  * The queue name;
  **/
  1: required string queueName;

  /**
  * The queue attribute;
  **/
  2: optional QueueAttribute queueAttribute;
}

struct SetQueueAttributesResponse {
  /**
  * The queue name;
  **/
  1: required string queueName;

  /**
  * The queue attribute;
  **/
  2: required QueueAttribute queueAttribute;
}

struct SetQueueQuotaRequest {
  /**
  * The queue name;
  **/
  1: required string queueName;

  /**
  * The queue quota;
  **/
  2: optional QueueQuota queueQuota;
}

struct SetQueueQuotaResponse {
  /**
  * The queue name;
  **/
  1: required string queueName;

  /**
  * The queue quota;
  **/
  2: optional QueueQuota queueQuota;
}

struct GetQueueInfoRequest {
  /**
  * The queue name;
  **/
  1: required string queueName;
}

struct GetQueueInfoResponse {
  /**
  * The queue name;
  **/
  1: required string queueName;

  /**
  * The queue attribute;
  **/
  2: required QueueAttribute queueAttribute;

  /**
  * The queue state;
  **/
  3: required QueueState queueState;

  /**
   * The queue quota;
   **/
  4: optional QueueQuota queueQuota;
}

struct ListQueueRequest {
  /**
  * The queue name prefix;
  **/
  1: optional string queueNamePrefix = "";
}

struct ListQueueResponse {
  /**
  * The queueName list with queueNamePrefix;
  **/
  1: required list<string> queueName;
}

enum Permission {
  NONE, /* Don't have any specific permission */
  SEND_MESSAGE, /* send messages */
  RECEIVE_MESSAGE, /* receive messages */
  SEND_RECEIVE_MESSAGE, /* send/receive messages */
  HANDLE_MESSAGE, /* receive messages, change messages visibility, delete messages */
  SEND_HANDLE_MESSAGE, /* send/handle messages */
  GET_QUEUE_INFO,  /* get queue info */
  USE_QUEUE, /* get queue info / full control messages */
  ADMIN_QUEUE, /* change attribute of queue / delete queue / list queue / purge queue */
  FULL_CONTROL /* all permissions. The owner of queue and the administrator of EMQ have this permission always */
}

struct SetPermissionRequest {
  1: required string queueName;
  2: required string developerId;
  3: required Permission permission;
}

struct RevokePermissionRequest {
  1: required string queueName;
  2: required string developerId;
}

struct QueryPermissionRequest {
  1: required string queueName;
}

struct QueryPermissionResponse {
  1: required Permission permission;
}

struct QueryPermissionForIdRequest {
  1: required string queueName;
  2: required string developerId;
}

struct QueryPermissionForIdResponse {
  1: required Permission permission;
}

struct ListPermissionsRequest {
  1: required string queueName;
}

struct ListPermissionsResponse {
  /* map contains developerId => Permission pair */
  1: map<string, Permission> permissionList;
}

service QueueService extends Common.EMQBaseService {
  /**
  * Create queue;
  **/
  CreateQueueResponse createQueue(1: CreateQueueRequest request) throws (1: Common.GalaxyEmqServiceException e);

  /**
  * Delete queue;
  **/
  void deleteQueue(1: DeleteQueueRequest request) throws (1: Common.GalaxyEmqServiceException e);

  /**
  * Purge queue;
  **/
  void purgeQueue(1: PurgeQueueRequest request) throws (1: Common.GalaxyEmqServiceException e);

  /**
  * Set queue attribute;
  **/
  SetQueueAttributesResponse setQueueAttribute(1: SetQueueAttributesRequest request) throws (1: Common.GalaxyEmqServiceException e);

  /**
  * Set queue quota;
  **/
  SetQueueQuotaResponse setQueueQuota(1: SetQueueQuotaRequest request) throws (1: Common.GalaxyEmqServiceException e);

  /**
  * Get queue info, include QueueAttribute, QueueState and QueueQuota;
  **/
  GetQueueInfoResponse getQueueInfo(1: GetQueueInfoRequest request) throws (1: Common.GalaxyEmqServiceException e);

  /**
  * List queue with queueNamePrefix;
  **/
  ListQueueResponse listQueue(1: ListQueueRequest request) throws (1: Common.GalaxyEmqServiceException e);

  /**
  * Set permission for developer
  * FULL_CONTROL required to use this method
  **/
  void setPermission(1: SetPermissionRequest request)
      throws (1: Common.GalaxyEmqServiceException e);

  /**
  * Revoke permission for developer
  * FULL_CONTROL required to use this method
  */
  void revokePermission(1: RevokePermissionRequest request)
      throws (1: Common.GalaxyEmqServiceException e);

  /**
  * query permission for developer using this method
  * no permission required to use this method
  */
  QueryPermissionResponse queryPermission(1: QueryPermissionRequest request)
      throws (1: Common.GalaxyEmqServiceException e);

  /**
  * List permission for developer
  * ADMIN_QUEUE required to use this method
  */
  QueryPermissionForIdResponse queryPermissionForId(
  1: QueryPermissionForIdRequest request)
      throws (1: Common.GalaxyEmqServiceException e);

  /**
  * list permissions for all users of the queue
  * ADMIN_QUEUE required to use this method
  **/
  ListPermissionsResponse listPermissions(1: ListPermissionsRequest request)
      throws (1: Common.GalaxyEmqServiceException e);
}
