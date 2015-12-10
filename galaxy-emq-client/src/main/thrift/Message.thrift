include "Common.thrift"

namespace java com.xiaomi.infra.galaxy.emq.thrift
namespace php EMQ.Message
namespace py emq.message
namespace go emq.message

/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: shenyuannan@xiaomi.com
 */

struct SendMessageRequest {
  /**
  * Queue name;
  **/
  1: required string queueName;

  /**
  * Message body;
  **/
  2: required string messageBody;

  /**
  * Delay seconds for this message, this will overwrite delaySecond of this
  * queue, default 0s (0s ~ 15min);
  **/
  3: optional i32 delaySeconds;

  /**
  * Invisibility seconds for this message, this will overwrite
  * invisibilitySeconds of this queue, default 30s (2s ~ 12hour);
  **/
  4: optional i32 invisibilitySeconds;

  /**
  * User-defined attributes attached to message
  **/
  5: optional map<string, Common.MessageAttribute> messageAttributes;
}

struct SendMessageResponse {
  /**
  * MessageID for the send message
  **/
  1: required string messageID;

  /**
  * Length of message body
  **/
  2: optional i32 bodyLength;


  /**
  * MD5 string of the message body
  **/
  3: optional string bodyMd5;

  /**
  * timestamp when the message arrived servers
  **/
  4: optional i64 sendTimestamp;
}


struct SendMessageBatchRequestEntry {
  /**
  * The identifier for this particular receipt handle;
  * Using to identify the result in response;
  * Need to be unique within one batch
  **/
  1: required string entryId;

  /**
  * Message body;
  **/
  2: required string messageBody;

  /**
  * Delay seconds for this message, this will overwrite delaySecond of this
  * queue, default 0s (0s ~ 15min);
  **/
  3: optional i32 delaySeconds;

  /**
  * Invisibility seconds for this message, this will overwrite
  * invisibilitySeconds of this queue, default 30s (2s ~ 12hour);
  **/
  4: optional i32 invisibilitySeconds;

  /**
  * User-defined attributes attached to message
  **/
  5: optional map<string, Common.MessageAttribute> messageAttributes;
}

struct SendMessageBatchRequest {
  /**
  * Queue name;
  **/
  1: required string queueName;

  /**
  * List of SendMessageBatchRequestEntry;
  **/
  2: required list<SendMessageBatchRequestEntry> sendMessageBatchRequestEntryList;
}

struct SendMessageBatchResponseEntry {
  /**
  * corresponding to the entryId in request
  **/
  1: required string entryId;

  /**
  * MessageID for the send message
  **/
  2: required string messageID;

  /**
  * Length of message body
  **/
  3: optional i32 bodyLength;


  /**
  * MD5 string of the message body
  **/
  4: optional string bodyMd5;

  /**
  * timestamp when the message arrived servers
  **/
  5: optional i64 sendTimestamp;
}

struct MessageBatchErrorEntry {
  /**
  * corresponding to the entryId/receiptHandle in request
  **/
  1: required string id;

  /**
  * The exception indicate why the request entry failed
  **/
  2: required Common.GalaxyEmqServiceException reason;
}

struct SendMessageBatchResponse {
  /**
  * The successful results list;
  **/
  1: list<SendMessageBatchResponseEntry> successful;

  /**
  * Failed results list;
  **/
  2: list<MessageBatchErrorEntry> failed;
}

struct ReceiveMessageRequest {
  /**
  * Queue name;
  **/
  1: required string queueName;

  /**
  * Max receive message number, default 100 (1 ~ 100);
  **/
  2: optional i32 maxReceiveMessageNumber = 100;

  /**
  * Max receive message wait seconds, default 20s (0 ~ 20), 0s means no wait;
  **/
  3: optional i32 maxReceiveMessageWaitSeconds = 0;

  /**
  * Attribute name to match
  * case-sensitive
  **/
  4: optional string attributeName;

  /**
  * Attribute value to match, corresponding to attributeName
  * case-sensitive
  **/
  5: optional Common.MessageAttribute attributeValue;

  /**
  * If this field is not_set/null/empty, default queue tag will be used
  **/
  6: optional string tagName;
}

struct ReceiveMessageResponse {
  /**
  * MessageID for the received message;
  **/
  1: required string messageID;

  /**
  * Receipt Handle for the received message
  * Using when change visibility time/delete message
  **/
  2: required string receiptHandle;

  /**
  * Message body for the received message;
  **/
  3: required string messageBody;

  /**
  * Attributes of message, including:
  * - senderId
  * - messageLength
  * - md5OfBody
  * - sendTimestamp
  * - receiveTimestamp
  * - firstReceiveTimestamp
  * - receiveCount
  **/
  4: optional map<string, string> attributes;

  /**
  * User-defined attributes attached to message
  **/
  5: optional map<string, Common.MessageAttribute> messageAttributes;
}

struct ChangeMessageVisibilityRequest {
  /**
  * Queue name;
  **/
  1: required string queueName;

  /**
  * receiptHandle for change visibility;
  **/
  2: required string receiptHandle;

  /**
  * The extra invisibilitySeconds for this message (0s ~ 12hour)
  **/
  3: required i32 invisibilitySeconds;
}

struct ChangeMessageVisibilityBatchRequestEntry {

  /**
  * receiptHandle for change visibility;
  **/
  1: required string receiptHandle;

  /**
  * The extra invisibilitySeconds for this message (0s ~ 12hour)
  **/
  2: required i32 invisibilitySeconds;
}

struct ChangeMessageVisibilityBatchRequest {
  /**
  * Queue name;
  **/
  1: required string queueName;

  /**
  * List of ChangeMessageVisibilityRequest;
  **/
  2: required list<ChangeMessageVisibilityBatchRequestEntry> changeMessageVisibilityRequestEntryList;
}

struct ChangeMessageVisibilityBatchResponse {
  /**
  * The successful receipt handle;
  **/
  1: list<string> successful;

  /**
  * Failed results list;
  * Using receipt handle to index
  **/
  2: list<MessageBatchErrorEntry> failed;
}

struct DeleteMessageRequest {
  /**
  * Queue name;
  **/
  1: required string queueName;

  /**
  * receipt handle of message to delete;
  **/
  2: required string receiptHandle;
}

struct DeleteMessageBatchRequestEntry {

  /**
  * receipt handle of message to delete;
  **/
  1: required string receiptHandle;
}

struct DeleteMessageBatchRequest {
  /**
  * Queue name;
  **/
  1: required string queueName;

  /**
  * List of DeleteMessageRequest;
  **/
  2: required list<DeleteMessageBatchRequestEntry> deleteMessageBatchRequestEntryList;
}

struct DeleteMessageBatchResponse {
  /**
  * The successful receipt handle;
  **/
  1: list<string> successful;

  /**
  * Failed results list;
  * Using receipt handle to index
  **/
  2: list<MessageBatchErrorEntry> failed;
}

service MessageService extends Common.EMQBaseService {
  /**
  * Send message;
  **/
  SendMessageResponse sendMessage(1: SendMessageRequest sendMessageRequest) throws (1: Common.GalaxyEmqServiceException e),

  /**
  * Send message batch;
  **/
  SendMessageBatchResponse sendMessageBatch(1: SendMessageBatchRequest sendMessageBatchRequest) throws (1: Common.GalaxyEmqServiceException e),

  /**
  * Receive message;
  **/
  list<ReceiveMessageResponse> receiveMessage(1: ReceiveMessageRequest receiveMessageRequest) throws (1: Common.GalaxyEmqServiceException e),

  /**
  * Change message invisibility seconds;
  **/
  void changeMessageVisibilitySeconds(1: ChangeMessageVisibilityRequest changeMessageVisibilityRequest) throws (1: Common.GalaxyEmqServiceException e),

  /**
  * Change message invisibility seconds batch;
  **/
  ChangeMessageVisibilityBatchResponse changeMessageVisibilitySecondsBatch(1: ChangeMessageVisibilityBatchRequest changeMessageVisibilityBatchRequest) throws (1:Common.GalaxyEmqServiceException e),

  /**
  * Delete message;
  **/
  void deleteMessage(1: DeleteMessageRequest deleteMessageRequest) throws (1: Common.GalaxyEmqServiceException e),

  /**
  * Delete message batch;
  **/
  DeleteMessageBatchResponse deleteMessageBatch(1: DeleteMessageBatchRequest deleteMessageBatchRequest) throws(1: Common.GalaxyEmqServiceException e),
}
