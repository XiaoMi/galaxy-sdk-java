namespace java com.xiaomi.infra.galaxy.lcs.thrift

enum ErrorCode {
  SUCCESS = 0,
  UNEXCEPTED_ERROR = 1,
  BUFFER_FULL = 2,
  DESERIALIZE_FAILED = 3,
  FILE_NOT_EXIST = 4,
  IO_ERROR = 5,
  TALOS_OPERATION_FAILED = 6,
  INVALID_TOPIC = 7,
  AGENT_NOT_READY = 8,
  MODULED_STOPED = 9,
}

exception GalaxyLCSException {
  1: required ErrorCode errorCode;

  2: optional string errMsg;

  3: optional string details;
}


struct Record {
  1: required string topicName;
  2: required list<binary> data;
}

service LCSThriftService {
  void Record(1: Record record) throws (1: GalaxyLCSException e);
}