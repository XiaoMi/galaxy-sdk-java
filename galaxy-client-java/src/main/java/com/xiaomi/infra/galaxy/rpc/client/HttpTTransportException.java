package com.xiaomi.infra.galaxy.rpc.client;

import com.xiaomi.infra.galaxy.rpc.thrift.ErrorCode;
import com.xiaomi.infra.galaxy.rpc.thrift.HttpStatusCode;
import libthrift091.transport.TTransportException;


public class HttpTTransportException extends TTransportException {
  private static final long serialVersionUID = -2806473721346391229L;
  private final int statusCode;
  private final ErrorCode errorCode;
  private final String errorMessage;

  public HttpTTransportException(int httpStatusCode, String errorMessage) {
    super("HTTP transport error, status code " + httpStatusCode + ", error code "
        + convertHtpCode(httpStatusCode) + ": " + errorMessage);
    this.statusCode = httpStatusCode;
    this.errorCode = convertHtpCode(httpStatusCode);
    this.errorMessage = errorMessage;
  }

  public int getStatusCode() {
    return statusCode;
  }

  public ErrorCode getErrorCode() {
    return errorCode;
  }

  public String getErorMessage() {
    return errorMessage;
  }

  private static ErrorCode convertHtpCode(int httpCode) {
    HttpStatusCode code = HttpStatusCode.findByValue(httpCode);
    if (code != null) {
      switch (code) {
      case INVALID_AUTH:
        return ErrorCode.INVALID_AUTH;
      case CLOCK_TOO_SKEWED:
        return ErrorCode.CLOCK_TOO_SKEWED;
      case REQUEST_TOO_LARGE:
        return ErrorCode.REQUEST_TOO_LARGE;
      default:
        return ErrorCode.UNKNOWN;
      }
    } else {
      return ErrorCode.UNKNOWN;
    }
  }
}
