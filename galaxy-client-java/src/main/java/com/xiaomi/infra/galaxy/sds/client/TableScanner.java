package com.xiaomi.infra.galaxy.sds.client;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import com.xiaomi.infra.galaxy.sds.thrift.Datum;
import com.xiaomi.infra.galaxy.sds.thrift.ErrorCode;
import com.xiaomi.infra.galaxy.sds.thrift.ErrorsConstants;
import com.xiaomi.infra.galaxy.sds.thrift.ScanRequest;
import com.xiaomi.infra.galaxy.sds.thrift.ScanResult;
import com.xiaomi.infra.galaxy.sds.thrift.ServiceException;
import com.xiaomi.infra.galaxy.sds.thrift.TableService;
import com.xiaomi.infra.galaxy.sds.thrift.TableService.Iface;
import libthrift091.TException;

public class TableScanner implements Iterable<Map<String, Datum>> {
  private final TableService.Iface tableClient;
  private final ScanRequest scan;

  public TableScanner(Iface tableClient, ScanRequest scan) {
    this.tableClient = tableClient;
    this.scan = scan;
  }

  @Override
  public Iterator<Map<String, Datum>> iterator() {
    return new RecordIterator(tableClient, scan.deepCopy());
  }

  class RecordIterator implements Iterator<Map<String, Datum>> {
    private final TableService.Iface tableClient;
    private final ScanRequest scan;
    private boolean finished = false;
    private long mayRetry = 0;
    private long baseWaitTime = 100;
    private Iterator<Map<String, Datum>> bufferIterator = null;

    public RecordIterator(Iface tableClient, ScanRequest scan) {
      this.tableClient = tableClient;
      this.scan = scan;
    }

    @Override
    public boolean hasNext() {
      if (bufferIterator != null && bufferIterator.hasNext()) {
        return true;
      } else {
        if (finished) {
          return false;
        } else {
          ScanResult result = null;
          if (mayRetry > 0) {
            ThrottleUtils.sleepPauseTime(baseWaitTime << (mayRetry - 1));
          }

          while (true) {
            try {
              result = tableClient.scan(scan);
            } catch (Throwable e) {
              throw new RuntimeException("Scan request " + scan + " failed to scan table", e);
            }
            break;
          }

          if (result.getRecords() == null) {
            throw new IllegalStateException(
                "Scan terminated due to illegal state, the returned records is not set: " + result);
          } else {
            List<Map<String, Datum>> buffer = result.getRecords();
            bufferIterator = buffer.iterator();
          }

          if (result.getNextStartKey() == null || result.getNextStartKey().isEmpty()) {
            finished = true;
            mayRetry = 0;
          } else {
            if (result.getRecords().isEmpty()) {
              throw new IllegalStateException(
                  "Scan terminated due to illegal state, scanner returns empty records set but "
                      + "not marked as finished, this may cause infinate loop: " + result);
            }
            if (scan.getLimit() == result.getRecordsSize()) {
              mayRetry = 0;
            } else {
              mayRetry++;
            }
            scan.setStartKey(result.getNextStartKey());
          }

          return hasNext(); // at most recurse once
        }
      }
    }

    @Override
    public Map<String, Datum> next() throws NoSuchElementException {
      if (hasNext()) {
        return bufferIterator.next();
      } else {
        throw new NoSuchElementException("Scanner reaches the end");
      }
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("remove is not supported");
    }
  }
}
