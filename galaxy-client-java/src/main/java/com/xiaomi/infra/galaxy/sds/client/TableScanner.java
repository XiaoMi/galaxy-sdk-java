package com.xiaomi.infra.galaxy.sds.client;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import com.xiaomi.infra.galaxy.sds.thrift.Datum;
import com.xiaomi.infra.galaxy.sds.thrift.ScanRequest;
import com.xiaomi.infra.galaxy.sds.thrift.ScanResult;
import com.xiaomi.infra.galaxy.sds.thrift.ServiceException;
import com.xiaomi.infra.galaxy.sds.thrift.TableService;
import com.xiaomi.infra.galaxy.sds.thrift.TableService.Iface;
import libthrift091.TException;

public class TableScanner implements Iterable<Map<String, Datum>> {
  private final TableService.Iface tableClient;
  private final ScanRequest scan;
  private final long mayRetryWhenTimeout;

  public TableScanner(Iface tableClient, ScanRequest scan, long mayRetryWhenTimeout) {
    this.tableClient = tableClient;
    this.scan = scan;
    this.mayRetryWhenTimeout = mayRetryWhenTimeout;
  }

  public TableScanner(Iface tableClient, ScanRequest scan) {
    // default retry 3 times at most
    this(tableClient, scan, 3);
  }

  @Override
  public Iterator<Map<String, Datum>> iterator() {
    return new RecordIterator(tableClient, scan.deepCopy(), mayRetryWhenTimeout);
  }

  class RecordIterator implements Iterator<Map<String, Datum>> {
    private final TableService.Iface tableClient;
    private final ScanRequest scan;
    private final long mayRetryWhenTimeout;
    private boolean finished = false;
    private long mayRetry = 0;
    private long baseWaitTime = 100;
    private Iterator<Map<String, Datum>> bufferIterator = null;

    public RecordIterator(Iface tableClient, ScanRequest scan, long mayRetryWhenTimeout) {
      this.tableClient = tableClient;
      this.scan = scan;
      this.mayRetryWhenTimeout = mayRetryWhenTimeout;
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
          try {
            if (mayRetry > 0) {
              Thread.sleep(baseWaitTime << (mayRetry - 1));
            }
          } catch (InterruptedException ie) {
            throw new RuntimeException("thread sleep failed", ie);
          }

          long retry = 0;
          while (true) {
            try {
              result = tableClient.scan(scan);
            } catch (ServiceException se) {
              throw new RuntimeException("failed to scan table", se);
            } catch (TException te) {
              // retry 3 times at most
              if (retry < mayRetryWhenTimeout) {
                retry++;
                continue;
              } else {
                throw new RuntimeException("failed to scan table", te);
              }
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
