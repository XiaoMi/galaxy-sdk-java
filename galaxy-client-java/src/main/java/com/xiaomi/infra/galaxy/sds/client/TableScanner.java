package com.xiaomi.infra.galaxy.sds.client;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import com.xiaomi.infra.galaxy.sds.thrift.Datum;
import com.xiaomi.infra.galaxy.sds.thrift.ErrorCode;
import com.xiaomi.infra.galaxy.sds.thrift.ScanRequest;
import com.xiaomi.infra.galaxy.sds.thrift.ScanResult;
import com.xiaomi.infra.galaxy.sds.thrift.TableService;
import com.xiaomi.infra.galaxy.sds.thrift.TableService.Iface;

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
    private int retry = 0;
    private final static int MAX_RETRY = 256;
    private Iterator<Map<String, Datum>> bufferIterator = null;
    private ScanResult lastResult = null;
    private ThreadLocal<Long> lastPauseTime = new ThreadLocal<Long>() {
      public Long initialValue() {
        return 0l;
      }
    };

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
          if (retry > 0) {
            // continue the last unfinished scan request
            if (lastResult != null && lastResult.isThrottled()) {
              // throttle scan qps quota
              long pauseTime = ThrottleUtils.getPauseTime(ErrorCode.THROUGHPUT_EXCEED, retry);
              ThrottleUtils.sleepPauseTime(pauseTime);
              lastPauseTime.set(pauseTime < 0 ? 0 : pauseTime);
            }
          } else {
            // start a new scan request
            assert retry == 0;
            long pauseTime = ThrottleUtils.getPauseTime(lastPauseTime.get());
            ThrottleUtils.sleepPauseTime(pauseTime);
            lastPauseTime.set(pauseTime < 0 ? 0 : pauseTime);
          }

          ScanResult result = null;
          try {
            result = tableClient.scan(scan);
          } catch (Throwable e) {
            throw new RuntimeException("Scan request " + scan + " failed", e);
          }

          if (result.getRecords() != null) {
            List<Map<String, Datum>> buffer = result.getRecords();
            bufferIterator = buffer.iterator();
          }

          if ((result.getNextStartKey() == null ||
              result.getNextStartKey().isEmpty()) &&
              result.getNextSplitIndex() == -1) {
            // finish the whole scan request
            finished = true;
          } else {
            if (result.getRecordsSize() < scan.getLimit() && result.isThrottled()) {
              // two possible cases: qps quota exceeds or scan limit is too large
              retry++;
              if (retry > MAX_RETRY) {
                throw new RuntimeException("Scan request " + scan + " failed with "
                    + retry + " retries");
              }
            } else {
              // finish the current sub scan request
              retry = 0;
            }
            lastResult = result;
            scan.setStartKey(result.getNextStartKey());
            if (result.getNextSplitIndex() > 0) {
              scan.setSplitIndex(result.getNextSplitIndex());
            }
          }
          return hasNext();
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
