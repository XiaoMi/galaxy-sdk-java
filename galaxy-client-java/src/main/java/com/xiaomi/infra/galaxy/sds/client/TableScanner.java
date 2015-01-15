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
    private int maxSmallestLimitRetry = 5;
    private int maxScanRetry = 1000;
    private int scanRetry = 0;
    private int smallestLimitRetry = 0;
    private final static int SMALLEST_LIMIT = 1;
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
          if (scanRetry > 0) {
            // continue last unfinished scan request
            if (lastResult != null && lastResult.isThrottle()) {
              // throttle scan qps quota
              long pauseTime = ThrottleUtils.getPauseTime(ErrorCode.THROUGHPUT_EXCEED, scanRetry);
              ThrottleUtils.sleepPauseTime(pauseTime);
              lastPauseTime.set(pauseTime < 0 ? 0 : pauseTime);
            }
          } else {
            // start a new scan request
            scanRetry = 0;
            long pauseTime = ThrottleUtils.getPauseTime(lastPauseTime.get());
            ThrottleUtils.sleepPauseTime(pauseTime);
            lastPauseTime.set(pauseTime < 0 ? 0 : pauseTime);
          }

          // adjust scan limit
          if (lastResult != null) {
            int lastScannedRecords = Math
                .max(lastResult.getRecordsSize(), lastResult.getScannedRecords());
            int limit = ThrottleUtils
                .getAdaptiveScanLimit(lastScannedRecords, scan.getLimit());
            if (limit == SMALLEST_LIMIT) {
              if (scan.getLimit() == SMALLEST_LIMIT) {
                smallestLimitRetry++;
              } else {
                // init
                smallestLimitRetry = 1;
              }
            }
            if (smallestLimitRetry > maxSmallestLimitRetry) {
              throw new RuntimeException("Scan request " + scan + " with limit 1 failed at "
                  + smallestLimitRetry + " times");
            }
            scan.setLimit(limit);
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

          if ((result.getNextStartKey() == null || result.getNextStartKey().isEmpty())
              && result.isSuccess()) {
            // finish the whole scan request
            finished = true;
          } else {
            if (scan.getLimit() == result.getRecordsSize()) {
              // finish the current sub scan request
              scanRetry = 0;
              smallestLimitRetry = 0;
            } else {
              // two possible cases: qps quota exceeds or scan limit is too large
              scanRetry++;
              if (scanRetry > maxScanRetry) {
                throw new RuntimeException("Scan request " + scan + " failed with "
                    + scanRetry + " retries");
              }
            }
            lastResult = result;
            scan.setStartKey(result.getNextStartKey());
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
