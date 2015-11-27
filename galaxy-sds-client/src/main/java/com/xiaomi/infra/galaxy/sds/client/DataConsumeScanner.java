package com.xiaomi.infra.galaxy.sds.client;

import com.xiaomi.infra.galaxy.sds.thrift.DataConsumeRequest;
import com.xiaomi.infra.galaxy.sds.thrift.DataConsumeResult;
import com.xiaomi.infra.galaxy.sds.thrift.Datum;
import com.xiaomi.infra.galaxy.sds.thrift.ErrorCode;
import com.xiaomi.infra.galaxy.sds.thrift.TableService;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: qiankai@xiaomi.com
 */
public class DataConsumeScanner implements Iterable<DataConsumeScanner.InternalScanResult> {
  private final TableService.Iface tableClient;
  private final DataConsumeRequest dataConsumeRequest;

  public DataConsumeScanner(TableService.Iface tableClient,
      DataConsumeRequest dataConsumeRequest) {
    this.tableClient = tableClient;
    this.dataConsumeRequest = dataConsumeRequest;
  }

  @Override
  public Iterator<InternalScanResult> iterator() {
    return new RecordIterator(tableClient, dataConsumeRequest.deepCopy());
  }

  public class InternalScanResult {
    private Map<String, Datum> record;
    private Map<String, Datum> nextConsumeOffset;

    public InternalScanResult(Map<String, Datum> record,
        Map<String, Datum> nextConsumeOffset) {
      this.record = record;
      this.nextConsumeOffset = nextConsumeOffset;
    }

    public Map<String, Datum> getNextConsumeOffset() {
      return nextConsumeOffset;
    }

    public Map<String, Datum> getRecord() {
      return record;
    }

    @Override
    public String toString() {
      return "InternalScanResult{" +
          "record=" + record +
          ", nextConsumeOffset=" + nextConsumeOffset +
          '}';
    }
  }

  class RecordIterator implements Iterator<InternalScanResult> {
    private final TableService.Iface tableClient;
    private final DataConsumeRequest dataConsumeRequest;
    private boolean finished = false;
    private int retry = 0;
    private final static int MAX_RETRY = 100;
    private ListIterator<Map<String, Datum>> bufferIterator = null;
    private List<Map<String, Datum>> buffer = null;
    private DataConsumeResult lastResult = null;
    private ThreadLocal<Long> lastPauseTime = new ThreadLocal<Long>() {
      public Long initialValue() {
        return 0l;
      }
    };

    public RecordIterator(TableService.Iface tableClient, DataConsumeRequest dataConsumeRequest) {
      this.tableClient = tableClient;
      this.dataConsumeRequest = dataConsumeRequest;
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

          DataConsumeResult result = null;
          buffer = null;
          bufferIterator = null;
          try {
            result = tableClient.consumePartitionData(dataConsumeRequest);
          } catch (Throwable e) {
            throw new RuntimeException("Consume data of partition " +
                dataConsumeRequest.getPartitionId() + " failed, edit " +
                "consume request is " + dataConsumeRequest, e);
          }

          lastResult = result;
          if (result.getRecords() != null) {
            buffer = result.getRecords();

            // use list iterator to get next record as committed receipt
            bufferIterator = buffer.listIterator();
          }

          if ((result.getNextConsumeOffset() == null
              || result.getNextConsumeOffset().isEmpty())) {
            // finish the whole scan request
            finished = true;
          } else {
            if (dataConsumeRequest.getConsumeNumber() == result.getRecordsSize()) {
              // finish the current sub scan request
              retry = 0;
            } else {
              // two possible cases: qps quota exceeds or scan limit is too large
              retry++;
              if (retry > MAX_RETRY) {
                throw new RuntimeException("Consume data of partition " +
                    dataConsumeRequest.getPartitionId() + " failed with " +
                    retry + " retries, data consume request is " +
                    dataConsumeRequest);
              }
            }
            dataConsumeRequest.setConsumeOffset(result.getNextConsumeOffset());
          }
          return hasNext();
        }
      }
    }

    @Override
    public InternalScanResult next() throws NoSuchElementException {
      if (hasNext()) {
        Map<String, Datum> nextConsumeOffset = null;
        Map<String, Datum> record = bufferIterator.next();
        int nextIndex = bufferIterator.nextIndex();
        if (nextIndex == buffer.size()) {
          nextConsumeOffset = lastResult.getNextConsumeOffset();
        } else {
          nextConsumeOffset = getRecordKey(buffer.get(nextIndex),
              lastResult.getKeys());
        }
        return new InternalScanResult(record, nextConsumeOffset);
      } else {
        throw new NoSuchElementException("Scanner reaches the end");
      }
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("Remove is not supported");
    }

    private Map<String, Datum> getRecordKey(Map<String, Datum> record,
        List<String> key) {
      Map<String, Datum> recordKey = new HashMap<String, Datum>(key.size());
      for (String entry : key) {
        recordKey.put(entry, record.get(entry));
      }
      return recordKey;
    }
  }
}
