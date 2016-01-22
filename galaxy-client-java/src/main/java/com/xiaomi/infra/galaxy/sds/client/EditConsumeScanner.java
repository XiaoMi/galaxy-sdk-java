package com.xiaomi.infra.galaxy.sds.client;

import com.xiaomi.infra.galaxy.sds.thrift.EditConsumeRequest;
import com.xiaomi.infra.galaxy.sds.thrift.EditConsumeResult;
import com.xiaomi.infra.galaxy.sds.thrift.ErrorCode;
import com.xiaomi.infra.galaxy.sds.thrift.RowEdit;
import com.xiaomi.infra.galaxy.sds.thrift.TableService;

import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;

/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: qiankai@xiaomi.com
 */
public class EditConsumeScanner implements Iterable<EditConsumeScanner.InternalScanResult> {
  private final TableService.Iface tableClient;
  private final EditConsumeRequest editConsumeRequest;

  public EditConsumeScanner(TableService.Iface tableClient,
      EditConsumeRequest editConsumeRequest) {
    this.tableClient = tableClient;
    this.editConsumeRequest = editConsumeRequest;
  }

  @Override
  public Iterator<InternalScanResult> iterator() {
    return new RecordIterator(tableClient, editConsumeRequest.deepCopy());
  }

  public class InternalScanResult {
    private RowEdit rowEdit;
    private long nextConsumeOffset;

    public InternalScanResult(RowEdit rowEdit, long nextConsumeOffset) {
      this.rowEdit = rowEdit;
      this.nextConsumeOffset = nextConsumeOffset;
    }

    public long getNextConsumeOffset() {
      return nextConsumeOffset;
    }

    public RowEdit getRowEdit() {
      return rowEdit;
    }

    @Override
    public String toString() {
      return "InternalScanResult{" +
          "rowEdit=" + rowEdit +
          ", nextConsumeOffset=" + nextConsumeOffset +
          '}';
    }
  }

  class RecordIterator implements Iterator<InternalScanResult> {
    private final TableService.Iface tableClient;
    private final EditConsumeRequest editConsumeRequest;
    private boolean finished = false;
    private int retry = 0;
    private final static int MAX_RETRY = 100;
    private ListIterator<RowEdit> bufferIterator = null;
    private List<RowEdit> buffer = null;
    private EditConsumeResult lastResult = null;
    private ThreadLocal<Long> lastPauseTime = new ThreadLocal<Long>() {
      public Long initialValue() {
        return 0l;
      }
    };

    public RecordIterator(TableService.Iface tableClient,
        EditConsumeRequest editConsumeRequest) {
      this.tableClient = tableClient;
      this.editConsumeRequest = editConsumeRequest;
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

          EditConsumeResult result = null;
          buffer = null;
          bufferIterator = null;
          try {
            result = tableClient.consumePartitionEdit(editConsumeRequest);
          } catch (Throwable e) {
            throw new RuntimeException("Consume edits of partition " +
                editConsumeRequest.getPartitionId() + " failed, edit " +
                "consume request is " + editConsumeRequest, e);
          }

          lastResult = result;
          if (result.getRowEdits() != null) {
            buffer = result.getRowEdits();
            bufferIterator = buffer.listIterator();
          }

          if (result.getRowEditsSize() == 0) {
            /*
            if (result.getRowEditsSize() != 0) {
              throw new RuntimeException("Expected received row edits are zero, but " +
                  "actual size is " + result.getRowEditsSize());
            }
            */
            // finish the whole edit consume request
            finished = true;
          } else {
            if (editConsumeRequest.getConsumeNumber() == result.getRowEditsSize()) {
              // finish the current sub scan request
              retry = 0;
            } else {
              // two possible cases: qps quota exceeds or scan limit is too large
              retry++;
              if (retry > MAX_RETRY) {
                throw new RuntimeException("Consume edits of partition " +
                    editConsumeRequest.getPartitionId() + " failed with " +
                    retry + " retries, edit consume request is " +
                    editConsumeRequest + ", edit consume result is " +
                    result);
              }
            }
            editConsumeRequest.setConsumeOffset(result.getNextConsumeOffset());
          }
          return hasNext();
        }
      }
    }

    @Override
    public InternalScanResult next() throws NoSuchElementException {
      if (hasNext()) {
        long nextConsumeOffset;
        RowEdit record = bufferIterator.next();
        int nextIndex = bufferIterator.nextIndex();
        if (nextIndex == buffer.size()) {
          nextConsumeOffset = lastResult.getNextConsumeOffset();
        } else {
          nextConsumeOffset = buffer.get(nextIndex).getConsumeOffset();
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
  }
}
