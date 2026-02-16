package com.open.messaging.bigbro.broker.keeper.ledger;

import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCountUtil;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import org.apache.bookkeeper.client.AsyncCallback.AddCallback;
import org.apache.bookkeeper.client.AsyncCallback.CloseCallback;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.api.BKException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Handles the lifecycle of a single add entry operation. */
public class AddEntryOperation implements AddCallback, CloseCallback, Runnable {

  private static final Logger log = LoggerFactory.getLogger(AddEntryOperation.class);

  public enum State {
    OPEN,
    INITIATED,
    COMPLETED,
    CLOSED
  }

  private static final AtomicReferenceFieldUpdater<AddEntryOperation, State> STATE_UPDATER =
      AtomicReferenceFieldUpdater.newUpdater(AddEntryOperation.class, State.class, "state");
  private static final AtomicReferenceFieldUpdater<AddEntryOperation, AddEntryCallback>
      CALLBACK_UPDATER =
          AtomicReferenceFieldUpdater.newUpdater(
              AddEntryOperation.class, AddEntryCallback.class, "callback");
  private static final AtomicLongFieldUpdater<AddEntryOperation> ADD_OP_COUNT_UPDATER =
      AtomicLongFieldUpdater.newUpdater(AddEntryOperation.class, "addOpCount");

  private ManagedLedgerImpl managedLedger;
  private LedgerHandle ledger;
  private long entryId = -1;
  private int numberOfMessages = 1;
  private volatile AddEntryCallback callback;
  private Object ctx;
  private volatile long addOpCount;
  private boolean closeWhenDone;
  private long startTime;
  private long lastInitTime;
  private ByteBuf data;
  private int dataLength;
  private volatile State state;
  private AtomicBoolean timeoutTriggered;

  private AddEntryOperation() {}

  public static AddEntryOperation create(
      ManagedLedgerImpl ml,
      ByteBuf data,
      int numberOfMessages,
      AddEntryCallback callback,
      Object ctx,
      AtomicBoolean timeoutTriggered) {
    AddEntryOperation op = new AddEntryOperation();
    op.managedLedger = ml;
    op.data = data;
    op.dataLength = data.readableBytes();
    op.callback = callback;
    op.ctx = ctx;
    op.numberOfMessages = numberOfMessages;
    op.addOpCount = ml.incrementAddOpCount();
    op.startTime = System.nanoTime();
    op.lastInitTime = -1; // Not initiated yet, will be set in initiate()
    op.state = State.OPEN;
    op.timeoutTriggered = timeoutTriggered;
    return op;
  }

  public void setLedger(LedgerHandle ledger) {
    this.ledger = ledger;
  }

  public void setCloseWhenDone(boolean closeWhenDone) {
    this.closeWhenDone = closeWhenDone;
  }

  public void setTimeoutTriggered(AtomicBoolean t) {
    this.timeoutTriggered = t;
  }

  public void initiate() {
    if (!STATE_UPDATER.compareAndSet(this, State.OPEN, State.INITIATED)) return;
    ByteBuf dup = data.retainedDuplicate();
    lastInitTime = System.nanoTime();
    try {
      ledger.asyncAddEntry(dup, this, addOpCount);
    } catch (Exception e) {
      ReferenceCountUtil.safeRelease(dup);
      failed(new ManagedLedgerException.WriteFailedException("Failed to initiate", e));
    }
  }

  public void failed(ManagedLedgerException e) {
    AddEntryCallback cb = CALLBACK_UPDATER.getAndSet(this, null);
    if (cb != null) {
      ReferenceCountUtil.release(data);
      cb.addFailed(e, ctx);
    }
  }

  @Override
  public void addComplete(int rc, LedgerHandle lh, long entryId, Object ctx) {
    State currentState = this.state;
    boolean casSuccess = STATE_UPDATER.compareAndSet(this, State.INITIATED, State.COMPLETED);

    if (!casSuccess) {
      log.warn(
          "[{}] addComplete() CAS FAILED: currentState={}, expected=INITIATED, "
              + "ledger={}, entryId={}, addOpCount={}, ctx={}",
          managedLedger.getName(),
          currentState,
          lh != null ? lh.getId() : "null",
          entryId,
          addOpCount,
          ctx);
      return; // ← Should prevent closed ops from continuing!
    }

    long expected = (ctx instanceof Long) ? (long) ctx : -1;
    boolean countCasSuccess = ADD_OP_COUNT_UPDATER.compareAndSet(this, expected, -1);

    if (expected == -1 || !countCasSuccess) {
      log.warn(
          "[{}] addComplete() COUNT CAS FAILED: expected={}, this.addOpCount={}, "
              + "casSuccess={}, ledger={}, entryId={}",
          managedLedger.getName(),
          expected,
          addOpCount,
          countCasSuccess,
          lh != null ? lh.getId() : "null",
          entryId);
      return;
    }

    log.info(
        "[{}] addComplete() SUCCESS: ledger={}, entryId={}, addOpCount={}, state=COMPLETED",
        managedLedger.getName(),
        lh != null ? lh.getId() : "null",
        entryId,
        expected);

    this.entryId = entryId;
    if (rc != BKException.Code.OK || (timeoutTriggered != null && timeoutTriggered.get())) {
      handleAddFailure(lh, rc);
    } else {
      managedLedger.getExecutor().execute(this);
    }
  }

  @Override
  public void run() {

    AddEntryOperation first = managedLedger.pollPendingAddEntry();

    if (first != null) {
      log.info(
          "[{}] run() POLLED: first.addOpCount={}, first.ledger={}, match={}, queueSizeAfter={}",
          managedLedger.getName(),
          first.addOpCount,
          first.ledger != null ? first.ledger.getId() : "null",
          this == first,
          managedLedger.getPendingAddEntriesCount());
    } else {
      log.warn(
          "[{}] run() POLLED NULL! this.addOpCount={}, queueSize={}",
          managedLedger.getName(),
          addOpCount,
          managedLedger.getPendingAddEntriesCount());
    }
    if (first == null || this != first) {
      if (first != null) first.failed(new ManagedLedgerException("Op mismatch"));
      return;
    }
    managedLedger.incrementEntryCount();
    managedLedger.addToTotalSize(dataLength);
    Position pos = Position.create(ledger != null ? ledger.getId() : -1, entryId);
    managedLedger.updateLastConfirmedEntry(pos);
    if (closeWhenDone && ledger != null) {
      ledger.asyncClose(this, ctx);
    } else {
      completeCallback(pos);
    }
  }

  @Override
  public void closeComplete(int rc, LedgerHandle lh, Object ctx) {
    managedLedger.ledgerClosed(lh);
    completeCallback(Position.create(lh.getId(), entryId));
  }

  private void completeCallback(Position pos) {
    managedLedger.recordAddEntryLatency(System.nanoTime() - startTime, TimeUnit.NANOSECONDS);
    AddEntryCallback cb = CALLBACK_UPDATER.getAndSet(this, null);
    if (cb != null) {
      cb.addComplete(pos, data.asReadOnly(), ctx);
      managedLedger.notifyWaitingCallbacks();
    }
    ReferenceCountUtil.release(data);
  }

  void handleAddFailure(LedgerHandle lh, int rc) {
    managedLedger
        .getExecutor()
        .execute(
            () -> {
              if (rc == BKException.Code.LedgerFencedException
                  || rc == BKException.Code.LedgerClosedException)
                managedLedger.handleLedgerFenced(lh);
              else managedLedger.ledgerClosed(lh);
            });
  }

  public State getState() {
    return state;
  }

  public ByteBuf getData() {
    return data;
  }

  public int getDataLength() {
    return dataLength;
  }

  public int getNumberOfMessages() {
    return numberOfMessages;
  }

  public LedgerHandle getLedger() {
    return ledger;
  }

  public long getLastInitTime() {
    return lastInitTime;
  }

  public long getEntryId() {
    return entryId;
  }

  /**
   * Creates a fresh copy of this operation with state=OPEN and closes this one. Used during ledger
   * rotation in createNewOpAddEntryForNewLedger when an op has already been initiated on the old
   * ledger and needs to be re-done.
   */
  public AddEntryOperation duplicateAndClose(AtomicBoolean newTimeoutTriggered) {
    STATE_UPDATER.set(this, State.CLOSED);
    AddEntryOperation dup = new AddEntryOperation();
    dup.managedLedger = this.managedLedger;
    dup.data = this.data;
    dup.dataLength = this.dataLength;
    dup.callback = this.callback;
    dup.ctx = this.ctx;
    dup.numberOfMessages = this.numberOfMessages;
    dup.addOpCount = managedLedger.incrementAddOpCount();
    dup.startTime = System.nanoTime();
    dup.lastInitTime = -1;
    dup.state = State.OPEN;
    dup.timeoutTriggered = newTimeoutTriggered;
    return dup;
  }
}
