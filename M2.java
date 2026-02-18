package com.open.messaging.bigbro.broker.keeper.ledger;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import org.apache.bookkeeper.client.AsyncCallback.CreateCallback;
import org.apache.bookkeeper.client.BKException.Code;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.api.BKException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Implementation of ManagedLedger following Apache Pulsar's ManagedLedgerImpl pattern exactly. */
public class ManagedLedgerImpl implements ManagedLedger, CreateCallback {

  private static final Logger log = LoggerFactory.getLogger(ManagedLedgerImpl.class);

  protected static final int AsyncOperationTimeoutSeconds = 30;

  protected final BookKeeper bookKeeper;
  protected final String name;
  protected ManagedLedgerConfig config;

  protected final NavigableMap<Long, LedgerInfo> ledgers = new ConcurrentSkipListMap<>();

  // Pending addEntry requests
  final ConcurrentLinkedQueue<AddEntryOperation> pendingAddEntries = new ConcurrentLinkedQueue<>();

  // Objects that are waiting to be notified when new entries are persisted
  final ConcurrentLinkedQueue<Runnable> waitingEntryCallBacks = new ConcurrentLinkedQueue<>();

  private ScheduledFuture<?> timeoutTask;

  protected volatile LedgerHandle currentLedger;
  protected volatile long currentLedgerEntries = 0;
  protected volatile long currentLedgerSize = 0;
  protected volatile long lastLedgerCreatedTimestamp = 0;
  private volatile long lastLedgerCreationFailureTimestamp = 0;
  private long lastLedgerCreationInitiationTimestamp = 0;

  volatile Position lastConfirmedEntry;

  protected volatile long lastAddEntryTimeMs = 0;

  /**
   * A signal that may trigger all the subsequent OpAddEntry of current ledger to be failed due to
   * timeout.
   */
  protected volatile AtomicBoolean currentLedgerTimeoutTriggered;

  // State updater
  protected static final AtomicReferenceFieldUpdater<ManagedLedgerImpl, State> STATE_UPDATER =
      AtomicReferenceFieldUpdater.newUpdater(ManagedLedgerImpl.class, State.class, "state");

  static final AtomicLongFieldUpdater<ManagedLedgerImpl> NUMBER_OF_ENTRIES_UPDATER =
      AtomicLongFieldUpdater.newUpdater(ManagedLedgerImpl.class, "numberOfEntries");

  @SuppressWarnings("unused")
  private volatile long numberOfEntries = 0;

  static final AtomicLongFieldUpdater<ManagedLedgerImpl> TOTAL_SIZE_UPDATER =
      AtomicLongFieldUpdater.newUpdater(ManagedLedgerImpl.class, "totalSize");

  @SuppressWarnings("unused")
  private volatile long totalSize = 0;

  static final AtomicLongFieldUpdater<ManagedLedgerImpl> ADD_OP_COUNT_UPDATER =
      AtomicLongFieldUpdater.newUpdater(ManagedLedgerImpl.class, "addOpCount");

  @SuppressWarnings("unused")
  private volatile long addOpCount = 0;

  protected volatile State state = null;

  protected final ScheduledExecutorService scheduledExecutor;
  protected final Executor executor;
  private final ManagedLedgerListener listener;

  public enum State {
    None, // Uninitialized
    LedgerOpened, // A ledger is ready to write into
    ClosingLedger, // Closing current ledger
    ClosedLedger, // Current ledger has been closed and there's no pending operation
    CreatingLedger, // Creating a new ledger
    Closed, // ManagedLedger has been closed
    Fenced, // A managed ledger is fenced when there is some concurrent access
    Terminated, // Managed ledger was terminated and no more entries are allowed
    WriteFailed; // The state that is transitioned to when a BK write failure happens

    public boolean isFenced() {
      return this == Fenced;
    }
  }

  public ManagedLedgerImpl(
      BookKeeper bookKeeper,
      ManagedLedgerConfig config,
      ScheduledExecutorService scheduledExecutor,
      String name) {
    this(bookKeeper, config, scheduledExecutor, name, null);
  }

  public ManagedLedgerImpl(
      BookKeeper bookKeeper,
      ManagedLedgerConfig config,
      ScheduledExecutorService scheduledExecutor,
      String name,
      ManagedLedgerListener listener) {
    this.bookKeeper = bookKeeper;
    this.config = config;
    this.name = name;
    this.scheduledExecutor = scheduledExecutor;
    this.executor = bookKeeper.getMainWorkerPool().chooseThread(name);
    this.listener = listener;
    TOTAL_SIZE_UPDATER.set(this, 0);
    NUMBER_OF_ENTRIES_UPDATER.set(this, 0);
    STATE_UPDATER.set(this, State.None);
  }

  /** Initializes the managed ledger by creating the first ledger. */
  public CompletableFuture<Void> initialize() {
    CompletableFuture<Void> future = new CompletableFuture<>();

    log.info("[{}] Opening managed ledger", name);
    STATE_UPDATER.set(this, State.CreatingLedger);
    this.lastLedgerCreationInitiationTimestamp = System.currentTimeMillis();

    // Use the asyncCreateLedger helper (supports timeout via checkAndCompleteLedgerOpTask)
    asyncCreateLedger(
        bookKeeper,
        config,
        (rc, lh, ctx) -> {
          if (checkAndCompleteLedgerOpTask(rc, lh, ctx)) {
            return;
          }
          // Dispatch to the partition executor — same thread that runs asyncAddEntry,
          // so no ops can interleave between state change and drain.
          executor.execute(
              () -> {
                if (rc != Code.OK) {
                  log.error(
                      "[{}] Error creating ledger rc={} {}", name, rc, BKException.getMessage(rc));
                  STATE_UPDATER.set(this, State.Closed);
                  lastLedgerCreationFailureTimestamp = System.currentTimeMillis();
                  future.completeExceptionally(
                      new ManagedLedgerException.LedgerCreationException(
                          rc, "Failed to create initial ledger: " + BKException.getMessage(rc)));
                  return;
                }

                log.info("[{}] Created new ledger {}", name, lh.getId());

                synchronized (ManagedLedgerImpl.this) {
                  ledgers.put(lh.getId(), new LedgerInfo(lh.getId(), 0, 0));
                  currentLedger = lh;
                  currentLedgerTimeoutTriggered = new AtomicBoolean();
                  currentLedgerEntries = 0;
                  currentLedgerSize = 0;
                  lastLedgerCreatedTimestamp = System.currentTimeMillis();
                  // Drain any ops that accumulated during CreatingLedger state,
                  // assigns them the new ledger and initiates each one.
                  updateLedgersIdsComplete(null);
                }

                scheduleTimeoutTask();
                notifyLedgerCreated(lh.getId());
                future.complete(null);
              });
        });

    return future;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public LedgerState getState() {
    State s = STATE_UPDATER.get(this);
    if (s == null) return LedgerState.NONE;
    switch (s) {
      case None:
        return LedgerState.NONE;
      case LedgerOpened:
        return LedgerState.LEDGER_OPENED;
      case ClosingLedger:
        return LedgerState.CLOSING_LEDGER;
      case ClosedLedger:
        return LedgerState.CLOSED_LEDGER;
      case CreatingLedger:
        return LedgerState.CREATING_LEDGER;
      case Closed:
        return LedgerState.CLOSED;
      case Fenced:
        return LedgerState.FENCED;
      case Terminated:
        return LedgerState.TERMINATED;
      case WriteFailed:
        return LedgerState.WRITE_FAILED;
      default:
        return LedgerState.NONE;
    }
  }

  @Override
  public ManagedLedgerConfig getConfig() {
    return config;
  }

  @Override
  public Position addEntry(byte[] data) throws ManagedLedgerException, InterruptedException {
    return addEntry(data, 0, data.length);
  }

  @Override
  public Position addEntry(byte[] data, int offset, int length)
      throws ManagedLedgerException, InterruptedException {
    CountDownLatch latch = new CountDownLatch(1);
    Position[] result = new Position[1];
    ManagedLedgerException[] error = new ManagedLedgerException[1];

    asyncAddEntry(
        Unpooled.wrappedBuffer(data, offset, length),
        new AddEntryCallback() {
          @Override
          public void addComplete(Position position, ByteBuf entryData, Object ctx) {
            result[0] = position;
            latch.countDown();
          }

          @Override
          public void addFailed(ManagedLedgerException exception, Object ctx) {
            error[0] = exception;
            latch.countDown();
          }
        },
        null);

    if (!latch.await(AsyncOperationTimeoutSeconds, TimeUnit.SECONDS)) {
      throw new ManagedLedgerException.OperationTimeoutException("Timeout during add entry");
    }
    if (error[0] != null) throw error[0];
    return result[0];
  }

  @Override
  public void asyncAddEntry(byte[] data, AddEntryCallback callback, Object ctx) {
    asyncAddEntry(Unpooled.wrappedBuffer(data), callback, ctx);
  }

  @Override
  public CompletableFuture<Position> asyncAddEntry(byte[] data) {
    return asyncAddEntry(Unpooled.wrappedBuffer(data));
  }

  @Override
  public CompletableFuture<Position> asyncAddEntry(ByteBuf buffer) {
    CompletableFuture<Position> future = new CompletableFuture<>();
    asyncAddEntry(
        buffer,
        new AddEntryCallback() {
          @Override
          public void addComplete(Position pos, ByteBuf data, Object ctx) {
            future.complete(pos);
          }

          @Override
          public void addFailed(ManagedLedgerException e, Object ctx) {
            future.completeExceptionally(e);
          }
        },
        null);
    return future;
  }

  @Override
  public void asyncAddEntry(ByteBuf buffer, AddEntryCallback callback, Object ctx) {
    if (log.isDebugEnabled()) {
      log.debug("[{}] asyncAddEntry size={} state={}", name, buffer.readableBytes(), state);
    }

    // retain buffer in this thread
    buffer.retain();

    // Jump to specific thread to avoid contention from writers writing from different threads
    executor.execute(
        () -> {
          AddEntryOperation addOperation =
              AddEntryOperation.create(
                  this, buffer, 1, callback, ctx, currentLedgerTimeoutTriggered);
          internalAsyncAddEntry(addOperation);
        });
  }

  /** Follows Pulsar's internalAsyncAddEntry exactly. */
  protected synchronized void internalAsyncAddEntry(AddEntryOperation addOperation) {
    final State state = STATE_UPDATER.get(this);
    if (state.isFenced()) {
      addOperation.failed(new ManagedLedgerException.ManagedLedgerFencedException());
      return;
    } else if (state == State.Terminated) {
      addOperation.failed(
          new ManagedLedgerException.ManagedLedgerTerminatedException(
              "Managed ledger was already terminated"));
      return;
    } else if (state == State.Closed) {
      addOperation.failed(
          new ManagedLedgerException.ManagedLedgerClosedException(
              "Managed ledger was already closed"));
      return;
    } else if (state == State.WriteFailed) {
      addOperation.failed(
          new ManagedLedgerException.ManagedLedgerClosedException(
              "Waiting to recover from failure"));
      return;
    }

    pendingAddEntries.add(addOperation);

    if (state == State.ClosingLedger || state == State.CreatingLedger) {
      // We don't have a ready ledger to write into
      // We are waiting for a new ledger to be created
      if (log.isDebugEnabled()) {
        log.debug("[{}] Queue addEntry request", name);
      }
      if (State.CreatingLedger == state) {
        long elapsedMs = System.currentTimeMillis() - this.lastLedgerCreationInitiationTimestamp;
        if (elapsedMs > TimeUnit.SECONDS.toMillis(2 * config.getOperationTimeout().getSeconds())) {
          log.info(
              "[{}] Ledger creation was initiated {} ms ago but it never completed and creation timeout"
                  + " task didn't kick in as well. Force to fail the create ledger operation.",
              name,
              elapsedMs);
          this.createComplete(Code.TimeoutException, null, null);
        }
      }
    } else if (state == State.ClosedLedger) {
      // No ledger and no pending operations. Create a new ledger
      if (STATE_UPDATER.compareAndSet(this, State.ClosedLedger, State.CreatingLedger)) {
        log.info("[{}] Creating a new ledger", name);
        this.lastLedgerCreationInitiationTimestamp = System.currentTimeMillis();
        asyncCreateLedger(bookKeeper, config, this);
      }
    } else {
      // state == State.LedgerOpened
      // Write into lastLedger
      addOperation.setLedger(currentLedger);
      addOperation.setTimeoutTriggered(currentLedgerTimeoutTriggered);

      ++currentLedgerEntries;
      currentLedgerSize += addOperation.getDataLength();

      if (log.isDebugEnabled()) {
        log.debug(
            "[{}] Write into current ledger lh={} entries={}",
            name,
            currentLedger.getId(),
            currentLedgerEntries);
      }

      if (currentLedgerIsFull()) {
        if (log.isDebugEnabled()) {
          log.debug("[{}] Closing current ledger lh={}", name, currentLedger.getId());
        }
        // This entry will be the last added to current ledger
        addOperation.setCloseWhenDone(true);
        STATE_UPDATER.set(this, State.ClosingLedger);
      }
      addOperation.initiate();
    }
    // mark add entry activity
    lastAddEntryTimeMs = System.currentTimeMillis();
  }

  /** BookKeeper create callback - follows Pulsar's createComplete exactly. */
  @Override
  public synchronized void createComplete(int rc, LedgerHandle lh, Object ctx) {
    if (log.isDebugEnabled()) {
      log.debug("[{}] createComplete rc={} ledger={}", name, rc, lh != null ? lh.getId() : -1);
    }

    // Check if ledger-op task is already completed by timeout-task
    if (checkAndCompleteLedgerOpTask(rc, lh, ctx)) {
      return;
    }

    if (rc != Code.OK) {
      log.error("[{}] Error creating ledger rc={} {}", name, rc, BKException.getMessage(rc));
      ManagedLedgerException status = ManagedLedgerException.fromBookKeeperException(rc);

      // no pending entries means that creating this new ledger is NOT caused by write failure
      if (pendingAddEntries.isEmpty()) {
        STATE_UPDATER.set(this, State.ClosedLedger);
      } else {
        STATE_UPDATER.set(this, State.WriteFailed);
      }

      // Empty the list of pending requests and make all of them fail
      clearPendingAddEntries(status);
      lastLedgerCreationFailureTimestamp = System.currentTimeMillis();
    } else {
      log.info("[{}] Created new ledger {}", name, lh.getId());

      LedgerHandle originalCurrentLedger = currentLedger;
      ledgers.put(lh.getId(), new LedgerInfo(lh.getId(), 0, 0));
      currentLedger = lh;
      currentLedgerTimeoutTriggered = new AtomicBoolean();
      currentLedgerEntries = 0;
      currentLedgerSize = 0;
      lastLedgerCreatedTimestamp = System.currentTimeMillis();
      notifyLedgerCreated(lh.getId());
      updateLedgersIdsComplete(originalCurrentLedger);
    }
  }

  /** Follows Pulsar's updateLedgersIdsComplete - drains pending entries after ledger creation. */
  void updateLedgersIdsComplete(LedgerHandle originalCurrentLedger) {
    STATE_UPDATER.set(this, State.LedgerOpened);
    lastConfirmedEntry = Position.create(currentLedger.getId(), -1);

    // Step 1: Reassign all pending ops to the new ledger.
    // If an op was already used by another ledger, duplicate it to get fresh state=OPEN.
    // This mirrors Pulsar's createNewOpAddEntryForNewLedger().
    int pendingSize = pendingAddEntries.size();
    for (int i = 0; i < pendingSize; i++) {
      AddEntryOperation existsOp = pendingAddEntries.poll();
      if (existsOp == null) {
        break;
      }
      // If op was already initiated on old ledger, duplicate with fresh OPEN state
      if (existsOp.getLedger() != null) {
        existsOp = existsOp.duplicateAndClose(currentLedgerTimeoutTriggered);
      } else {
        existsOp.setTimeoutTriggered(currentLedgerTimeoutTriggered);
      }
      existsOp.setLedger(currentLedger);
      pendingAddEntries.add(existsOp);
    }

    // Step 2: Iterate (without polling!) and initiate each op.
    // The entries stay in the queue; run() will poll them after BK callback.
    for (AddEntryOperation op : pendingAddEntries) {
      ++currentLedgerEntries;
      currentLedgerSize += op.getDataLength();

      if (currentLedgerIsFull()) {
        op.setCloseWhenDone(true);
        STATE_UPDATER.set(this, State.ClosingLedger);
        op.initiate();
        break;
      }

      op.initiate();
    }
  }

  /**
   * Checks if current ledger is full and needs rotation. Follows Pulsar's currentLedgerIsFull
   * exactly.
   */
  protected boolean currentLedgerIsFull() {
    boolean spaceQuotaReached =
        (currentLedgerEntries >= config.getMaxEntriesPerLedger()
            || currentLedgerSize >= config.getMaxSizePerLedger());

    long timeSinceLedgerCreationMs = System.currentTimeMillis() - lastLedgerCreatedTimestamp;
    long maxRolloverTimeMs = config.getMaxLedgerRolloverTime().toMillis();
    boolean maxLedgerTimeReached =
        maxRolloverTimeMs > 0 && timeSinceLedgerCreationMs >= maxRolloverTimeMs;

    return spaceQuotaReached || maxLedgerTimeReached;
  }

  /**
   * Explicitly roll the current ledger if it's full. Follows Pulsar's rollCurrentLedgerIfFull
   * exactly.
   */
  public void rollCurrentLedgerIfFull() {
    log.info("[{}] Start checking if current ledger is full", name);
    if (currentLedgerEntries > 0
        && currentLedgerIsFull()
        && STATE_UPDATER.compareAndSet(this, State.LedgerOpened, State.ClosingLedger)) {
      currentLedger.asyncClose(
          (rc, lh, ctx) -> {
            if (rc == Code.OK) {
              if (log.isDebugEnabled()) {
                log.debug(
                    "[{}] Successfully closed ledger {}, trigger by rollover full ledger",
                    name,
                    lh.getId());
              }
            } else {
              log.warn(
                  "[{}] Error when closing ledger {}, trigger by rollover full ledger, Status={}",
                  name,
                  lh.getId(),
                  BKException.getMessage(rc));
            }
            ledgerClosed(lh);
          },
          null);
    }
  }

  /**
   * Create ledger async and schedule a timeout task to check ledger-creation is complete else it
   * fails the callback with TimeoutException.
   *
   * <p>Follows Pulsar's asyncCreateLedger exactly.
   */
  protected void asyncCreateLedger(
      BookKeeper bookKeeper, ManagedLedgerConfig config, CreateCallback cb) {
    CompletableFuture<LedgerHandle> ledgerFutureHook = new CompletableFuture<>();

    try {
      bookKeeper.asyncCreateLedger(
          config.getEnsembleSize(),
          config.getWriteQuorumSize(),
          config.getAckQuorumSize(),
          config.getDigestType(),
          config.getPassword(),
          cb,
          ledgerFutureHook, // Pass as ctx for timeout checking
          null);
    } catch (Throwable cause) {
      log.error("[{}] Encountered unexpected error when creating ledger", name, cause);
      ledgerFutureHook.completeExceptionally(cause);
      cb.createComplete(Code.UnexpectedConditionException, null, ledgerFutureHook);
      return;
    }

    // Schedule timeout checker
    ScheduledFuture<?> timeoutChecker =
        scheduledExecutor.schedule(
            () -> {
              if (!ledgerFutureHook.isDone()
                  && ledgerFutureHook.completeExceptionally(
                      new java.util.concurrent.TimeoutException(name + " Create ledger timeout"))) {
                if (log.isDebugEnabled()) {
                  log.debug("[{}] Timeout creating ledger", name);
                }
                cb.createComplete(Code.TimeoutException, null, ledgerFutureHook);
              } else {
                if (log.isDebugEnabled()) {
                  log.debug("[{}] Ledger already created when timeout task is triggered", name);
                }
              }
            },
            config.getOperationTimeout().getSeconds(),
            TimeUnit.SECONDS);

    // Cancel timeout when ledger creation completes
    ledgerFutureHook.whenComplete(
        (ignore, ex) -> {
          timeoutChecker.cancel(false);
        });
  }

  /**
   * Check if ledger-op task is already completed by timeout-task. If completed then delete the
   * created ledger.
   *
   * <p>Follows Pulsar's checkAndCompleteLedgerOpTask exactly.
   */
  @SuppressWarnings("unchecked")
  protected boolean checkAndCompleteLedgerOpTask(int rc, LedgerHandle lh, Object ctx) {
    if (ctx instanceof CompletableFuture) {
      CompletableFuture<LedgerHandle> future = (CompletableFuture<LedgerHandle>) ctx;
      // ledger-creation is already timed out and callback is already completed so, delete
      // this ledger and return.
      if (future.complete(lh) || rc == Code.TimeoutException) {
        return false;
      } else {
        if (rc == Code.OK) {
          log.warn("[{}]-{} ledger creation timed-out, deleting ledger", this.name, lh.getId());
          asyncDeleteLedger(lh.getId());
        }
        return true;
      }
    }
    return false;
  }

  /** Delete a ledger asynchronously (cleanup after timeout). */
  private void asyncDeleteLedger(long ledgerId) {
    bookKeeper.asyncDeleteLedger(
        ledgerId,
        (rc, ctx) -> {
          if (rc != Code.OK) {
            log.warn(
                "[{}] Failed to delete ledger {} after timeout: {}",
                name,
                ledgerId,
                BKException.getMessage(rc));
          } else {
            log.info("[{}] Deleted ledger {} after timeout", name, ledgerId);
          }
        },
        null);
  }

  private void clearPendingAddEntries(ManagedLedgerException status) {
    AddEntryOperation op;
    while ((op = pendingAddEntries.poll()) != null) {
      op.failed(status);
    }
  }

  // ==================== Timeout Task (follows Pulsar's scheduleTimeoutTask) ====================

  private void scheduleTimeoutTask() {
    // disable timeout task checker if timeout <= 0
    long timeoutSec = config.getOperationTimeout().getSeconds();
    if (timeoutSec > 0) {
      this.timeoutTask =
          this.scheduledExecutor.scheduleAtFixedRate(
              this::checkTimeouts, timeoutSec, timeoutSec, TimeUnit.SECONDS);
    }
  }

  private void checkTimeouts() {
    final State state = STATE_UPDATER.get(this);
    if (state == State.Closed || state.isFenced()) {
      return;
    }
    checkAddTimeout();
  }

  /**
   * Check if the oldest pending add entry operation has timed out. Follows Pulsar's checkAddTimeout
   * exactly.
   */
  private void checkAddTimeout() {
    long timeoutSec = config.getOperationTimeout().getSeconds();
    if (timeoutSec < 1) {
      return;
    }
    AddEntryOperation opAddEntry = pendingAddEntries.peek();
    if (opAddEntry != null) {
      // Check if operation was initiated and has been waiting too long
      boolean isTimedOut =
          opAddEntry.getLastInitTime() != -1
              && TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - opAddEntry.getLastInitTime())
                  >= timeoutSec;
      if (isTimedOut) {
        log.warn(
            "[{}] Failed to add entry {}:{} in time-out {} sec",
            this.name,
            opAddEntry.getLedger() != null ? opAddEntry.getLedger().getId() : -1,
            opAddEntry.getEntryId(),
            timeoutSec);
        currentLedgerTimeoutTriggered.set(true);
        opAddEntry.handleAddFailure(opAddEntry.getLedger(), Code.TimeoutException);
      }
    }
  }

  // ==================== Methods called by AddEntryOperation ====================

  public Executor getExecutor() {
    return executor;
  }

  public AddEntryOperation pollPendingAddEntry() {
    return pendingAddEntries.poll();
  }

  public void incrementEntryCount() {
    NUMBER_OF_ENTRIES_UPDATER.incrementAndGet(this);
  }

  public void addToTotalSize(long size) {
    TOTAL_SIZE_UPDATER.addAndGet(this, size);
  }

  public void updateLastConfirmedEntry(Position pos) {
    this.lastConfirmedEntry = pos;
  }

  public void recordAddEntryError() {
    // TODO: metrics
  }

  public void recordAddEntryLatency(long value, TimeUnit unit) {
    // TODO: metrics
  }

  public void notifyWaitingCallbacks() {
    Runnable r;
    while ((r = waitingEntryCallBacks.poll()) != null) {
      r.run();
    }
  }

  /**
   * Called when a ledger is closed (either from closeWhenDone or failure). Follows Pulsar's
   * ledgerClosed exactly.
   */
  public synchronized void ledgerClosed(LedgerHandle lh) {
    final State state = STATE_UPDATER.get(this);
    LedgerHandle currentLh = this.currentLedger;

    if (currentLh == lh && (state == State.ClosingLedger || state == State.LedgerOpened)) {
      STATE_UPDATER.set(this, State.ClosedLedger);
    } else if (state == State.Closed) {
      // The managed ledger was closed during the write operation
      clearPendingAddEntries(
          new ManagedLedgerException.ManagedLedgerClosedException(
              "Managed ledger was already closed"));
      return;
    } else {
      // In case we get multiple write errors for different outstanding write request,
      // we should close the ledger just once
      return;
    }

    long entriesInLedger = lh.getLastAddConfirmed() + 1;
    if (log.isDebugEnabled()) {
      log.debug("[{}] Ledger has been closed id={} entries={}", name, lh.getId(), entriesInLedger);
    }

    if (entriesInLedger > 0) {
      LedgerInfo info = new LedgerInfo(lh.getId(), entriesInLedger, lh.getLength());
      ledgers.put(lh.getId(), info);
    } else {
      // The last ledger was empty, so we can discard it
      ledgers.remove(lh.getId());
    }

    notifyLedgerClosed(lh.getId(), entriesInLedger, lh.getLength());

    // No cursors/reads needed — trim all old fully-acked ledgers immediately
    trimConsumedLedgers();

    createLedgerAfterClosed();
  }

  /**
   * Creates a new ledger after the previous one was closed. Follows Pulsar's
   * createLedgerAfterClosed exactly.
   */
  synchronized void createLedgerAfterClosed() {
    if (isNeededCreateNewLedgerAfterCloseLedger()) {
      log.info(
          "[{}] Creating a new ledger after closed {}",
          name,
          currentLedger == null ? "null" : currentLedger.getId());
      STATE_UPDATER.set(this, State.CreatingLedger);
      this.lastLedgerCreationInitiationTimestamp = System.currentTimeMillis();
      // Use the executor here to avoid using the Zookeeper thread to create the ledger
      this.executor.execute(() -> asyncCreateLedger(bookKeeper, config, this));
    }
  }

  /**
   * Checks if we need to create a new ledger after one was closed. Follows Pulsar's
   * isNeededCreateNewLedgerAfterCloseLedger exactly.
   */
  boolean isNeededCreateNewLedgerAfterCloseLedger() {
    final State state = STATE_UPDATER.get(this);
    if (state != State.CreatingLedger && state != State.LedgerOpened) {
      return true;
    }
    return false;
  }

  /** Called when ledger is fenced by BookKeeper. */
  public void handleLedgerFenced(LedgerHandle lh) {
    log.warn("[{}] Ledger {} was fenced", name, lh != null ? lh.getId() : -1);
    ledgerClosed(lh);
  }

  public long incrementAddOpCount() {
    return ADD_OP_COUNT_UPDATER.incrementAndGet(this);
  }

  // ==================== Ledger Trimming ====================

  /**
   * Removes all old ledgers from the in-memory ledgers map. Ledger data is retained in BookKeeper
   * for recovery purposes. Since there are no cursors/readers, once a ledger is closed and all
   * entries are acked, it is fully consumed and no longer needs to be tracked in memory. Only the
   * currentLedger (active write ledger) is retained in the map.
   */
  private void trimConsumedLedgers() {
    LedgerHandle activeLedger = currentLedger;
    long activeLedgerId = activeLedger != null ? activeLedger.getId() : -1;

    Iterator<Map.Entry<Long, LedgerInfo>> iterator = ledgers.entrySet().iterator();
    while (iterator.hasNext()) {
      Map.Entry<Long, LedgerInfo> entry = iterator.next();
      long ledgerId = entry.getKey();

      // Never delete the current active ledger
      if (ledgerId == activeLedgerId) {
        continue;
      }

      LedgerInfo info = entry.getValue();
      log.info(
          "[{}] Trimming consumed ledger {} - entries={} size={}",
          name,
          ledgerId,
          info.entries,
          info.size);

      // Remove from in-memory map only — ledger data stays in BookKeeper for recovery
      iterator.remove();
      NUMBER_OF_ENTRIES_UPDATER.addAndGet(this, -info.entries);
      TOTAL_SIZE_UPDATER.addAndGet(this, -info.size);
    }
  }

  /**
   * Checks if a ledger is currently in use by this managed ledger. A ledger is in use if it is the
   * active write ledger or is tracked in the ledgers map (not yet trimmed).
   */
  @Override
  public boolean isLedgerInUse(long ledgerId) {
    // Check if it's the currently active ledger (may not yet be in ledgers map)
    LedgerHandle lh = currentLedger;
    if (lh != null && lh.getId() == ledgerId) {
      return true;
    }
    // Check tracked ledgers
    return ledgers.containsKey(ledgerId);
  }

  // ==================== Listener Notifications ====================

  private void notifyLedgerCreated(long ledgerId) {
    if (listener != null) {
      try {
        listener.onLedgerCreated(name, ledgerId);
      } catch (Exception e) {
        log.warn(
            "[{}] Listener.onLedgerCreated failed for ledger {}: {}",
            name,
            ledgerId,
            e.getMessage(),
            e);
      }
    }
  }

  private void notifyLedgerClosed(long ledgerId, long entries, long size) {
    if (listener != null) {
      try {
        listener.onLedgerClosed(name, ledgerId, entries, size);
      } catch (Exception e) {
        log.warn(
            "[{}] Listener.onLedgerClosed failed for ledger {}: {}",
            name,
            ledgerId,
            e.getMessage(),
            e);
      }
    }
  }

  // ==================== Getters ====================

  @Override
  public Position getLastConfirmedEntry() {
    return lastConfirmedEntry;
  }

  @Override
  public long getNumberOfEntries() {
    return numberOfEntries;
  }

  @Override
  public long getTotalSize() {
    return totalSize;
  }

  @Override
  public long getCurrentLedgerId() {
    return currentLedger != null ? currentLedger.getId() : -1;
  }

  @Override
  public long getCurrentLedgerEntries() {
    return currentLedgerEntries;
  }

  @Override
  public int getPendingAddEntriesCount() {
    return pendingAddEntries.size();
  }

  @Override
  public boolean isTerminated() {
    return state == State.Terminated;
  }

  @Override
  public void close() throws ManagedLedgerException, InterruptedException {
    try {
      asyncClose().get(AsyncOperationTimeoutSeconds, TimeUnit.SECONDS);
    } catch (java.util.concurrent.ExecutionException e) {
      throw new ManagedLedgerException("Error during close", e.getCause());
    } catch (java.util.concurrent.TimeoutException e) {
      throw new ManagedLedgerException.OperationTimeoutException(
          "Timeout during managed ledger close");
    }
  }

  @Override
  public CompletableFuture<Void> asyncClose() {
    CompletableFuture<Void> future = new CompletableFuture<>();

    log.info("[{}] Closing managed ledger", name);
    STATE_UPDATER.set(this, State.Closed);

    // Cancel timeout task
    if (timeoutTask != null) {
      timeoutTask.cancel(false);
    }

    // Fail all pending entries
    clearPendingAddEntries(new ManagedLedgerException.ManagedLedgerClosedException());

    if (currentLedger != null) {
      currentLedger.asyncClose(
          (rc, lh, ctx) -> {
            if (rc != Code.OK) {
              log.warn("[{}] Error closing ledger: {}", name, BKException.getMessage(rc));
            }
            future.complete(null);
          },
          null);
    } else {
      future.complete(null);
    }
    return future;
  }

  /** Simple ledger info holder */
  protected static class LedgerInfo {
    final long ledgerId;
    long entries;
    long size;
    long timestamp;

    LedgerInfo(long id, long entries, long size) {
      this.ledgerId = id;
      this.entries = entries;
      this.size = size;
      this.timestamp = 0;
    }
  }
}
