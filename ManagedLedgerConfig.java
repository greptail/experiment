package com.open.messaging.bigbro.broker.keeper.ledger;

import java.time.Duration;
import org.apache.bookkeeper.client.BookKeeper;

/** Configuration for the managed ledger. Provides all tunable parameters for ledger behavior. */
public class ManagedLedgerConfig {

  /** Default maximum entries per ledger before rotation. */
  public static final int DEFAULT_MAX_ENTRIES_PER_LEDGER = 25000;

  /** Default maximum size per ledger in bytes (256 MB). */
  public static final long DEFAULT_MAX_SIZE_PER_LEDGER = 256 * 1024 * 1024;

  /** Default async operation timeout. */
  public static final Duration DEFAULT_OPERATION_TIMEOUT = Duration.ofSeconds(30);

  /** Default ledger rollover time (4 hours). */
  public static final Duration DEFAULT_ROLLOVER_TIME = Duration.ofHours(4);

  private int ensembleSize = 1;
  private int writeQuorumSize = 1;
  private int ackQuorumSize = 1;
  private byte[] password = "managed-ledger".getBytes();
  private BookKeeper.DigestType digestType = BookKeeper.DigestType.CRC32;

  private int maxEntriesPerLedger = DEFAULT_MAX_ENTRIES_PER_LEDGER;
  private long maxSizePerLedger = DEFAULT_MAX_SIZE_PER_LEDGER;
  private Duration operationTimeout = DEFAULT_OPERATION_TIMEOUT;
  private Duration maxLedgerRolloverTime = DEFAULT_ROLLOVER_TIME;

  private boolean proactiveLedgerRotation = true;
  private int rotationThresholdPercentage = 80;

  private int pendingAddEntryLimit = 10000;

  /** Creates a config with default values. */
  public ManagedLedgerConfig() {}

  /**
   * Builder pattern for creating config.
   *
   * @return a new builder
   */
  public static Builder builder() {
    return new Builder();
  }

  // Getters

  public int getEnsembleSize() {
    return ensembleSize;
  }

  public int getWriteQuorumSize() {
    return writeQuorumSize;
  }

  public int getAckQuorumSize() {
    return ackQuorumSize;
  }

  public byte[] getPassword() {
    return password;
  }

  public BookKeeper.DigestType getDigestType() {
    return digestType;
  }

  public int getMaxEntriesPerLedger() {
    return maxEntriesPerLedger;
  }

  public long getMaxSizePerLedger() {
    return maxSizePerLedger;
  }

  public Duration getOperationTimeout() {
    return operationTimeout;
  }

  public Duration getMaxLedgerRolloverTime() {
    return maxLedgerRolloverTime;
  }

  public boolean isProactiveLedgerRotation() {
    return proactiveLedgerRotation;
  }

  public int getRotationThresholdPercentage() {
    return rotationThresholdPercentage;
  }

  public int getPendingAddEntryLimit() {
    return pendingAddEntryLimit;
  }

  /**
   * Calculates the entry threshold for triggering proactive ledger creation.
   *
   * @return the threshold entry count
   */
  public int getProactiveLedgerCreationThreshold() {
    return (maxEntriesPerLedger * rotationThresholdPercentage) / 100;
  }

  /** Builder for ManagedLedgerConfig. */
  public static class Builder {
    private final ManagedLedgerConfig config = new ManagedLedgerConfig();

    public Builder ensembleSize(int size) {
      config.ensembleSize = size;
      return this;
    }

    public Builder writeQuorumSize(int size) {
      config.writeQuorumSize = size;
      return this;
    }

    public Builder ackQuorumSize(int size) {
      config.ackQuorumSize = size;
      return this;
    }

    public Builder password(byte[] password) {
      config.password = password;
      return this;
    }

    public Builder digestType(BookKeeper.DigestType digestType) {
      config.digestType = digestType;
      return this;
    }

    public Builder maxEntriesPerLedger(int max) {
      config.maxEntriesPerLedger = max;
      return this;
    }

    public Builder maxSizePerLedger(long max) {
      config.maxSizePerLedger = max;
      return this;
    }

    public Builder operationTimeout(Duration timeout) {
      config.operationTimeout = timeout;
      return this;
    }

    public Builder maxLedgerRolloverTime(Duration time) {
      config.maxLedgerRolloverTime = time;
      return this;
    }

    public Builder proactiveLedgerRotation(boolean enabled) {
      config.proactiveLedgerRotation = enabled;
      return this;
    }

    public Builder rotationThresholdPercentage(int percentage) {
      config.rotationThresholdPercentage = percentage;
      return this;
    }

    public Builder pendingAddEntryLimit(int limit) {
      config.pendingAddEntryLimit = limit;
      return this;
    }

    public ManagedLedgerConfig build() {
      return config;
    }
  }
}
