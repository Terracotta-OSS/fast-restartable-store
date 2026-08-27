package com.terracottatech.frs.transaction;

public class TransactionAccount implements TransactionLSNCallback {
  private long lsn = Long.MAX_VALUE;
  private boolean beginWritten = false;

  public synchronized boolean begin() {
    if (beginWritten) {
      return false;
    } else {
      beginWritten = true;
      return true;
    }
  }

  public synchronized void setLsn(long lsn) {
    if (this.lsn == Long.MAX_VALUE) {
      this.lsn = lsn;
    } else {
      // This shouldn't happen as we're getting LSNs in increasing order
      assert lsn > this.lsn;
    }
  }
  
  public long getLsn() {
    return lsn;
  }
}
