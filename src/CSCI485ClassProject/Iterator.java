package CSCI485ClassProject;

import CSCI485ClassProject.models.Record;
import CSCI485ClassProject.models.TableMetadata;
import com.apple.foundationdb.Transaction;

public abstract class Iterator {

  public enum Mode {
    READ,
    READ_WRITE
  }

  private Mode mode;

  public Mode getMode() {
    return mode;
  };

  public void setMode(Mode mode) {
    this.mode = mode;
  };

  public abstract Record next();

  public abstract void commit();

  public abstract void abort();
  public abstract String getTableName();
  public abstract Transaction getTx();
  public abstract TableMetadata getTableMetadata();
  public abstract StatusCode delete();
}
