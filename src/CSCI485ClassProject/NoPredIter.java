package CSCI485ClassProject;

import CSCI485ClassProject.models.Record;
import CSCI485ClassProject.models.TableMetadata;
import com.apple.foundationdb.Transaction;

public class NoPredIter extends Iterator {


	private Cursor cursor;
	public NoPredIter(Cursor cursor) {
		this.cursor = cursor;
	}
	@Override
	public Record next() {
		if (cursor.isInitialized()) {
			return cursor.next(false);
		} else {
			return cursor.getFirst();
		}
	}

	@Override
	public void commit() {
		cursor.commit();
	}

	@Override
	public void abort() {
		cursor.abort();
	}

	@Override
	public String getTableName() {
		return cursor.getTableName();
	}

	@Override
	public Transaction getTx() {
		return cursor.getTx();
	}

	@Override
	public TableMetadata getTableMetadata() {
		return cursor.getTableMetadata();
	}

	@Override
	public StatusCode delete() {
		return cursor.deleteCurrentRecord();
	}
}
