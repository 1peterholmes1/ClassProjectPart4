package CSCI485ClassProject;

import CSCI485ClassProject.models.Record;
import CSCI485ClassProject.models.TableMetadata;
import com.apple.foundationdb.Transaction;

public class OnePredIter extends Iterator{

	private Mode mode;
	private Cursor cursor;

	public OnePredIter(Cursor cursor) {
		this.cursor = cursor;
	}

	@Override
	public Record next() {
		if(cursor.isInitialized()) {
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
		cursor.commit();
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
		if (mode == Mode.READ) {
			return StatusCode.OPERATOR_DELETE_ITERATOR_INVALID;
		}
		if (cursor.isInitialized()){
			return cursor.deleteCurrentRecord();
		} else {
			cursor.getFirst();
		}
		return cursor.deleteCurrentRecord();

	}
}
