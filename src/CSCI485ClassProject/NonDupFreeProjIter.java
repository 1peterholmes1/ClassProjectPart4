package CSCI485ClassProject;

import CSCI485ClassProject.models.Record;
import CSCI485ClassProject.models.TableMetadata;
import com.apple.foundationdb.Transaction;

public class NonDupFreeProjIter extends Iterator {

	private boolean fromIt;
	private Cursor cursor;
	private String attrName;
	private Iterator it;
	private Transaction tx;
	private String tableName;

	public NonDupFreeProjIter(Iterator it, String attrName) {
		this.tx = it.getTx();
		this.tableName = it.getTableName();
		this.fromIt = true;
		this.attrName = attrName;
		this.it = it;
	}
	public NonDupFreeProjIter(Cursor cursor, String attrName) {
		this.tx = cursor.getTx();
		this.tableName = cursor.getTableName();
		this.fromIt = false;
		this.cursor = cursor;
		this.attrName = attrName;
	}

	@Override
	public Record next() {
		Record rec;
		if (!fromIt) {
			if (cursor.isInitialized()) {
				rec = cursor.next(false);
			} else {
				rec = cursor.getFirst();
			}
		} else {
			rec = it.next();
		}
		if (rec == null) {
			return null;
		}
		Record out;
		if (rec.getValueForGivenAttrName(attrName) == null) {
			out = new Record();
		} else {
			Object projobj = rec.getValueForGivenAttrName(attrName);
			out = new Record();
			out.setAttrNameAndValue(attrName, projobj);
		}
		return out;

	}

	@Override
	public void commit() {
		if (!fromIt)
			cursor.commit();
		else
			it.commit();
	}

	@Override
	public void abort() {
		if (!fromIt)
			cursor.abort();
		else
			it.abort();
	}

	@Override
	public String getTableName() {
		return tableName;
	}

	@Override
	public Transaction getTx() {
		return tx;
	}

	@Override
	public TableMetadata getTableMetadata() {
		return cursor.getTableMetadata();
	}

	@Override
	public StatusCode delete() {
		return StatusCode.OPERATOR_DELETE_ITERATOR_INVALID;
	}
}
