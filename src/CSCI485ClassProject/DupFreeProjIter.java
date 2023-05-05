package CSCI485ClassProject;

import CSCI485ClassProject.fdb.FDBHelper;
import CSCI485ClassProject.models.Record;
import CSCI485ClassProject.models.TableMetadata;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;

import java.util.List;

public class DupFreeProjIter extends Iterator {

	private Cursor cursor;
	private Iterator iter;
	private String attrName;
	private Records recs;
	private DirectorySubspace dir;
	private AsyncIterator<KeyValue> it;
	private List<String> path;
	private Transaction tx;
	private String tableName;

	public DupFreeProjIter(Iterator iter, String attrName) {
		this.iter = iter;
		this.attrName = attrName;
		this.tableName = iter.getTableName();
		this.tx = iter.getTx();
		recs = new RecordsImpl();

		path = List.of(tableName,"projections",attrName);

		dir = FDBHelper.createOrOpenSubspace(tx, path);

		while(true) {
			Record rec = iter.next();
			if (rec == null) {
				break;
			}
			tx.set(dir.pack(rec.getValueForGivenAttrName(attrName)),new Tuple().pack());
		}
		it = null;
	}
	public DupFreeProjIter(Cursor cursor, String attrName) {
		this.cursor = cursor;
		this.attrName = attrName;
		recs = new RecordsImpl();
		this.tableName = cursor.getTableName();
		this.tx = cursor.getTx();

		path = List.of(cursor.getTableName(),"projections",attrName);
		dir = FDBHelper.createOrOpenSubspace(tx, path);

		NonDupFreeProjIter iter = new NonDupFreeProjIter(cursor, attrName);
		while(true) {
			Record rec = iter.next();
			if (rec == null) {
				break;
			}
			tx.set(dir.pack(rec.getValueForGivenAttrName(attrName)),new Tuple().pack());
		}
		it = null;

	}

	@Override
	public Record next() {
		if (it == null) {
			it = tx.getRange(dir.range()).iterator();
		}
		if (it.hasNext()) {
			KeyValue kv = it.next();
			Tuple tup = Tuple.fromBytes(kv.getKey());
			Object o = tup.get(tup.size()-1);
			Record rec = new Record();
			rec.setAttrNameAndValue(attrName, o);
			return rec;
		} else {
			return null;
		}
	}

	@Override
	public void commit() {
		FDBHelper.dropSubspace(tx,path);
		if (cursor != null) {
			cursor.commit();
		} else {
			iter.commit();
		}

	}

	@Override
	public void abort() {
		FDBHelper.dropSubspace(tx,path);
		if (cursor != null) {
			cursor.abort();
		} else {
			iter.abort();
		}
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
