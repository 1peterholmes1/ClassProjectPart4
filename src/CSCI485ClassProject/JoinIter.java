package CSCI485ClassProject;

import CSCI485ClassProject.fdb.FDBHelper;
import CSCI485ClassProject.fdb.FDBKVPair;
import CSCI485ClassProject.models.AttributeType;
import CSCI485ClassProject.models.ComparisonPredicate;
import CSCI485ClassProject.models.Record;
import CSCI485ClassProject.models.TableMetadata;
import CSCI485ClassProject.utils.ComparisonUtils;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;

import java.util.*;

public class JoinIter extends Iterator {

	private AsyncIterator<KeyValue> resIt;
	private Transaction tx;
	private String tableName1;
	private String tableName2;
	private ComparisonPredicate pred;
	private List<String> path;
	private DirectorySubspace dir;
	private Set<String> attrNames;

	public JoinIter(Iterator it1, Iterator it2, ComparisonPredicate pred, Set<String> attrNames) {
		tx = it1.getTx();
		this.pred = pred;
		tableName1 = it1.getTableName();
		tableName2 = it2.getTableName();

		Records recs3 = new RecordsImpl();
		path = List.of(tableName1, tableName2, "join");
		dir = FDBHelper.createOrOpenSubspace(tx, path);

		if (attrNames == null) {
			this.attrNames = new HashSet<>();
			this.attrNames.addAll(it1.getTableMetadata().getAttributes().keySet());
			this.attrNames.addAll(it2.getTableMetadata().getAttributes().keySet());
		} else {
			this.attrNames = attrNames;
		}

//		Transaction jointx = FDBHelper.openTransaction(FDBHelper.initialization());
		while (true) {
			Record rec1 = it1.next();
			if (rec1 == null) {
				break;
			}

			Cursor c = recs3.openCursor(tableName2, Cursor.Mode.READ);
			it2 = new NoPredIter(c);
			while (true) {
				Record rec2 = it2.next();
				if (rec2 == null) {
					break;
				}
				if (testPred(rec1, rec2)) {
					Record rec = new Record();
					for (String attrName : this.attrNames) {
						Object r1val = rec1.getValueForGivenAttrName(attrName);
						Object r2val = rec2.getValueForGivenAttrName(attrName);
						if (r1val != null && r2val != null) {
							String newAttrName1 = tableName1 + "." + attrName;
							String newAttrName2 = tableName2 + "." + attrName;
							rec.setAttrNameAndValue(newAttrName1, r1val);
							rec.setAttrNameAndValue(newAttrName2, r2val);
						} else {
							if (r1val != null) {
								rec.setAttrNameAndValue(attrName, r1val);
							} else if (r2val != null) {
								rec.setAttrNameAndValue(attrName, r2val);
							}
						}
					}

					List<String> keys = new ArrayList<>();
					for (String attrName : rec.getMapAttrNameToValue().keySet()) {
						keys.add(attrName);
					}
					List<Object> vals = new ArrayList<>();
					for (String attrName : keys) {
						vals.add(rec.getValueForGivenAttrName(attrName));
					}
					Tuple k = Tuple.fromList(keys);
					Tuple v = Tuple.fromList(vals);
//					System.out.println("writing key: " + k + " value: " + v);
					tx.set(dir.pack(Tuple.fromList(vals)),Tuple.fromList(keys).pack());

				}
			}
		}
		resIt = null;
//		jointx.commit().join();
//		tx = FDBHelper.openTransaction(FDBHelper.initialization());

	}

	@Override
	public Record next() {
		if (resIt == null) {
			resIt = tx.getRange(dir.range()).iterator();
			List<KeyValue> kvs = tx.getRange(dir.range()).asList().join();
			for (KeyValue kv : kvs) {
//				System.out.println("key: " + dir.unpack(kv.getKey()) + " value: " + Tuple.fromBytes(kv.getValue()));
			}

		}
		if (resIt.hasNext()) {
			KeyValue kv = resIt.next();
			Record rec = new Record();

			Tuple val = dir.unpack(kv.getKey());
			Tuple key = Tuple.fromBytes(kv.getValue());
			for (int i = 0; i < key.size(); i++) {
				rec.setAttrNameAndValue(key.getString(i), val.get(i));
			}

			return rec;
		} else {
			return null;
		}
	}

	@Override
	public void commit() {
		FDBHelper.dropSubspace(tx,path);
		tx.commit();

	}

	@Override public void abort() {
		tx.cancel();
	}

	@Override
	public String getTableName() {
		return tableName1;
	}

	@Override
	public Transaction getTx() {
		return tx;
	}

	@Override
	public TableMetadata getTableMetadata() {
		return null;
	}

	@Override
	public StatusCode delete() {
		return StatusCode.OPERATOR_DELETE_ITERATOR_INVALID;
	}


	boolean testPred(Record rec1, Record rec2) {
		Object l = rec1.getValueForGivenAttrName(pred.getLeftHandSideAttrName());
		Object r = rec2.getValueForGivenAttrName(pred.getRightHandSideAttrName());
		Object r2 = pred.getRightHandSideValue();

		if (pred.getRightHandSideAttrType() == AttributeType.INT) {
			long r1;
			if (r instanceof Integer) {
				r1 = new Long((Integer) r);
			} else {
				r1 = (Long) r;
			}

			long r3;
			if (pred.getRightHandSideValue() instanceof Integer) {
				r3 = new Long((Integer) pred.getRightHandSideValue());
			} else {
				r3 = (Long) pred.getRightHandSideValue();
			}

			switch (pred.getRightHandSideOperator()) {
				case PLUS:
					return ComparisonUtils.compareTwoINT(l,r1 + r3, pred.getOperator());
				case MINUS:
					return ComparisonUtils.compareTwoINT(l,r1 - r3, pred.getOperator());
				case PRODUCT:
					return ComparisonUtils.compareTwoINT(l,r1 * r3, pred.getOperator());
				case DIVISION:
					return ComparisonUtils.compareTwoINT(l,r1 / r3, pred.getOperator());
				default:
					break;
			}
		} else if (pred.getRightHandSideAttrType() == AttributeType.DOUBLE) {

			Double r1 = (Double) r;
			Double r3 = (Double) pred.getRightHandSideValue();

			switch (pred.getRightHandSideOperator()) {
				case PLUS:
					return ComparisonUtils.compareTwoDOUBLE(l,r1 + r3, pred.getOperator());
				case MINUS:
					return ComparisonUtils.compareTwoDOUBLE(l,r1 - r3, pred.getOperator());
				case PRODUCT:
					return ComparisonUtils.compareTwoDOUBLE(l,r1 * r3,  pred.getOperator());
				case DIVISION:
					return ComparisonUtils.compareTwoDOUBLE(l,r1 / r3,  pred.getOperator());
				default:
					break;
			}

		}

		return false;
	}


}
