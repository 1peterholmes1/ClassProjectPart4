package CSCI485ClassProject;
import CSCI485ClassProject.Cursor;
import CSCI485ClassProject.Iterator;
import CSCI485ClassProject.models.ComparisonPredicate;
import CSCI485ClassProject.models.Record;
import CSCI485ClassProject.models.AttributeType;
import CSCI485ClassProject.models.TableMetadata;
import CSCI485ClassProject.utils.ComparisonUtils;
import com.apple.foundationdb.Transaction;

import java.util.function.Predicate;

public class TwoPredIter extends Iterator{

	private Cursor cursor;
	private ComparisonPredicate pred;

	public TwoPredIter(Cursor cursor, ComparisonPredicate pred) {
		this.cursor = cursor;
		this.pred = pred;
	}

	@Override
	public Record next() {
		Record rec;
		if (cursor.isInitialized()) {
			rec = cursor.next(false);
		} else {
			rec = cursor.getFirst();
		}
		if (rec == null) {
			return null;
		}
		if (testPred(rec)) {
			return rec;
		} else {
			return next();
		}
	}

	@Override
	public void commit() {

	}

	@Override
	public void abort() {

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

	boolean testPred(Record rec) {
		Object l = rec.getValueForGivenAttrName(pred.getLeftHandSideAttrName());
		Object r = rec.getValueForGivenAttrName(pred.getRightHandSideAttrName());
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
