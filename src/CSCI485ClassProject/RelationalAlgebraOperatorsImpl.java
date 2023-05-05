package CSCI485ClassProject;

import CSCI485ClassProject.models.AssignmentExpression;
import CSCI485ClassProject.models.AttributeType;
import CSCI485ClassProject.models.ComparisonPredicate;
import CSCI485ClassProject.models.Record;
import CSCI485ClassProject.Cursor.Mode;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.ArrayList;
import java.util.Map;

// your codes
public class RelationalAlgebraOperatorsImpl implements RelationalAlgebraOperators {

  @Override
  public Iterator select(String tableName, ComparisonPredicate predicate, Iterator.Mode mode, boolean isUsingIndex) {
    Records records = new RecordsImpl();
    if (predicate.getRightHandSideAttrType() == AttributeType.VARCHAR || predicate.getLeftHandSideAttrType() == AttributeType.VARCHAR) {
      return null;
    }
    if (predicate.getPredicateType() == ComparisonPredicate.Type.NONE) {
      return new NoPredIter(records.openCursor(tableName,iterToCursor(mode)));
    } else if (predicate.getPredicateType() == ComparisonPredicate.Type.ONE_ATTR) {
        return new OnePredIter(records.openCursor(tableName,predicate.getLeftHandSideAttrName(),predicate.getRightHandSideValue(),predicate.getOperator(),iterToCursor(mode),isUsingIndex));
    } else if (predicate.getPredicateType() == ComparisonPredicate.Type.TWO_ATTRS) {
      Cursor cursor = records.openCursor(tableName,iterToCursor(mode));
      Iterator i = new TwoPredIter(cursor,predicate);
      return i;
    } else {
        return null;
    }

  }
  @Override
  public Set<Record> simpleSelect(String tableName, ComparisonPredicate predicate, boolean isUsingIndex) {
    Iterator i = select(tableName,predicate,Iterator.Mode.READ,isUsingIndex);
    if (i == null) return null;
    Set<Record> records = new HashSet<>();
    while(true) {
        Record r = i.next();
        if (r == null) {
            break;
        }
        records.add(r);
    }
    return records;
  }

  @Override
  public Iterator project(String tableName, String attrName, boolean isDuplicateFree) {
    Records records = new RecordsImpl();
    if (!isDuplicateFree) {
        return new NonDupFreeProjIter(records.openCursor(tableName,Mode.READ),attrName);
    }
    return new DupFreeProjIter(records.openCursor(tableName,Mode.READ),attrName);
  }

  @Override
  public Iterator project(Iterator iterator, String attrName, boolean isDuplicateFree) {
    if (!isDuplicateFree) {
        return new NonDupFreeProjIter(iterator,attrName);
    }
    return new DupFreeProjIter(iterator,attrName);
  }

  @Override
  public List<Record> simpleProject(String tableName, String attrName, boolean isDuplicateFree) {
    Iterator i = project(tableName,attrName,isDuplicateFree);
    if (i == null) return null;
    List<Record> records = new ArrayList<>();
    while(true) {
        Record r = i.next();
        if (r == null) {
            break;
        }
        records.add(r);
    }
    return records;
  }

  @Override
  public List<Record> simpleProject(Iterator iterator, String attrName, boolean isDuplicateFree) {
    Iterator i = project(iterator,attrName,isDuplicateFree);
    if (i == null) return null;
    List<Record> records = new ArrayList<>();
    while(true) {
        Record r = i.next();
        if (r == null) {
            break;
        }
        records.add(r);
    }
    return records;
  }

  @Override
  public Iterator join(Iterator outerIterator, Iterator innerIterator, ComparisonPredicate predicate, Set<String> attrNames) {
    return new JoinIter(outerIterator,innerIterator,predicate,attrNames);
  }

  @Override
  public StatusCode insert(String tableName, Record record, String[] primaryKeys) {
    Records records = new RecordsImpl();
    Map<String,Record.Value> recmap = record.getMapAttrNameToValue();
    List<String> attrNames = new ArrayList<>();
    List<Object> primaryKeysVals = new ArrayList<>();
    List<Object> attrVals = new ArrayList<>();
    Set<String> pks = Set.of(primaryKeys);
    for (String attrName : recmap.keySet()) {
      if (pks.contains(attrName)) {
        primaryKeysVals.add(recmap.get(attrName).getValue());
      } else {
        attrNames.add(attrName);
        attrVals.add(recmap.get(attrName).getValue());
      }
    }
    return records.insertRecord(tableName,primaryKeys, primaryKeysVals.toArray(), attrNames.toArray(new String[0]), attrVals.toArray());
  }

  @Override
  public StatusCode update(String tableName, AssignmentExpression assignExp, Iterator dataSourceIterator) {

    return null;
  }

  @Override
  public StatusCode delete(String tableName, Iterator iterator) {
    if (iterator == null) {
      return StatusCode.ITERATOR_NOT_POINTED_TO_ANY_RECORD;
    }
    if (iterator.getMode() == Iterator.Mode.READ) {
      return StatusCode.OPERATOR_DELETE_ITERATOR_INVALID;
    }
    return iterator.delete();
  }

  static Iterator.Mode cursorToIter(Cursor.Mode mode) {
    switch (mode) {
      case READ:
        return Iterator.Mode.READ;
      case READ_WRITE:
        return Iterator.Mode.READ_WRITE;
      default:
        return null;
    }
  }

  static Cursor.Mode iterToCursor(Iterator.Mode mode) {
    switch (mode) {
      case READ:
        return Cursor.Mode.READ;
      case READ_WRITE:
        return Cursor.Mode.READ_WRITE;
      default:
        return null;
    }
  }


}
