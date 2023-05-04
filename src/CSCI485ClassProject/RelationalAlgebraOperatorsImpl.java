package CSCI485ClassProject;

import CSCI485ClassProject.models.AssignmentExpression;
import CSCI485ClassProject.models.ComparisonOperator;
import CSCI485ClassProject.models.ComparisonPredicate;
import CSCI485ClassProject.models.Record;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.Database;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

// your codes
public class RelationalAlgebraOperatorsImpl implements RelationalAlgebraOperators {

  private Database db;

  public RelationalAlgebraOperatorsImpl() {
      this.db = FDBHelper.initialization();
  }

  private boolean checkCpAndTableCompatible(ComparisonOperator cp, String table){
    if (cp == null || table == null) {
      System.out.println("ComparisonOperator or table name should not be null");
      return false;
    }
    return true;
    // TODO: implement this if need to
  }

  public static Number multiplyNumber(Number a, Number b){
    if (a instanceof Integer && b instanceof Integer) {
      return a.intValue() * b.intValue();
    } else if (a instanceof Float && b instanceof Float) {
      return a.floatValue() * b.floatValue();
    } else if (a instanceof Double && b instanceof Double) {
      return a.doubleValue() * b.doubleValue();
    } else if (a instanceof Long && b instanceof Long) {
      return a.longValue() * b.longValue();
    } else if (a instanceof Long && b instanceof Integer) {
      return a.longValue() * b.longValue(); // Upgrades b (Integer) to Long
    } else if (a instanceof Double && b instanceof Float) {
      return a.doubleValue() * b.doubleValue(); // Upgrades b (Float) to Double
    } else {
      Class<? extends Number> aclass = a.getClass();
      Class<? extends Number> bclass = b.getClass();
      System.out.println("Unsupported type for multiplication, LHS: " + a.getClass() + ", RHS: " + b.getClass());
      return null;
    }
  }

  public static boolean compareNumber(Number a, Number b, ComparisonOperator comp){
    switch (comp){
      case EQUAL_TO:
        return a.equals(b);
      case GREATER_THAN_OR_EQUAL_TO:
        return a.doubleValue() >= b.doubleValue();
      case LESS_THAN_OR_EQUAL_TO:
        return a.doubleValue() <= b.doubleValue();
      case GREATER_THAN:
        return a.doubleValue() > b.doubleValue();
      case LESS_THAN:
        return a.doubleValue() < b.doubleValue();
    }
    return false;
  }

  // Assumes that the table and the comparison predicate are compatible
  public static Function<Record, Boolean> createCpFunction(ComparisonPredicate cp){
    return record -> {
      Map<String, Object> map = record.getMapAttrNameToValueValue();
      if (cp.getPredicateType() == ComparisonPredicate.Type.NONE){
        return true;
      }

      Number lhs = (Number) map.get(cp.getLeftHandSideAttrName());
      Number rhs = null;
      if (cp.getPredicateType() == ComparisonPredicate.Type.ONE_ATTR){
        rhs = (Number) cp.getRightHandSideValue();
      }else{
        rhs = (Number) map.get(cp.getRightHandSideAttrName());
        Number multiplier = (Number) cp.getRightHandSideValue();
        rhs = multiplyNumber(rhs, multiplier);
      }
      return compareNumber(lhs, rhs, cp.getOperator());

    };
  }

  public static Record projectOne(Record record, String attrName){
    Map<String, Object> map = record.getMapAttrNameToValueValue();
    Record newRecord = new Record();
    newRecord.setAttrNameAndValue(attrName, map.get(attrName));
    return newRecord;
  }

  private class CursorIterator extends Iterator {
    private Cursor cursor;
    private Function<Record, Boolean> pcFunction;
    private boolean isUsingIndex;
    private Record nextRecord;
    private boolean isInitialized = false;

    private String projAttrName;
    public CursorIterator(Cursor cursor, Function<Record, Boolean> pcFunction, boolean isUsingIndex) {
      this.cursor = cursor;
      this.pcFunction = pcFunction;
      this.isUsingIndex = isUsingIndex;
    }

    @Override
    public Record next() {
      Record tmp =  cursor.getNextRecord();
      while (tmp != null && pcFunction.apply(tmp) == false){
        tmp = cursor.getNextRecord();
      }

      if (projAttrName != null) {
        tmp = projectOne(tmp, projAttrName);
      }
      return tmp;
    }

    @Override
    public void commit() {
      System.out.println("CursorIterator commit");
      cursor.commit();
    }

    @Override
    public void abort() {
      System.out.println("CursorIterator abort");
      cursor.commit();
    }
  }

  public Iterator getSubspaceProjectIterator(DirectorySubspace subspace, Transaction tx, boolean clearSubspace, String attributeName) {
    return new ProjectIterator(subspace, tx, clearSubspace, attributeName);
  }

  public Iterator getProjectIterator(DirectorySubspace subspace, Transaction tx, boolean clearSubspace, String attributeName) {
    return new ProjectIterator(subspace, tx, clearSubspace, attributeName);
  }

  public class ProjectIterator extends Iterator {

    DirectorySubspace subspace;
    Transaction tx;
    boolean clearSubspace;
    AsyncIterator<KeyValue> iterator;
    String attributeName;
    Object prevValue = null;

    boolean noDup = false;

    Iterator recordIterator = null;
    public ProjectIterator(DirectorySubspace subspace, Transaction tx, boolean clearSubspace, String attributeName) {
      this.subspace = subspace;
      this.tx = tx;
      this.clearSubspace = clearSubspace;
      this.iterator = tx.getRange(subspace.range()).iterator();
      this.attributeName = attributeName;
    }

    public ProjectIterator(Iterator iterator, String projAttrName) {
      this.recordIterator = iterator;
      this.attributeName = projAttrName;
      clearSubspace = false;

    }

    @Override
    public Record next() {
      if (recordIterator == null) {
        Record record = null;
        while (iterator.hasNext()) {
          KeyValue kv = iterator.next();
          Object value = Tuple.fromBytes(kv.getValue());
          System.out.println("Got value: " + value + " for key: " + kv.getKey() + " in ProjectIterator");
          if (!noDup || prevValue == null || !prevValue.equals(value)) {
            prevValue = value;
            record = new Record();
            Object v = Tuple.fromBytes(kv.getValue()).get(0);
            record.setAttrNameAndValue(attributeName, v);
            System.out.println("ProjectIterator: " + record.toString());
            return record;
          }
        }
      }
      else {
        Record record = recordIterator.next();
        if (record != null) {
          record = projectOne(record, attributeName);
        }
        return record;
      }
      return null;
    }

    @Override
    public void commit() {
      if (clearSubspace) {
        tx.clear(subspace.range());
      }
      // remove the tmp_UUID directory if exist
    }

    @Override
    public void abort() {
      if (clearSubspace) {
        tx.clear(subspace.range());
      }
      // remove the tmp_UUID directory if exist
    }
  }


  @Override
  public Iterator select(String tableName, ComparisonPredicate predicate, Iterator.Mode mode, boolean isUsingIndex) {
    // validate predicate
    if (predicate == null) {
      System.out.println("Predicate should not be null");
      return null;
    }

    if (predicate.validate() != StatusCode.PREDICATE_OR_EXPRESSION_VALID) {
      System.out.println("Predicate is not valid");
      return null;
    }


    if (isUsingIndex) {
      // check # attributes that needs attention

      // check if the LHS attribute is indexed
      // return ErrorCode if not indexed

      // if # attributes == 2
        // check if the RHS attribute is indexed
        // return ErrorCode if not indexed

    } else {
      // naive approach

      // get a general iterator over the table

      // get cursor

      Cursor cursor = new Cursor(tableName, Utils.getCursorModeFromIteratorMode(mode));
      cursor.moveToFirst();

      if (predicate.getPredicateType() == ComparisonPredicate.Type.NONE){
        // all good
      }
      else if (checkCpAndTableCompatible(predicate.getOperator(), tableName) == false) {
        System.out.println("ComparisonOperator and table name are not compatible");
        return null;
      }

      return new CursorIterator(cursor, createCpFunction(predicate), false);
    }

    return null;
  }

  @Override
  public Set<Record> simpleSelect(String tableName, ComparisonPredicate predicate, boolean isUsingIndex) {
    Iterator it = select(tableName, predicate, Iterator.Mode.READ, isUsingIndex);

    Set<Record> recordSet = new HashSet<>();

    Record tmp = it.next();
    while (tmp != null) {
      recordSet.add(tmp);
      tmp = it.next();
    }

    return recordSet;
  }

  @Override
  public Iterator project(String tableName, String attrName, boolean isDuplicateFree) {
    // get iterator
    System.out.println("selecting from table: " + tableName + " with attrName: " + attrName + " and isDuplicateFree: " + isDuplicateFree);
    Iterator iterator = select(tableName, new ComparisonPredicate(), Iterator.Mode.READ, false);
    System.out.println("Got iterator: " + iterator.toString());

//    Record rec = iterator.next();
    // call project(iterator, attrName, isDuplicateFree)
    Iterator projectIterator = project(iterator, attrName, isDuplicateFree);

    return projectIterator;
//    return null;
  }

  @Override
  public Iterator project(Iterator iterator, String attrName, boolean isDuplicateFree) {

    // Assume that attribute Name exists

    Transaction tx = FDBHelper.openTransaction(db);

    // create a tmp directory with UUID
    DirectorySubspace tmpDir = FDBHelper.createOrOpenSubspace(tx, Arrays.asList("tmp", UUID.randomUUID().toString()));

    if (isDuplicateFree){
      // iterate over attributeName and write to fdb under tmp directory and some random prefix
      Record tmp = iterator.next();
      while (tmp != null){
          Object value = tmp.getValueForGivenAttrName(attrName);
          if (value == null){
              // do nothing
          }
          else{
              // write to fdb
              FDBHelper.setSubspaceKV(tmpDir, tx, value.toString(), value);
          }
          tmp = iterator.next();
      }

      // create a new iterator over the tmp directory and return it
      //   public static Iterator getSubspaceProjectIterator(DirectorySubspace subspace, Transaction tx, boolean clearSubspace, String attributeName) {
      return getSubspaceProjectIterator(tmpDir, tx, true, attrName);
    }
    else {
      return new ProjectIterator(iterator, attrName);
    }


    //    public




//    throw new NotImplementedException();
//    return null;
  }

  @Override
  public List<Record> simpleProject(String tableName, String attrName, boolean isDuplicateFree) {
    // get iterator
    Iterator iterator = project(tableName, attrName, isDuplicateFree);
    ArrayList<Record> recordList = new ArrayList<>();
    Record tmp = iterator.next();
    while (tmp != null) {
      recordList.add(tmp);
      tmp = iterator.next();
    }
    return recordList;
//    throw new NotImplementedException();
//    return null;
  }

  @Override
  public List<Record> simpleProject(Iterator iterator, String attrName, boolean isDuplicateFree) {
    // get iterator
    Iterator projectIterator = project(iterator, attrName, isDuplicateFree);
    ArrayList<Record> recordList = new ArrayList<>();
    Record tmp = projectIterator.next();
    while (tmp != null) {
      recordList.add(tmp);
      tmp = projectIterator.next();
    }
    return recordList;
//    return null;
  }

  @Override
  public Iterator join(Iterator outerIterator, Iterator innerIterator, ComparisonPredicate predicate, Set<String> attrNames) {

    UUID uuid = UUID.randomUUID();
    String tmpDirName = uuid.toString();
    Transaction tx = FDBHelper.openTransaction(db);
    DirectorySubspace tmpDirInner = FDBHelper.createOrOpenSubspace(tx, Arrays.asList("tmp", tmpDirName, "inner"));
    // iterate over innerIterator
    Record rInnerOnce = innerIterator.next();
    int fakeHashCode = 0; // Note, the hash function of Record have collision, so we need to use a fake hashcode
    while (rInnerOnce != null) {
      // for each record, write to DirectorySubspace
      FDBHelper.setSubspaceRecord(tmpDirInner, tx, rInnerOnce, fakeHashCode++);
      System.out.println("Writing to tmpDirInner: " + rInnerOnce.toString());
      rInnerOnce = innerIterator.next();
    }

    System.out.println("Finished writing to tmpDirInner");

    BiFunction<Record, Record, Record> joinFunction = Cursor.createPredicateJoinFunction(predicate);
    DirectorySubspace tmpDirRes = FDBHelper.createOrOpenSubspace(tx, Arrays.asList("tmp", tmpDirName, "res"));
    Record rOuter = outerIterator.next();

    Iterator wtfIter = FDBHelper.getSubspaceRecordIterator(tmpDirInner, tx);
    Record wtf = wtfIter.next();
    while (wtf != null) {
      System.out.println("wtf: " + wtf.toString());
      wtf = wtfIter.next();
    }


    while (rOuter != null) {
      System.out.println("rOuter: " + rOuter.toString());
      Iterator innerIter = FDBHelper.getSubspaceRecordIterator(tmpDirInner, tx);
      Record rInner = innerIter.next();

      while (rInner != null) {
        System.out.println("rInner: " + rInner.toString());
        Record res = joinFunction.apply(rOuter, rInner);
        if (res != null) {
          System.out.println("Join result: " + res.toString());
          FDBHelper.setSubspaceRecord(tmpDirRes, tx, res, fakeHashCode++);
        }
        rInner = innerIter.next();
      }
      rOuter = outerIterator.next();
    }

    // clear tmpDirInner
    tx.clear(tmpDirInner.range());
    return FDBHelper.getSubspaceRecordIterator(tmpDirRes, tx);


      // for each record, write to DirectorySubspace

    // iterate over outerIterator
      // get record recA
      // if recA == null
        // break
      // else
        // innerIterator = getSubspaceProjectIterator(tmpDir, tx, true, attrName);
          // get record recB
          // if recB == null
            // break
         // else if predicate (A,B) == true
            // create a new record with attrNames
            // return record
          // else
            // continue


//    return null;
  }

  @Override
  public StatusCode insert(String tableName, Record record, String[] primaryKeys) {
    return null;
  }

  @Override
  public StatusCode update(String tableName, AssignmentExpression assignExp, Iterator dataSourceIterator) {
    return null;
  }

  @Override
  public StatusCode delete(String tableName, Iterator iterator) {
    return null;
  }
}
