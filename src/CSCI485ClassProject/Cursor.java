package CSCI485ClassProject;

import CSCI485ClassProject.models.*;
import com.apple.foundationdb.Database;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.*;
import com.sun.org.apache.xpath.internal.operations.Bool;

import javax.swing.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

public class Cursor {
  public enum Mode {
    READ,
    READ_WRITE
  }

  // your code here
  private String tableName;
  private ComparisonOperator operator;
  private Mode mode;
  private boolean isUsingIndex;


  private Transaction tx;
  private Database db;

  private TableManagerImpl tableManager;

  private IndexesImpl indexes;

  private RecordTransformer recordTransformer;

  private AsyncIterator<KeyValue> iterator;

  private DirectorySubspace tableDirectory;

  private List<String> recordAttributeStorePath;

  private Direction direction = Direction.UNSET;

  enum Direction {
    FIRST_2_LAST,
    LAST_2_FIRST,
    UNSET
  }

  private CursorStatus cursorStatus = CursorStatus.UNINITIALIZED;

  enum CursorStatus {
    UNINITIALIZED,
    DIRECTION_SET,
    ITERATOR_INITIALIZED,
    EOF,
    COMMITTED,
    ERROR
  }

  //  private
  private Tuple currentPrimaryKeyValueTuple = null;
  private Record currentRecord = null;

  private String attributeNameOfInterest = null;

  private Object attributeValueOfInterest = null;
  private Function<Record, Boolean> predicateFunction = null;

  private Function<Object, Boolean> predicateFunctionO = null;

  private static Function<Record, Boolean> createPredicateFunction(String key, ComparisonOperator comparisonOperator, Object compareValue) {
    if (compareValue instanceof Integer) {
      compareValue = ((Integer) compareValue).longValue();
    }
    Object finalCompareValue = compareValue;
    return record -> {
      Map<String, Object> map = record.getMapAttrNameToValueValue();
      if (!map.containsKey(key)) {
        return false;
      }
      Object value = map.get(key);
      if (value == null) {
        return false;
      }
      switch (comparisonOperator) {
        case EQUAL_TO:
          return value.equals(finalCompareValue);
        case GREATER_THAN_OR_EQUAL_TO:
          if (value instanceof Comparable) {
            return ((Comparable) value).compareTo(finalCompareValue) >= 0;
          } else {
            throw new IllegalArgumentException("Value is not comparable");
          }
        case LESS_THAN_OR_EQUAL_TO:
          if (value instanceof Comparable) {
            return ((Comparable) value).compareTo(finalCompareValue) < 0;
          } else {
            throw new IllegalArgumentException("Value is not comparable");
          }
        case GREATER_THAN:
          if (value instanceof Comparable) {
            return ((Comparable) value).compareTo(finalCompareValue) > 0;
          } else {
            throw new IllegalArgumentException("Value is not comparable");
          }
        case LESS_THAN:
          if (value instanceof Comparable) {
            return ((Comparable) value).compareTo(finalCompareValue) <= 0;
          } else {
            throw new IllegalArgumentException("Value is not comparable");
          }
        default:
          throw new IllegalArgumentException("Invalid comparison operator");
      }
    };
  }

  public static BiFunction<Record, Record, Record> createPredicateJoinFunction(ComparisonPredicate comparisonPredicate) {
    if (comparisonPredicate.validate() != StatusCode.PREDICATE_OR_EXPRESSION_VALID) {
      throw new IllegalArgumentException("Predicate is not valid");
    }
    String leftKey = comparisonPredicate.getLeftHandSideAttrName();
    String rightKey = comparisonPredicate.getRightHandSideAttrName();
    AttributeType leftType = comparisonPredicate.getLeftHandSideAttrType();
    AttributeType rightType = comparisonPredicate.getRightHandSideAttrType();
    ComparisonOperator comparisonOperator = comparisonPredicate.getOperator();
    Long rhsMult = ((Integer)comparisonPredicate.getRightHandSideValue()).longValue();


    if (leftType != rightType) {
      throw new IllegalArgumentException("Left and right attribute types are not the same");
    }
    if (leftType != AttributeType.INT) {
      throw new IllegalArgumentException("Left and right attribute types are not integers, not supported for now");
    }

    if (comparisonPredicate.getPredicateType() != ComparisonPredicate.Type.TWO_ATTRS) {
      throw new IllegalArgumentException("Predicate is not a two attribute predicate");
    }


    return (a, b) -> {
      Map<String, Object> map = a.getMapAttrNameToValueValue();
      if (!map.containsKey(leftKey)) {
        System.out.println("Left key not found");
        return null;
      }
      Object leftKeyAttrValue = map.get(leftKey);

      Map<String, Object> map2 = b.getMapAttrNameToValueValue();
      if (!map2.containsKey(rightKey)) {
        System.out.println("Right key not found");
        return null;
      }
      Object rightKeyAttrValue = map2.get(rightKey);
      if (leftKeyAttrValue == null || rightKeyAttrValue == null) {
        System.out.println("One of the values is null");
        return null;
      }

      Long lhs = ((Long) leftKeyAttrValue);
      Long rhs = ((Long) rightKeyAttrValue);
      rhs = rhsMult * rhs;


      Boolean result = false;
      switch (comparisonOperator) {
        case EQUAL_TO:
          result = lhs.equals(rhs);
          break;
        case GREATER_THAN_OR_EQUAL_TO:
          result = lhs.compareTo(rhs) >= 0;
          break;
        case LESS_THAN_OR_EQUAL_TO:
          result = lhs.compareTo(rhs) < 0;
          break;
        case GREATER_THAN:
          result = lhs.compareTo(rhs) > 0;
          break;
        case LESS_THAN:
          result = lhs.compareTo(rhs) <= 0;
          break;
        default:
          throw new IllegalArgumentException("Invalid comparison operator");
      }

      String aTableName = "Employee";
      String bTableName = "Department";

      if (result) {
        Record record = new Record();
        Set<String> akeys = a.getMapAttrNameToValueValue().keySet();
        Set<String> bkeys = b.getMapAttrNameToValueValue().keySet();
        Set<String> sharedKeys = new HashSet<>(akeys);
        sharedKeys.retainAll(bkeys);

        //iterate over a keys
        for (String key : akeys){
          if (sharedKeys.contains(key)){
              record.setAttrNameAndValue(aTableName + "." + key, a.getValueForGivenAttrName(key));
          }
          else{
                record.setAttrNameAndValue(key, a.getValueForGivenAttrName(key));
          }
        }

        for (String key : bkeys){
          if (sharedKeys.contains(key)){
              record.setAttrNameAndValue(bTableName + "." + key, b.getValueForGivenAttrName(key));
          }
          else{
                record.setAttrNameAndValue(key, b.getValueForGivenAttrName(key));
          }
        }

        return record;
      } else {
        return null;
      }
    };
  }

  private static Function<Object, Boolean> createPredicateFunctionO(ComparisonOperator comparisonOperator, Object compareValue) {
    if (compareValue instanceof Integer) {
      compareValue = ((Integer) compareValue).longValue();
    }
    Object finalCompareValue = compareValue;
    return value -> {
      if (value == null) {
        return false;
      }
      switch (comparisonOperator) {
        case EQUAL_TO:
          return value.equals(finalCompareValue);
        case GREATER_THAN_OR_EQUAL_TO:
          if (value instanceof Comparable) {
            return ((Comparable) value).compareTo(finalCompareValue) >= 0;
          } else {
            throw new IllegalArgumentException("Value is not comparable");
          }
        case LESS_THAN_OR_EQUAL_TO:
          if (value instanceof Comparable) {
            return ((Comparable) value).compareTo(finalCompareValue) < 0;
          } else {
            throw new IllegalArgumentException("Value is not comparable");
          }
        case GREATER_THAN:
          if (value instanceof Comparable) {
            return ((Comparable) value).compareTo(finalCompareValue) > 0;
          } else {
            throw new IllegalArgumentException("Value is not comparable");
          }
        case LESS_THAN:
          if (value instanceof Comparable) {
            return ((Comparable) value).compareTo(finalCompareValue) <= 0;
          } else {
            throw new IllegalArgumentException("Value is not comparable");
          }
        default:
          throw new IllegalArgumentException("Invalid comparison operator");
      }
    };
  }

  private boolean isInitialized = false;
  private boolean committed = false;


  /****
   * Initialize the database and transaction, and table manager
   */
  private void init(String tableName) {
    db = FDBHelper.initialization();
    tx = FDBHelper.openTransaction(db);
    tableManager = new TableManagerImpl();
    recordTransformer = new RecordTransformer(tableName);
    recordAttributeStorePath = recordTransformer.getRecordAttributeStorePath();
    tableDirectory = FDBHelper.createOrOpenSubspace(tx, recordAttributeStorePath);
    indexes = new IndexesImpl();
  }

  public Cursor(String tableName, String attrName, Object attrValue, ComparisonOperator operator, Mode mode, boolean isUsingIndex) {
    this.tableName = tableName;
    this.operator = operator;
    this.mode = mode;
    this.isUsingIndex = isUsingIndex; // we don't need this for now
    this.predicateFunction = createPredicateFunction(attrName, operator, attrValue);
    this.predicateFunctionO = createPredicateFunctionO(operator, attrValue);
    this.attributeNameOfInterest = attrName;
    this.attributeValueOfInterest = attrValue;

//      System.out.println("attributeNameOfInterest " + attributeNameOfInterest);
    init(tableName);

    if (isUsingIndex != false && indexes.isIndexExist(tx, tableName, attributeNameOfInterest) == false) {
      System.out.println("Index does not exist");
      this.cursorStatus = CursorStatus.ERROR;
    }

  }

  public CursorStatus getCursorStatus() {
    return cursorStatus;
  }

  public Cursor(String tableName, Mode mode) {
    this.tableName = tableName;
    this.mode = mode;
    this.recordTransformer = new RecordTransformer(tableName);
    this.predicateFunction = null;
    init(tableName);

    TableMetadata tableMetadata = tableManager.getTableMetadataTx(tx, tableName);
    if (tableMetadata == null) {
      this.iterator = null;
    }

  }

  public Cursor initializeCursor() {
    if (cursorStatus != CursorStatus.DIRECTION_SET) {
      System.out.println("Cursor not ready to be initialized or already initialized");
      return null;
    }

    TableMetadata tableMetadata = tableManager.getTableMetadataTx(tx, tableName);

    if (isUsingIndex == false) {
      byte[] keyPrefixB = null;

      if (predicateFunction == null) {
        List<String> primaryKeys = tableMetadata.getPrimaryKeys();

        Tuple pkExistPrefix = recordTransformer.getTableRecordExistTuplePrefix(primaryKeys);

        keyPrefixB = tableDirectory.pack(pkExistPrefix);
      } else {
//      System.out.println("attributeNameOfInterest " + attributeNameOfInterest);
        keyPrefixB = tableDirectory.pack(new Tuple().add(attributeNameOfInterest));
//      System.out.println("keyPrefixB predicated" + Tuple.fromBytes(keyPrefixB));
      }

      Range range = Range.startsWith(keyPrefixB);

      assert (direction != Direction.UNSET);

      if (direction == Direction.FIRST_2_LAST) {
        AsyncIterator<KeyValue> iterator = tx.getRange(range).iterator();
        this.iterator = iterator;
      } else if (direction == Direction.LAST_2_FIRST) {
        AsyncIterator<KeyValue> iterator = tx.getRange(range, DBConf.MAX_RECORD, true).iterator();
        this.iterator = iterator;
      }
    } else {
      // now we are using index

      // make sure the index exists
      if (indexes.isIndexExist(tx, tableName, attributeNameOfInterest) == false) {
        System.out.println("Index does not exist");
        return null;
      } else {
//        System.out.println("Index " + attributeNameOfInterest + " exists");
      }

      IndexType indexType = indexes.getIndexType(tx, tableName, attributeNameOfInterest);

//      System.out.println("indexType " + indexType);

      Range range = null;
      // if Equals_to
      if (this.operator == ComparisonOperator.EQUAL_TO) {
        Tuple indexKeyPrefix = indexes.buildKeyTuplePrefixToIndex(tableName, attributeNameOfInterest, indexType, attributeValueOfInterest);
        range = Range.startsWith(indexKeyPrefix.pack());
      } else if (indexType == IndexType.NON_CLUSTERED_B_PLUS_TREE_INDEX) {
        Tuple indexKeyPrefix = indexes.buildKeyTuplePrefixToIndex(tableName, attributeNameOfInterest, indexType);
        KeySelector generalStart = KeySelector.firstGreaterThan(indexKeyPrefix.pack());
        byte[] startB = tx.getKey(generalStart).join();

        KeySelector generalEnd = KeySelector.lastLessOrEqual(Utils.getLastKeyWithPrefix(indexKeyPrefix.pack()));
        byte[] endB = tx.getKey(generalEnd).join();

        Tuple indexKeyPrefixValue = indexes.buildKeyTuplePrefixToIndex(tableName, attributeNameOfInterest, indexType, attributeValueOfInterest);
        if (this.operator == ComparisonOperator.GREATER_THAN_OR_EQUAL_TO) {
          KeySelector start = KeySelector.firstGreaterThan(indexKeyPrefixValue.pack());
          startB = tx.getKey(start).join();
        } else if (this.operator == ComparisonOperator.GREATER_THAN) {
          KeySelector start = KeySelector.firstGreaterThan(indexKeyPrefixValue.pack());
          startB = tx.getKey(start).join();
        } else if (this.operator == ComparisonOperator.LESS_THAN_OR_EQUAL_TO) {
          KeySelector end = KeySelector.lastLessOrEqual(indexKeyPrefixValue.pack());
          endB = tx.getKey(end).join();
        } else if (this.operator == ComparisonOperator.LESS_THAN) {
          KeySelector end = KeySelector.lastLessThan(indexKeyPrefixValue.pack());
          endB = tx.getKey(end).join();
        }

        endB = Utils.getLastKeyWithPrefix(endB);
        range = new Range(startB, endB);

      } else {
        assert (indexType == IndexType.NON_CLUSTERED_HASH_INDEX);
        Tuple indexKeyPrefix = indexes.buildKeyTuplePrefixToIndex(tableName, attributeNameOfInterest, indexType);
        range = Range.startsWith(indexKeyPrefix.pack());
      }

      if (tx.getRange(range).iterator().hasNext() == false) {
        System.out.println("No record found");
        return null;
      }

      assert (direction != Direction.UNSET);

      if (direction == Direction.FIRST_2_LAST) {
        AsyncIterator<KeyValue> iterator = tx.getRange(range).iterator();
        this.iterator = iterator;
      } else if (direction == Direction.LAST_2_FIRST) {
        AsyncIterator<KeyValue> iterator = tx.getRange(range, DBConf.MAX_RECORD, true).iterator();
        this.iterator = iterator;
      }

    }


    cursorStatus = CursorStatus.ITERATOR_INITIALIZED;

    return this;
  }

  public Cursor moveToFirst() {
    if (direction != Direction.UNSET || cursorStatus != CursorStatus.UNINITIALIZED) {
      iterator = null;
      cursorStatus = CursorStatus.ERROR;
      return null;
    }

    direction = Direction.FIRST_2_LAST;
    cursorStatus = CursorStatus.DIRECTION_SET;
    return initializeCursor();
  }

  public Cursor moveToLast() {
    if (direction != Direction.UNSET || cursorStatus != CursorStatus.UNINITIALIZED) {
      this.iterator = null;
      cursorStatus = CursorStatus.ERROR;
      return null;
    }

    direction = Direction.LAST_2_FIRST;
    cursorStatus = CursorStatus.DIRECTION_SET;
    return initializeCursor();
  }

  public Record getNextRecord() {
    if (direction != Direction.FIRST_2_LAST) {
      cursorStatus = CursorStatus.ERROR;
      return null;
    }

    if (cursorStatus == CursorStatus.EOF) {
      return null;
    }

    if (cursorStatus != CursorStatus.ITERATOR_INITIALIZED) {
      cursorStatus = CursorStatus.ERROR;
      return null;
    }

    currentPrimaryKeyValueTuple = null;
    currentRecord = null;
    return getCurrentRecord();

  }

  public Record getPreviousRecord() {
    if (direction != Direction.LAST_2_FIRST) {
      cursorStatus = CursorStatus.ERROR;
      return null;
    }

    if (cursorStatus == CursorStatus.EOF) {
      return null;
    }

    if (cursorStatus != CursorStatus.ITERATOR_INITIALIZED) {
      cursorStatus = CursorStatus.ERROR;
      return null;
    }

    currentPrimaryKeyValueTuple = null;
    currentRecord = null;
    return getCurrentRecord();
  }

  private Tuple getNextPKTuple(AsyncIterator<KeyValue> iterator, boolean isUsingIndex) {
    if (iterator == null) {
      System.out.println("wtf iterator is null");
      cursorStatus = CursorStatus.ERROR;
      return null;
    }

    if (!iterator.hasNext()) {
      cursorStatus = CursorStatus.EOF;
      return null;
    }

    KeyValue nextKeyValue = null;

    nextKeyValue = iterator.next();
    if (nextKeyValue == null) {
      System.out.println("wtf, get null from iterator.next()");
      cursorStatus = CursorStatus.ERROR;
      return null;
    }

    if (!isUsingIndex) {
      Tuple recordExistTuple = Tuple.fromBytes(nextKeyValue.getKey());
      if (recordExistTuple == null) {
        System.out.println("wtf, get null from Tuple.fromBytes(nextKeyValue.getKey())");
        cursorStatus = CursorStatus.ERROR;
        return null;
      }

      Tuple primaryKeyValueTuple = recordTransformer.getPrimaryKeyValueTuple(recordExistTuple);
      return primaryKeyValueTuple;
    } else {
      Tuple primaryKeyValueTuple = Tuple.fromBytes(nextKeyValue.getValue());
      return primaryKeyValueTuple;
    }


  }

  public Record getCurrentRecord() {

    if (cursorStatus == CursorStatus.EOF) {
      return null;
    }

    if (cursorStatus != CursorStatus.ITERATOR_INITIALIZED) {
      cursorStatus = CursorStatus.ERROR;
      return null;
    }

    if (currentRecord != null) {
      return currentRecord;
    }

    Tuple primaryKeyValueTuple = getNextPKTuple(iterator, isUsingIndex);
    if (primaryKeyValueTuple == null) {
      return null;
    }

    if (predicateFunctionO == null) {
      currentPrimaryKeyValueTuple = primaryKeyValueTuple;
      currentRecord = getRecordByPrimaryKeyValueTuple(primaryKeyValueTuple);
      return currentRecord;
    } else {
      Object attributeValue = getAttrValByPrimaryKeyValueTupleAndAttrName(primaryKeyValueTuple, attributeNameOfInterest);
      while (attributeValue != null && !predicateFunctionO.apply(attributeValue)) {
        primaryKeyValueTuple = getNextPKTuple(iterator, isUsingIndex);
        if (primaryKeyValueTuple == null) {
          return null;
        }
        attributeValue = getAttrValByPrimaryKeyValueTupleAndAttrName(primaryKeyValueTuple, attributeNameOfInterest);
      }
      currentPrimaryKeyValueTuple = primaryKeyValueTuple;
      currentRecord = getRecordByPrimaryKeyValueTuple(primaryKeyValueTuple);
      return currentRecord;
    }

  }

  private Record getRecordByPrimaryKeyValueTuple(Tuple primaryKeyValueTuple) {
    TableMetadata tableMetadata = tableManager.getTableMetadataTx(tx, tableName);
    Record currentRecord = new Record();
    for (String attributeName : tableMetadata.getAttributes().keySet()) {
      Tuple attributeKeyTuple = recordTransformer.getTableRecordAttributeKeyTuple(primaryKeyValueTuple, attributeName);
      FDBKVPair fdbkvPair = FDBHelper.getCertainKeyValuePairInSubdirectory(
              tableDirectory,
              tx,
              attributeKeyTuple,
              recordAttributeStorePath);
      if (fdbkvPair == null) {
        currentRecord.setAttrNameAndValue(attributeName, null);
      } else {
        Tuple attributeValueTuple = fdbkvPair.getValue();
        Object attributeValue = attributeValueTuple.get(0);
        currentRecord.setAttrNameAndValue(attributeName, attributeValue);
      }
    }
    return currentRecord;
  }

  private Object getAttrValByPrimaryKeyValueTupleAndAttrName(Tuple primaryKeyValueTuple, String attributeName) {
    Tuple attributeKeyTuple = recordTransformer.getTableRecordAttributeKeyTuple(primaryKeyValueTuple, attributeName);
    FDBKVPair fdbkvPair = FDBHelper.getCertainKeyValuePairInSubdirectory(
            tableDirectory,
            tx,
            attributeKeyTuple,
            recordAttributeStorePath);
    if (fdbkvPair == null) {
      return null;
    }
    Tuple attributeValueTuple = fdbkvPair.getValue();
    Object attributeValue = attributeValueTuple.get(0);
    return attributeValue;

  }

  private Object getRecordValueObjectByAttributeKeyTuple(Tuple keyValueTuple) {
    TableMetadata tableMetadata = tableManager.getTableMetadataTx(tx, tableName);
    byte[] valBytes = tx.get(keyValueTuple.pack()).join();
    if (valBytes == null) {
      return null;
    }
    Tuple value = Tuple.fromBytes(valBytes);

//    FDBKVPair fdbkvPair = FDBHelper.getCertainKeyValuePairInSubdirectory(
//            tableDirectory,
//            tx,
//            keyValueTuple,
//            recordAttributeStorePath);

    return value.get(0);
  }

  public StatusCode dropRecord() {
    if (cursorStatus == CursorStatus.EOF) {
      return StatusCode.CURSOR_REACH_TO_EOF;
    }
    if (currentPrimaryKeyValueTuple == null) {
      return StatusCode.CURSOR_INVALID;
    }
    Tuple primaryKeyValueTuple = currentPrimaryKeyValueTuple;
    TableMetadata tableMetadata = tableManager.getTableMetadataTx(tx, tableName);
    for (String attributeName : tableMetadata.getAttributes().keySet()) {
      Tuple attributeKeyTuple = recordTransformer.getTableRecordAttributeKeyTuple(primaryKeyValueTuple, attributeName);
      FDBKVPair fdbkvPair = FDBHelper.getCertainKeyValuePairInSubdirectory(
              tableDirectory,
              tx,
              attributeKeyTuple,
              recordAttributeStorePath);
      if (fdbkvPair == null) {
        continue;
      }
//      System.out.println(fdbkvPair);
//      System.out.println(fdbkvPair.getKey());
      FDBHelper.removeKeyValuePair(tx, tableDirectory, fdbkvPair.getKey());

//      System.out.println("delete attribute " + attributeName + " of record " + fdbkvPair.getValue().get(0) );

      indexes.deleteIndex(tx, tableName, attributeName, fdbkvPair.getValue().get(0), primaryKeyValueTuple);

      byte[] keyPrefixB = tableDirectory.pack(RecordTransformer.getTableRecordAttributeKeyTuplePrefix(attributeName));
      Range range = Range.startsWith(keyPrefixB);
      AsyncIterator<KeyValue> iterator = tx.getRange(range).iterator();
      if (!iterator.hasNext()) {
        // the record we are deleating are the only record that have the attribute, so we need to shrink table metadata
        tableManager.dropAttributeTx(tx, tableName, attributeName);
      }
    }
    return StatusCode.SUCCESS;
  }

  public StatusCode commit() {
    if (tx == null) {
      return StatusCode.CURSOR_INVALID;
    }
    if (cursorStatus == CursorStatus.EOF || cursorStatus == CursorStatus.ITERATOR_INITIALIZED) {
      assert (FDBHelper.commitTransaction(tx));
      cursorStatus = CursorStatus.COMMITTED;
      return StatusCode.SUCCESS;
    }
    System.out.println("Cursor status is" + cursorStatus + " not ready to commit");
    return StatusCode.CURSOR_INVALID;
  }

  public StatusCode updateRecord(String[] attrNames, Object[] attrValues) {

    if (cursorStatus == CursorStatus.EOF) {
      return StatusCode.CURSOR_REACH_TO_EOF;
    }
    if (cursorStatus != CursorStatus.ITERATOR_INITIALIZED) {
      return StatusCode.CURSOR_NOT_INITIALIZED;
    }

    TableMetadata tableMetadata = tableManager.getTableMetadataTx(tx, tableName);
    Record record = currentRecord;

    for (int i = 0; i < attrNames.length; i++) {
      String attrName = attrNames[i];
      Object attrValue = attrValues[i];
      if (!tableMetadata.getAttributes().containsKey(attrName)) {
        return StatusCode.CURSOR_UPDATE_ATTRIBUTE_NOT_FOUND;
      }
      record.setAttrNameAndValue(attrName, attrValue);
    }

    // IF attribute not in record, update mapMetadata
    boolean isAttributeTypeMatched = Arrays.stream(attrNames)
            .allMatch(attrName ->
                    tableMetadata.getAttributes().get(attrName) == null ||
                            record.getTypeForGivenAttrName(attrName) == tableMetadata.getAttributes().get(attrName));
    if (!isAttributeTypeMatched) {
      return StatusCode.DATA_RECORD_CREATION_ATTRIBUTE_TYPE_UNMATCHED;
    }

    Map<String, Object> pkMap = record.getMapAttrNameToValueValue();

    Tuple primaryKeyValueTuple = Tuple.fromList(tableMetadata.getPrimaryKeys().stream().map(pkMap::get).collect(Collectors.toList()));
    Tuple primaryKeyValueTupleL = new Tuple().add(primaryKeyValueTuple);

    // Drop old record
    Tuple oldPrimaryKeyValueTuple = currentPrimaryKeyValueTuple;
    for (String attributeName : tableMetadata.getAttributes().keySet()) {
      Tuple attributeKeyTuple = recordTransformer.getTableRecordAttributeKeyTuple(oldPrimaryKeyValueTuple, attributeName);
      FDBKVPair fdbkvPair = FDBHelper.getCertainKeyValuePairInSubdirectory(
              tableDirectory,
              tx,
              attributeKeyTuple,
              recordAttributeStorePath);
      if (fdbkvPair == null) {
        continue;
      }
//      System.out.println(fdbkvPair);
//      System.out.println(fdbkvPair.getKey());
      FDBHelper.removeKeyValuePair(tx, tableDirectory, fdbkvPair.getKey());
    }

    // Insert new record
    List<FDBKVPair> pairs = recordTransformer.convertToFDBKVPairs(record, primaryKeyValueTuple);
    for (FDBKVPair kvPair : pairs) {
      FDBHelper.setFDBKVPair(tableDirectory, tx, kvPair);
    }

    return StatusCode.SUCCESS;
  }

}
