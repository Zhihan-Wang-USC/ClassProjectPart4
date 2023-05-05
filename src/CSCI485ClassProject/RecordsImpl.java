package CSCI485ClassProject;

import CSCI485ClassProject.models.ComparisonOperator;
import CSCI485ClassProject.models.Pair;
import CSCI485ClassProject.models.Record;
import CSCI485ClassProject.models.TableMetadata;
import com.apple.foundationdb.Database;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class RecordsImpl implements Records {

  private Database db;
  private TableManagerImpl tableManager;

  private IndexesImpl indexes;

  private boolean overwrite = false;

  public RecordsImpl() {
    db = FDBHelper.initialization();
    tableManager = new TableManagerImpl();
    indexes = new IndexesImpl();

  }

  public RecordsImpl(boolean overwrite) {
    this();
    this.overwrite = overwrite;
  }


  public StatusCode insertRecordTx(String tableName, String[] primaryKeys, Object[] primaryKeysValues, String[] attrNames, Object[] attrValues, Transaction tx) {
    // Get table metadata
    TableMetadata tableMetadata = tableManager.getTableMetadata(tableName);
    if (tableMetadata == null) {
      return StatusCode.TABLE_NOT_FOUND;
    }

    // Check primaryKeys match table metadata
    Set<String> primaryKeySetOnRecord = tableMetadata.getPrimaryKeys().stream().collect(Collectors.toSet());
    Set<String> primaryKeySetOnInsert = new HashSet<>(Arrays.asList(primaryKeys));
    if (primaryKeys.length != primaryKeysValues.length || !primaryKeySetOnRecord.equals(primaryKeySetOnInsert)) {
      return StatusCode.DATA_RECORD_PRIMARY_KEYS_UNMATCHED;
    }


    // Check attrNames are valid
    if (attrNames.length != attrValues.length) {
      return StatusCode.DATA_RECORD_CREATION_ATTRIBUTES_INVALID;
    }
    Set<String> attributeNamesOnRecord = tableMetadata.getAttributes().keySet().stream().collect(Collectors.toSet());
    Set<String> attributeNamesOnInsert = new HashSet<>(Arrays.asList(attrNames));


    ArrayList<String> attrNamesArr = new ArrayList<>(Arrays.asList(attrNames));
    ArrayList<Object> attrValuesArr = new ArrayList<>(Arrays.asList(attrValues));

    for (int i = 0; i < primaryKeys.length; i++) {
      String primaryKey = primaryKeys[i];
      if (!attributeNamesOnInsert.contains(primaryKey)) {
        attrNamesArr.add(primaryKey);
        attrValuesArr.add(primaryKeysValues[i]);
        attributeNamesOnInsert.add(primaryKey);
      }
    }

    for (int i = 0; i < attrNamesArr.size(); i++) {
      String attrName = attrNamesArr.get(i);
      if (!attributeNamesOnRecord.contains(attrName)) {
        StatusCode success = tableManager.addAttributeTx(tx, tableName, attrName, FDBHelper.getType(attrValuesArr.get(i)));
        assert (success == StatusCode.SUCCESS);
      }
    }

    // Check attrTypes match table metadata
    Record insertRecord = new Record();
    // feed attrNames and attrValues into record
    Map<String, Object> mapAttributeNameToValue = IntStream.range(0, attrNamesArr.size()).boxed()
            .collect(Collectors.toMap(i -> attrNamesArr.get(i), i -> attrValuesArr.get(i)));
    HashMap<String, Object> hashMapAttributeNameToValue = new HashMap<String, Object>(mapAttributeNameToValue);
    insertRecord.setMapAttrNameToValue(hashMapAttributeNameToValue);

    // IF attribute not in record, update mapMetadata
    boolean isAttributeTypeMatched = Arrays.stream(attrNames)
            .allMatch(attrName ->
                    tableMetadata.getAttributes().get(attrName) == null ||
                            insertRecord.getTypeForGivenAttrName(attrName) == tableMetadata.getAttributes().get(attrName));
    if (!isAttributeTypeMatched) {
      return StatusCode.DATA_RECORD_CREATION_ATTRIBUTE_TYPE_UNMATCHED;
    }


    // Check primaryKeys not exist in table
    // use order in tableMetadata to ensure the order of primaryKeys
    Map<String, Object> pkMap = IntStream.range(0, primaryKeys.length).boxed()
            .collect(Collectors.toMap(i -> primaryKeys[i], i -> primaryKeysValues[i]));
    Tuple primaryKeyValueTuple = Tuple.fromList(tableMetadata.getPrimaryKeys().stream().map(pkMap::get).collect(Collectors.toList()));
    Tuple primaryKeyValueTupleL = new Tuple().add(primaryKeyValueTuple);


    RecordTransformer recordTransformer = new RecordTransformer(tableName);
    List<String> recordAttributeStorePath = recordTransformer.getRecordAttributeStorePath();
    DirectorySubspace tableDirectory = FDBHelper.createOrOpenSubspace(tx, recordAttributeStorePath);


    Tuple attributeKeyTuple = recordTransformer.getTableRecordAttributeKeyTuple(primaryKeyValueTupleL, tableMetadata.getPrimaryKeys().get(0));

    FDBKVPair pair = FDBHelper.getCertainKeyValuePairInSubdirectory(
            tableDirectory,
            tx,
            attributeKeyTuple,
            recordAttributeStorePath);
//
//    FDBKVPair pair = FDBHelper.getCertainKeyValuePairInSubdirectory(
//            tableDirectory,
//            tx,
//            RecordTransformer.getTableRecordExistTuple(primaryKeyValueTuple, tableMetadata.getPrimaryKeys()),
//            recordAttributeStorePath);

    if (pair != null) {
      // KVPair exists
      FDBHelper.abortTransaction(tx);
      return StatusCode.DATA_RECORD_CREATION_RECORD_ALREADY_EXISTS;
    }

    // Insert record into table : convert to FDBKVPairs
    List<FDBKVPair> pairs = recordTransformer.convertToFDBKVPairs(insertRecord, primaryKeyValueTuple);
//    List<FDBKVPair> augmentedPairs = recordTransformer.augmentWithPrimaryKeyValue(pairs, primaryKeyValueTuple);
    // Insert KVPair into FDB
    for (FDBKVPair kvPair : pairs) {
      FDBHelper.setFDBKVPair(tableDirectory, tx, kvPair);
    }


    // Insert index
    for (Map.Entry<String, Object> entry : mapAttributeNameToValue.entrySet()) {
      String attrName = entry.getKey();
      Object attrValue = entry.getValue();
      if (indexes.isIndexExist(tx, tableName, attrName)) {
        indexes.insertIndex(tx, tableName, attrName, attrValue, primaryKeyValueTupleL);
      }
    }

    return StatusCode.SUCCESS;
  }

  @Override
  public StatusCode insertRecord(String tableName, String[] primaryKeys, Object[] primaryKeysValues, String[] attrNames, Object[] attrValues) {
    // Get table metadata
    TableMetadata tableMetadata = tableManager.getTableMetadata(tableName);
    if (tableMetadata == null) {
      return StatusCode.TABLE_NOT_FOUND;
    }

    // Check primaryKeys match table metadata
    Set<String> primaryKeySetOnRecord = tableMetadata.getPrimaryKeys().stream().collect(Collectors.toSet());
    Set<String> primaryKeySetOnInsert = new HashSet<>(Arrays.asList(primaryKeys));
    if (primaryKeys.length != primaryKeysValues.length || !primaryKeySetOnRecord.equals(primaryKeySetOnInsert)) {
      return StatusCode.DATA_RECORD_PRIMARY_KEYS_UNMATCHED;
    }


    // Check attrNames are valid
    if (attrNames.length != attrValues.length) {
      return StatusCode.DATA_RECORD_CREATION_ATTRIBUTES_INVALID;
    }
    Set<String> attributeNamesOnRecord = tableMetadata.getAttributes().keySet().stream().collect(Collectors.toSet());
    Set<String> attributeNamesOnInsert = new HashSet<>(Arrays.asList(attrNames));


    ArrayList<String> attrNamesArr = new ArrayList<>(Arrays.asList(attrNames));
    ArrayList<Object> attrValuesArr = new ArrayList<>(Arrays.asList(attrValues));

    for (int i = 0; i < primaryKeys.length; i++) {
      String primaryKey = primaryKeys[i];
      if (!attributeNamesOnInsert.contains(primaryKey)) {
        attrNamesArr.add(primaryKey);
        attrValuesArr.add(primaryKeysValues[i]);
        attributeNamesOnInsert.add(primaryKey);
      }
    }

    Transaction tx = db.createTransaction();

    for (int i = 0; i < attrNamesArr.size(); i++) {
      String attrName = attrNamesArr.get(i);
      if (!attributeNamesOnRecord.contains(attrName)) {
        StatusCode success = tableManager.addAttributeTx(tx, tableName, attrName, FDBHelper.getType(attrValuesArr.get(i)));
        assert (success == StatusCode.SUCCESS);
      }
    }

    // Check attrTypes match table metadata
    Record insertRecord = new Record();
    // feed attrNames and attrValues into record
    Map<String, Object> mapAttributeNameToValue = IntStream.range(0, attrNamesArr.size()).boxed()
            .collect(Collectors.toMap(i -> attrNamesArr.get(i), i -> attrValuesArr.get(i)));
    HashMap<String, Object> hashMapAttributeNameToValue = new HashMap<String, Object>(mapAttributeNameToValue);
    insertRecord.setMapAttrNameToValue(hashMapAttributeNameToValue);

    // IF attribute not in record, update mapMetadata
    boolean isAttributeTypeMatched = Arrays.stream(attrNames)
            .allMatch(attrName ->
                    tableMetadata.getAttributes().get(attrName) == null ||
                            insertRecord.getTypeForGivenAttrName(attrName) == tableMetadata.getAttributes().get(attrName));
    if (!isAttributeTypeMatched) {
      return StatusCode.DATA_RECORD_CREATION_ATTRIBUTE_TYPE_UNMATCHED;
    }


    // Check primaryKeys not exist in table
    // use order in tableMetadata to ensure the order of primaryKeys
    Map<String, Object> pkMap = IntStream.range(0, primaryKeys.length).boxed()
            .collect(Collectors.toMap(i -> primaryKeys[i], i -> primaryKeysValues[i]));
    Tuple primaryKeyValueTuple = Tuple.fromList(tableMetadata.getPrimaryKeys().stream().map(pkMap::get).collect(Collectors.toList()));
    Tuple primaryKeyValueTupleL = new Tuple().add(primaryKeyValueTuple);


    RecordTransformer recordTransformer = new RecordTransformer(tableName);
    List<String> recordAttributeStorePath = recordTransformer.getRecordAttributeStorePath();
    DirectorySubspace tableDirectory = FDBHelper.createOrOpenSubspace(tx, recordAttributeStorePath);


    Tuple attributeKeyTuple = recordTransformer.getTableRecordAttributeKeyTuple(primaryKeyValueTupleL, tableMetadata.getPrimaryKeys().get(0));

    FDBKVPair pair = FDBHelper.getCertainKeyValuePairInSubdirectory(
            tableDirectory,
            tx,
            attributeKeyTuple,
            recordAttributeStorePath);
//
//    FDBKVPair pair = FDBHelper.getCertainKeyValuePairInSubdirectory(
//            tableDirectory,
//            tx,
//            RecordTransformer.getTableRecordExistTuple(primaryKeyValueTuple, tableMetadata.getPrimaryKeys()),
//            recordAttributeStorePath);

    if (pair != null && overwrite == false) {
      // KVPair exists
      FDBHelper.abortTransaction(tx);
      return StatusCode.DATA_RECORD_CREATION_RECORD_ALREADY_EXISTS;
    }

    // Insert record into table : convert to FDBKVPairs
    List<FDBKVPair> pairs = recordTransformer.convertToFDBKVPairs(insertRecord, primaryKeyValueTuple);
//    List<FDBKVPair> augmentedPairs = recordTransformer.augmentWithPrimaryKeyValue(pairs, primaryKeyValueTuple);
    // Insert KVPair into FDB
    for (FDBKVPair kvPair : pairs) {
      FDBHelper.setFDBKVPair(tableDirectory, tx, kvPair);
    }


    // Insert index
    for (Map.Entry<String, Object> entry : mapAttributeNameToValue.entrySet()) {
      String attrName = entry.getKey();
      Object attrValue = entry.getValue();
      if (indexes.isIndexExist(tx, tableName, attrName)) {
        indexes.insertIndex(tx, tableName, attrName, attrValue, primaryKeyValueTupleL);
      }
    }


    FDBHelper.commitTransaction(tx);

    return StatusCode.SUCCESS;
  }

  @Override
  public Cursor openCursor(String tableName, String attrName, Object attrValue, ComparisonOperator operator, Cursor.Mode mode, boolean isUsingIndex) {
    Cursor cursor = new Cursor(tableName, attrName, attrValue, operator, mode, isUsingIndex);
    if (cursor.getCursorStatus() == Cursor.CursorStatus.ERROR) {
      return null;
    }
    return cursor;
  }

  @Override
  public Cursor openCursor(String tableName, Cursor.Mode mode) {
    Cursor cursor = new Cursor(tableName, mode);
    return cursor;
  }

  @Override
  public Record getFirst(Cursor cursor) {
    if (cursor == null) {
      return null;
    }
    cursor = cursor.moveToFirst();
    if (cursor == null) {
      return null;
    }
    return cursor.getCurrentRecord();
  }

  @Override
  public Record getLast(Cursor cursor) {
    if (cursor == null) {
      return null;
    }
    cursor = cursor.moveToLast();
    if (cursor == null) {
      return null;
    }
    return cursor.getCurrentRecord();
  }

  @Override
  public Record getNext(Cursor cursor) {
    if (cursor == null) {
      return null;
    }
    return cursor.getNextRecord();
  }

  @Override
  public Record getPrevious(Cursor cursor) {
    if (cursor == null) {
      return null;
    }
    return cursor.getPreviousRecord();
  }

  @Override
  public StatusCode updateRecord(Cursor cursor, String[] attrNames, Object[] attrValues) {
    return cursor.updateRecord(attrNames, attrValues);
  }

  @Override
  public StatusCode deleteRecord(Cursor cursor) {
    return cursor.dropRecord();
  }

  @Override
  public StatusCode commitCursor(Cursor cursor) {
    return cursor.commit();
  }

  @Override
  public StatusCode abortCursor(Cursor cursor) {
    return null;
  }

  @Override
  public StatusCode deleteDataRecord(String tableName, String[] attrNames, Object[] attrValues) {
    return null;
  }
}
