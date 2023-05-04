package CSCI485ClassProject;

import CSCI485ClassProject.models.IndexType;
import CSCI485ClassProject.models.TableMetadata;
import com.apple.foundationdb.Database;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;

import javax.swing.*;
import javax.xml.crypto.Data;
import java.util.List;
import java.util.Optional;

public class IndexesImpl implements Indexes{

  private Database db;

  private TableManagerImpl tableManager;
  public IndexesImpl() {
      db = FDBHelper.initialization();
      tableManager = new TableManagerImpl();
  }

  public boolean isIndexExist(Transaction tx, String tableName, String attrName) {
    Tuple keyTuple = buildKeyTupleToIndexType(tableName, attrName);
    byte[] value = tx.get(keyTuple.pack()).join();
    if (value == null) {
      return false;
    }
    Tuple valueTuple = Tuple.fromBytes(value);

    return true;
  }

  public IndexType getIndexType(Transaction tx, String tableName, String attrName) {
    Tuple keyTuple = buildKeyTupleToIndexType(tableName, attrName);
    byte[] value = tx.get(keyTuple.pack()).join();
    if (value == null) {
      return null;
    }
    Tuple valueTuple = Tuple.fromBytes(value);
//    System.out.println("Index type for " + attrName + " : " + valueTuple.getString(0));
    return IndexType.String2IndexType(valueTuple.getString(0));
  }

  private boolean writeIndexType(Transaction tx, String tableName, String attrName, IndexType indexType) {
    Tuple keyTuple = buildKeyTupleToIndexType(tableName, attrName);
    Tuple valueTuple = new Tuple().add(IndexType.IndexType2String(indexType));
    tx.set(keyTuple.pack(), valueTuple.pack());
    return true;
  }

  public Tuple buildKeyTupleToIndexType(String tableName, String attrName) {
    Tuple keyTuple = new Tuple().add(tableName).add(attrName).add("IndexType");
    return keyTuple;
  }

  public Tuple buildKeyTuplePrefixToIndex(String tableName, String attrName, IndexType indexType) {
    Tuple keyTuple = new Tuple().add(tableName).add(attrName).add(IndexType.IndexType2String(indexType));
    return keyTuple;
  }

  public Tuple buildKeyTuplePrefixToIndex(String tableName, String attrName, IndexType indexType, Object attrValue) {
    Tuple keyTuple = buildKeyTuplePrefixToIndex(tableName, attrName, indexType); // start builidng keyTuple
    switch (indexType) {
      case NON_CLUSTERED_HASH_INDEX:
        keyTuple = keyTuple.add(attrValue.hashCode());
        break;
      case NON_CLUSTERED_B_PLUS_TREE_INDEX:
        if (attrValue instanceof String) {
          keyTuple = keyTuple.add(attrValue.toString());
        }
        else if (attrValue instanceof Integer){
          keyTuple = keyTuple.add((Integer) attrValue);
        }
        else if (attrValue instanceof Long){
          keyTuple = keyTuple.add((Long) attrValue);
        }
        else{
          System.out.println("Type: " + attrValue.getClass().getName());
          System.out.println("Value: " + attrValue.toString());
          throw new RuntimeException("attrValue is not String or Integer");

        }
        break;
      default:
        throw new RuntimeException("Unknown index type");
    }
    return keyTuple;
  }

  public Tuple buildKeyTupleToIndex(String tableName, String attrName, IndexType indexType, Object attrValue, Tuple primaryKeyValueTuple) {
    Tuple keyTuple = buildKeyTuplePrefixToIndex(tableName, attrName, indexType, attrValue).add(primaryKeyValueTuple);
    return keyTuple;
  }


  @Override
  public StatusCode createIndex(String tableName, String attrName, IndexType indexType) {
    Transaction tx = FDBHelper.openTransaction(db);

    // Read table metadata to make sture that attrName is a valid attribute
    TableMetadata tableMetadata = tableManager.getTableMetadataTx(tx, tableName);
    if (tableMetadata == null) {
      return StatusCode.TABLE_NOT_FOUND;
    }
    if (!tableMetadata.getAttributes().keySet().contains(attrName)) {
      return StatusCode.ATTRIBUTE_NOT_FOUND;
    }

    // Check if index already exists
    if (isIndexExist(tx, tableName, attrName)) {
      return StatusCode.INDEX_ALREADY_EXISTS_ON_ATTRIBUTE;
    }

    // Write index type to the database
    writeIndexType(tx, tableName, attrName, indexType);

    // Iterate through all records in the table and build the index
    List<String> primaryKeys = tableMetadata.getPrimaryKeys();

    RecordTransformer recordTransformer = new RecordTransformer(tableName);

    Tuple pkExistPrefix = recordTransformer.getTableRecordExistTuplePrefix(primaryKeys);

    List<String > recordAttributeStorePath = recordTransformer.getRecordAttributeStorePath();
    DirectorySubspace tableDirectory = FDBHelper.createOrOpenSubspace(tx, recordAttributeStorePath);
    byte[] keyPrefixB = tableDirectory.pack(pkExistPrefix);
    Range range = Range.startsWith(keyPrefixB);

    AsyncIterator<KeyValue> iterator = tx.getRange(range).iterator();

    while (iterator.hasNext()){
      Tuple recordExistTuple = null;
      KeyValue nextKeyValue = iterator.next();
      recordExistTuple = Tuple.fromBytes(nextKeyValue.getKey());
      Tuple primaryKeyValueTuple = recordTransformer.getPrimaryKeyValueTuple(recordExistTuple);
      Tuple attributeKeyTuple = recordTransformer.getTableRecordAttributeKeyTuple(primaryKeyValueTuple, attrName);
      FDBKVPair fdbkvPair = FDBHelper.getCertainKeyValuePairInSubdirectory(
              tableDirectory,
              tx,
              attributeKeyTuple,
              recordAttributeStorePath);

      if (fdbkvPair == null) {
        System.out.println("Attribute val not found");
      }
      else {
        Tuple attributeValueTuple = fdbkvPair.getValue();
        Object attributeValue = attributeValueTuple.get(0);
        insertIndex(tx, tableName, attrName, attributeValue, primaryKeyValueTuple);
      }

    }

    FDBHelper.commitTransaction(tx);

    return StatusCode.SUCCESS;
  }

  public boolean insertIndex(Transaction tx, String tableName, String attrName, Object attrValue, Tuple primaryKeyValueTuple) {
    if (!isIndexExist(tx, tableName, attrName)) {
      return false;
    }
    IndexType indexType = getIndexType(tx, tableName, attrName);
    if (indexType == null) {
      return false;
    }
    Tuple indexKeyTuple = buildKeyTupleToIndex(tableName, attrName, indexType, attrValue, primaryKeyValueTuple);
    Tuple indexValueTuple = primaryKeyValueTuple;
    tx.set(indexKeyTuple.pack(), indexValueTuple.pack());
    return true;
  }

  public boolean deleteIndex(Transaction tx, String tableName, String attrName, Object attrValue, Tuple primaryKeyValueTuple) {
    if (!isIndexExist(tx, tableName, attrName)) {
      return false;
    }
    IndexType indexType = getIndexType(tx, tableName, attrName);
    if (indexType == null) {
      return false;
    }
    Tuple indexKeyTuple = buildKeyTupleToIndex(tableName, attrName, indexType, attrValue, primaryKeyValueTuple);
    tx.clear(indexKeyTuple.pack());
    return true;
  }


  @Override
  public StatusCode dropIndex(String tableName, String attrName) {

    Transaction tx = FDBHelper.openTransaction(db);

    // Check if index already exists
    if (!isIndexExist(tx, tableName, attrName)) {
      return StatusCode.INDEX_NOT_FOUND;
    }

    IndexType indexType = getIndexType(tx, tableName, attrName);

    // Delete index type from the database
    Tuple keyTuple = buildKeyTupleToIndexType(tableName, attrName);
    tx.clear(keyTuple.pack());

    // Delete all index entries
    Tuple keyTuplePrefix = buildKeyTuplePrefixToIndex(tableName, attrName, indexType);
    byte[] keyPrefixB = keyTuplePrefix.pack();
    Range range = Range.startsWith(keyPrefixB);
    tx.clear(range);  // clear all index entries

    FDBHelper.commitTransaction(tx);

    return StatusCode.SUCCESS;
  }
}
