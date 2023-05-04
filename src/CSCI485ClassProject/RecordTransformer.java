package CSCI485ClassProject;

import CSCI485ClassProject.models.AttributeType;
import CSCI485ClassProject.models.Record;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class RecordTransformer {

  private List<String> recordAttrStorePath;

//  public FDBKVPair getRecordKVPair(String attributeName, AttributeType attributeType) {
//    Tuple keyTuple = getRecordAttributeKeyTuple(attributeName);
//    Tuple valueTuple = new Tuple().add(attributeType.ordinal()).add(false);
//
//    return new FDBKVPair(recordAttrStorePath, keyTuple, valueTuple);
//  }

  public static Tuple getRecordAttributeKeyTuple(String attributeName, Tuple primaryKeyValueTuple) {
    return new Tuple().add(attributeName).add(primaryKeyValueTuple);
  }
//  public static Tuple getRecordAttributeKeyTuple(Tuple primaryKeyTuple) { return primaryKeyTuple;}

  public RecordTransformer(String tableName) {
    recordAttrStorePath = new ArrayList<>();
    recordAttrStorePath.add(tableName);
    recordAttrStorePath.add(DBConf.RECORD_TABLE_ATTR_STORE);
  }

  public List<String> getRecordAttributeStorePath() {
    return recordAttrStorePath;
  }

  public static Tuple getTableRecordAttributeKeyTuple(Tuple primaryKeyValueTuple, String attributeName) {
    return new Tuple().add(attributeName).addAll(primaryKeyValueTuple);
  }

  public static Tuple getTableRecordAttributeKeyTuplePrefix(String attributeName) {
    return new Tuple().add(attributeName);
  }

  public static Tuple getTableRecordExistTuple(Tuple primaryKeyValueTuple, List<String> primaryKeys) {
    return new Tuple().add(primaryKeys.get(0)).addAll(primaryKeyValueTuple);
  }

  public static Tuple getTableRecordExistTuplePrefix(List<String> primaryKeys) {
      return new Tuple().add(primaryKeys.get(0));
  }



  public static Tuple getPrimaryKeyValueTuple(Tuple tableRecordExistTuple) {
    // Why 2? Because the first two elements are directorySubspace and attributeName
    return Tuple.fromItems(tableRecordExistTuple.getItems().subList(2, tableRecordExistTuple.size()));
  }

  public Record convertBackToRecord(List<FDBKVPair> pairs) {
    HashMap<String, Object> mapAttrNameToValue = new HashMap<>();
    for (FDBKVPair kv : pairs) {
      Tuple key = kv.getKey();
      Tuple value = kv.getValue();

      String attributeName = key.getString(0);
      Object attributeValue = value.get(0);

      mapAttrNameToValue.put(attributeName, attributeValue);
    }
    Record record = new Record();
    StatusCode status = record.setMapAttrNameToValue(mapAttrNameToValue);
    assert status == StatusCode.SUCCESS; // TODO What else can it be?
    return record;
  }

//  public static String getLastElementAsString(Tuple tuple) {
//    int size = tuple.size();
//    String lastElementAsString = tuple.getString(size-1);
//    return lastElementAsString;
//  }

  public List<FDBKVPair> convertToFDBKVPairs(Record record, Tuple primaryKeyValueTuple) {
    List<FDBKVPair> res = new ArrayList<>();

    HashMap<String, Object> attributeMap = record.getMapAttrNameToValueValue();

    for (Map.Entry<String, Object> kv : attributeMap.entrySet()) {
      Tuple keyTuple = getRecordAttributeKeyTuple(kv.getKey(), primaryKeyValueTuple);
      Tuple valueTuple = Tuple.from(kv.getValue());
      res.add(new FDBKVPair(recordAttrStorePath, keyTuple, valueTuple));
    }
    return res;
  }

  public List<FDBKVPair> augmentWithPrimaryKeyValue(List<FDBKVPair> pairs, Tuple primaryKeyValueTuple) {
    List<FDBKVPair> res = new ArrayList<>();
    for (FDBKVPair kv : pairs) {
      Tuple key = kv.getKey();
      Tuple value = kv.getValue();
      Tuple newKey = RecordTransformer.getTableRecordAttributeKeyTuple(primaryKeyValueTuple, key.getString(0));
      res.add(new FDBKVPair(recordAttrStorePath, newKey, value));
    }
    return res;
  }
}
