package CSCI485ClassProject;

import CSCI485ClassProject.models.AttributeType;
import CSCI485ClassProject.models.Record;
import com.apple.foundationdb.*;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

public class FDBHelper {

  public static int FDB_API_VERSION = 710;

  public static int MAX_TRANSACTION_COMMIT_RETRY_TIMES = 20;

  public static String SUPSPACE_RECORD_PREFIX = "record";

  public static Database initialization() {
    FDB fdb = FDB.selectAPIVersion(FDB_API_VERSION);

    Database db = null;
    try {
      db = fdb.open();
    } catch (Exception e) {
      System.out.println("ERROR: the database is not successfully opened: " + e);
    }
    return db;
  }

  public static void setFDBKVPair(DirectorySubspace tgtSubspace, Transaction tx, FDBKVPair kv) {
    if (tgtSubspace == null) {
      tgtSubspace = FDBHelper.createOrOpenSubspace(tx, kv.getSubspacePath());
    }
    tx.set(tgtSubspace.pack(kv.getKey()), kv.getValue().pack());
  }

  public static void setSubspaceKV(DirectorySubspace tgtSubspace, Transaction tx, Object key, Object value) {
    tx.set(tgtSubspace.pack(key), new Tuple().addObject(value).pack());
  }

  public static Object getSubspaceKV(DirectorySubspace tgtSubspace, Transaction tx, Object key) {
    byte[] value = tx.get(tgtSubspace.pack(key)).join();
    if (value == null) {
      return null;
    }
    return Tuple.fromBytes(value).get(0);
  }

  public static void setSubspaceRecord(DirectorySubspace tgtSubspace, Transaction tx, Record record) {
    UUID uuid = UUID.randomUUID();
    byte[] prefix = tgtSubspace.pack(SUPSPACE_RECORD_PREFIX);
    byte[] key = Tuple.fromBytes(prefix).add(uuid).pack(); // hash

    byte[] value = new Tuple().add(uuid).pack(); // hash

    tx.set(key, value);
//    System.out.println("key: " + Utils.byteArray2String(key) + " value: " + Utils.byteArray2String(value));

    HashMap<String, Object> mapAttrNameToValueValue = record.getMapAttrNameToValueValue();
    // iterate over key value using for each loop
    for (String attrName : mapAttrNameToValueValue.keySet()) {
      byte[] nkey = tgtSubspace.pack(new Tuple().add(uuid).add(attrName)); //hash
      Object attrValue = mapAttrNameToValueValue.get(attrName);
      byte[] nvalue = new Tuple().addObject(attrValue).pack();
      tx.set(nkey, nvalue);
    }
  }
//  public static void setSubspaceRecord(DirectorySubspace tgtSubspace, Transaction tx, Record record, int hashCode) {
//    if (hashCode == -1) {
//      hashCode = record.hashCode();
//    }
//
//
////    record.getMapAttrNameToValueValue().forEach((attrName, attrValue) -> {
////      byte[] nkey = tgtSubspace.pack(new Tuple().add(hashCode).add(attrName));
//////      System.out.println("nkey: " + Utils.byteArray2String(nkey));
////      byte[] nvalue = new Tuple().addObject(attrValue).pack();
////      tx.set(nkey, nvalue);
////      hashCode++;
////    });
//  }

  public static SubspaceRecordIterator getSubspaceRecordIterator(DirectorySubspace tgtSubspace, Transaction tx) {
    return new SubspaceRecordIterator(tgtSubspace, tx);
  }

  public static SubspaceRecordIterator getSubspaceRecordIterator(DirectorySubspace tgtSubspace, Transaction tx, boolean clearOnExit) {
    return new SubspaceRecordIterator(tgtSubspace, tx, clearOnExit);
  }

  public static class SubspaceRecordIterator extends Iterator {
    AsyncIterator<KeyValue> iterator;
    DirectorySubspace tgtSubspace;
    Transaction tx;

    boolean clearOnExit = false;

    public SubspaceRecordIterator(DirectorySubspace tgtSubspace, Transaction tx) {
        this.tgtSubspace = tgtSubspace;
        this.tx = tx;

        byte[] prefix = tgtSubspace.pack(SUPSPACE_RECORD_PREFIX);

        byte[] firstKey = Utils.getFirstKeyWithPrefix(prefix);

        byte[] lastKey = Utils.getLastKeyWithPrefix(prefix);

        KeySelector beginKeySelector = KeySelector.firstGreaterOrEqual(firstKey);
        KeySelector endKeySelector = KeySelector.firstGreaterThan(lastKey);

        Range range = new Range(beginKeySelector.getKey(), endKeySelector.getKey());

        this.iterator = tx.getRange(range, ReadTransaction.ROW_LIMIT_UNLIMITED, true).iterator();
    }

    public SubspaceRecordIterator(DirectorySubspace tgtSubspace, Transaction tx, boolean clearOnExit) {
      this.tgtSubspace = tgtSubspace;
      this.tx = tx;
      this.clearOnExit = clearOnExit;

      byte[] prefix = tgtSubspace.pack(SUPSPACE_RECORD_PREFIX);

      byte[] firstKey = Utils.getFirstKeyWithPrefix(prefix);

      byte[] lastKey = Utils.getLastKeyWithPrefix(prefix);

      KeySelector beginKeySelector = KeySelector.firstGreaterOrEqual(firstKey);
      KeySelector endKeySelector = KeySelector.firstGreaterThan(lastKey);

      Range range = new Range(beginKeySelector.getKey(), endKeySelector.getKey());

      this.iterator = tx.getRange(range, ReadTransaction.ROW_LIMIT_UNLIMITED, true).iterator();
    }

    private Record getRecordFromUuid(UUID uuid) {
        byte[] prefix = tgtSubspace.pack(uuid);
        byte[] firstKey = Utils.getFirstKeyWithPrefix(prefix);
        byte[] lastKey = Utils.getLastKeyWithPrefix(prefix);

        KeySelector beginKeySelector = KeySelector.firstGreaterOrEqual(firstKey);
        KeySelector endKeySelector = KeySelector.firstGreaterThan(lastKey);

        Range range = new Range(beginKeySelector.getKey(), endKeySelector.getKey());

//        System.out.println("range: " + Utils.byteArray2String(range.begin) + " " + Utils.byteArray2String(range.end));

        Record record = new Record();

        AsyncIterator<KeyValue> it = tx.getRange(range, ReadTransaction.ROW_LIMIT_UNLIMITED, true).iterator();


        while (it.hasNext()) {
            KeyValue kv = it.next();
            Object value = Tuple.fromBytes(kv.getValue()).get(0);
            String key = tgtSubspace.unpack(kv.getKey()).getString(1);
            record.setAttrNameAndValue(key, value);
        }

        return record;
    }

    @Override
    public Record next() {
      if (iterator.hasNext()) {
        KeyValue kv = iterator.next();
        Tuple value = Tuple.fromBytes(kv.getValue());
        UUID uuid = value.getUUID(0);
        return getRecordFromUuid((uuid));
      }
      return null;
    }

    @Override
    public void commit() {
        if (clearOnExit){
          tx.clear(tgtSubspace.range());
        }
    }

    @Override
    public void abort() {
      if (clearOnExit){
        tx.clear(tgtSubspace.range());
      }
    }
  }




  public static byte[] getFDBKVPair(DirectorySubspace tgtSubspace, Transaction tx, FDBKVPair kv) {
//    FDBKVPair kv = new FDBKVPair(, key, value);
    if (tgtSubspace == null) {
      tgtSubspace = FDBHelper.createOrOpenSubspace(tx, kv.getSubspacePath());
    }
    tx.set(tgtSubspace.pack(kv.getKey()), kv.getValue().pack());

//    System.out.println("Set key: " + Utils.byteArray2String( tgtSubspace.pack(kv.getKey()))
//            + " KeyTuple: " + kv.getKey().toString()
//            + " KeyTubleBytes: " + Utils.byteArray2String(kv.getKey().pack())
//            + " value: " + Utils.byteArray2String(kv.getValue().pack())
//            + " in subspace: " + Utils.byteArray2String(tgtSubspace.pack())
//    );
    return null;
  }

  public static List<String> getAllDirectSubspaceName(Transaction tx) {
    List<String> subpaths = DirectoryLayer.getDefault().list(tx).join();
    return subpaths;
  }

  public static List<FDBKVPair> getAllKeyValuePairsOfSubdirectory(Database db, Transaction tx, List<String> path) {
    List<FDBKVPair> res = new ArrayList<>();
    if (!doesSubdirectoryExists(tx, path)) {
      return res;
    }

    DirectorySubspace dir = FDBHelper.createOrOpenSubspace(tx, path);
    Range range = dir.range();

    List<KeyValue> kvs = tx.getRange(range).asList().join();
    for (KeyValue kv : kvs) {
      Tuple key = dir.unpack(kv.getKey());
      Tuple value = Tuple.fromBytes(kv.getValue());
      res.add(new FDBKVPair(path, key, value));
    }

    return res;
  }

  public static FDBKVPair getCertainKeyValuePairInSubdirectory(DirectorySubspace dir, Transaction tx, Tuple keyTuple, List<String> path) {
    if (dir == null) {
      return null;
    }
    Tuple f = keyTuple;
    byte[] keyTupleB = keyTuple.pack();
    byte[] dirB = dir.pack();
    byte[] dirPackKey = dir.pack(keyTuple);

//    System.out.println("Get key: " + Utils.byteArray2String(dirPackKey)
//            + " KeyTuple: " + keyTuple.toString()
//            + " in subspace: " + Utils.byteArray2String(dir.pack()));

    byte[] valBytes = tx.get(dir.pack(keyTuple)).join();
    if (valBytes == null) {
      return null;
    }
    Tuple value = Tuple.fromBytes(valBytes);
    return new FDBKVPair(path, keyTuple, value);
  }

  public static void clear(Database db) {

    Transaction tx = openTransaction(db);
    final byte[] st = new Subspace(new byte[]{(byte) 0x00}).getKey();
    final byte[] en = new Subspace(new byte[]{(byte) 0xFF}).getKey();
    tx.clear(st, en);
    commitTransaction(tx);
  }

  public static DirectorySubspace createOrOpenSubspace(Transaction tx, List<String> path) {
    return DirectoryLayer.getDefault().createOrOpen(tx, path).join();
  }

  public static DirectorySubspace openSubspace(Transaction tx, List<String> path) {
    return DirectoryLayer.getDefault().open(tx, path).join();
  }

  public static boolean doesSubdirectoryExists(Transaction tx, List<String> path) {
    return DirectoryLayer.getDefault().exists(tx, path).join();
  }

  public static void dropSubspace(Transaction tx, List<String> path) {
    DirectoryLayer.getDefault().remove(tx, path).join();
  }

  public static void removeKeyValuePair(Transaction tx, DirectorySubspace dir, Tuple keyTuple) {
    tx.clear(dir.pack(keyTuple));
  }

  public static Transaction openTransaction(Database db) {
    Transaction transaction = db.createTransaction();
    transaction.options().setTimeout(60*1000);
    return transaction;
  }

  public static boolean commitTransaction(Transaction tx) {
    boolean b = tryCommitTx(tx, 0);
    tx.close();
    return b;
  }

  public static boolean tryCommitTx(Transaction tx, int retryCounter) {
    try {
      tx.commit().join();
      return true;
    } catch (FDBException e) {
      if (retryCounter < MAX_TRANSACTION_COMMIT_RETRY_TIMES) {
        retryCounter++;
        tryCommitTx(tx, retryCounter);
      } else {
        tx.cancel();
        return false;
      }
    }
    return false;
  }

  public static void abortTransaction(Transaction tx) {
    tx.cancel();
    tx.close();
  }

  public static AttributeType getType(Object value) {
    if (value instanceof Integer || value instanceof Long) {
      return AttributeType.INT;
    } else if (value instanceof String) {
      return AttributeType.VARCHAR;
    } else if (value instanceof Double) {
      return AttributeType.DOUBLE;
    }
    return AttributeType.NULL;
  }
}
