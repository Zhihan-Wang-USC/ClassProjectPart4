package CSCI485ClassProject.models;

public enum IndexType {
  NON_CLUSTERED_HASH_INDEX,
  NON_CLUSTERED_B_PLUS_TREE_INDEX;

  public static String IndexType2String(IndexType indexType) {
    switch (indexType) {
      case NON_CLUSTERED_HASH_INDEX:
        return "HASH_INDEX";
      case NON_CLUSTERED_B_PLUS_TREE_INDEX:
        return "B_PLUS_TREE_INDEX";
      default:
        return "UNKNOWN";
    }
  }

  public static IndexType String2IndexType(String indexType) {
    switch (indexType) {
      case "HASH_INDEX":
        return NON_CLUSTERED_HASH_INDEX;
      case "B_PLUS_TREE_INDEX":
        return NON_CLUSTERED_B_PLUS_TREE_INDEX;
      default:
        return null;
    }
  }
}


