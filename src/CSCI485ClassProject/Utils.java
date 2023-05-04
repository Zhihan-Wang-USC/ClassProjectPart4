package CSCI485ClassProject;

public class Utils {

//  public static void printByteArray(byte[] bytes) {
//    String hexString = String.format("%02X", bytes[0]);
//    for (int i = 1; i < bytes.length; i++) {
//      hexString += String.format(" %02X", bytes[i]);
//    }
//    System.out.println(hexString);
//  }

  public static String byteArray2String(byte[] bytes) {
    String hexString = String.format("%02X", bytes[0]);
    for (int i = 1; i < bytes.length; i++) {
      hexString += String.format(" %02X", bytes[i]);
    }
    return hexString;
  }

  public static byte[] getLastKeyWithPrefix(byte[] prefix) {
    byte[] lastKey = new byte[prefix.length + 1];
    System.arraycopy(prefix, 0, lastKey, 0, prefix.length);
    lastKey[lastKey.length - 1] = (byte)0xFF;
    return lastKey;
  }

  public static byte[] getFirstKeyWithPrefix(byte[] prefix) {
    byte[] firstKey = new byte[prefix.length + 1];
    System.arraycopy(prefix, 0, firstKey, 0, prefix.length);
    firstKey[firstKey.length - 1] = (byte)0x00;
    return firstKey;
  }

  public static Iterator.Mode getIteartoModeFromCursorMode(Cursor.Mode mode) {
    switch (mode) {
      case READ:
        return Iterator.Mode.READ;
      case READ_WRITE:
        return Iterator.Mode.READ_WRITE;
      default:
        return null;
    }
  }

  public static Cursor.Mode getCursorModeFromIteratorMode(Iterator.Mode mode) {
    switch (mode) {
      case READ:
        return Cursor.Mode.READ;
      case READ_WRITE:
        return Cursor.Mode.READ_WRITE;
      default:
        return null;
    }
  }

  public static String generateUUID() {
    return java.util.UUID.randomUUID().toString();
  }

}
