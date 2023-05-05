package CSCI485ClassProject.test;

import CSCI485ClassProject.*;
import CSCI485ClassProject.Iterator;
import CSCI485ClassProject.models.*;
import com.apple.foundationdb.Database;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;
import org.junit.Test;

import javax.xml.crypto.Data;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;

import static CSCI485ClassProject.RelationalAlgebraOperatorsImpl.createCpFunction;
import static org.junit.Assert.*;

public class cpfunctionTest {

  @Test
  public void unitTest1() {
    Record rec1_10 = new Record();
    rec1_10.setAttrNameAndValue("ID", 1);
    rec1_10.setAttrNameAndValue("Age", 10);

    Record rec2_19 = new Record();
    rec2_19.setAttrNameAndValue("ID", 2);
    rec2_19.setAttrNameAndValue("Age", 19);

    Record rec3_31 = new Record();
    rec3_31.setAttrNameAndValue("ID", 3);
    rec3_31.setAttrNameAndValue("Age", 31);


    ComparisonPredicate cp1 = new ComparisonPredicate();
    boolean result1 = createCpFunction(cp1).apply(rec1_10);
    assertEquals(true, result1);

    ComparisonPredicate cp2 = new ComparisonPredicate("Age", AttributeType.INT, ComparisonOperator.GREATER_THAN_OR_EQUAL_TO, 19);
    Function<Record, Boolean> f = createCpFunction(cp2);
    System.out.println(f);

    boolean result2 = createCpFunction(cp2).apply(rec1_10);
    assertEquals(false, result2);
    boolean result3 = createCpFunction(cp2).apply(rec2_19);
    assertEquals(true, result3);
    boolean result4 = createCpFunction(cp2).apply(rec3_31);
    assertEquals(true, result4);


    ComparisonPredicate cp3 = new ComparisonPredicate("Age", AttributeType.INT, ComparisonOperator.LESS_THAN_OR_EQUAL_TO, "ID", AttributeType.INT, 10, AlgebraicOperator.PRODUCT);
    boolean result5 = createCpFunction(cp3).apply(rec1_10);
    assertEquals(true, result5);
    boolean result6 = createCpFunction(cp3).apply(rec2_19);
    assertEquals(true, result6);
    boolean result7 = createCpFunction(cp3).apply(rec3_31);
    assertEquals(false, result7);

  }

  @Test
  public void recordEqTest2() {
    Record rec1_10 = new Record();
    rec1_10.setAttrNameAndValue("ID", 1);
    rec1_10.setAttrNameAndValue("Age", 10);

    Record rec1_10_2 = new Record();
    rec1_10_2.setAttrNameAndValue("ID", 1);
    rec1_10_2.setAttrNameAndValue("Age", (long) 10);

//    assertEquals(rec1_10, rec1_10_2);
  }

  @Test
  public void testReadAndWriteKV(){
    String key = "key";
    Long value = 1L;
    Database db = FDBHelper.initialization();
    Transaction tx = FDBHelper.openTransaction(db);
    DirectorySubspace subspace = FDBHelper.createOrOpenSubspace(tx, Collections.singletonList("test"));
    FDBHelper.setSubspaceKV(subspace, tx, key, value);
    Long result = (Long) FDBHelper.getSubspaceKV(subspace, tx, key);
    assertEquals(value, result);

    FDBHelper.setSubspaceKV(subspace, tx, "key2", value);

    RelationalAlgebraOperatorsImpl ra = new RelationalAlgebraOperatorsImpl();
    Iterator it = ra.getProjectIterator(subspace, tx, false, "keyAttribute");
    Record record = it.next();

    assertEquals(value, record.getMapAttrNameToValueValue().get("keyAttribute"));

//    Record record2 = it.next();
//    assertEquals(null, record2);

    System.out.println("/////////////////////////////");

    it = ra.getProjectIterator(subspace, tx, false, "keyAttribute");
    Iterator itprojectDup = ra.project(it, "keyAttribute", false);
    Record record5 = itprojectDup.next();
    assertEquals(value, record5.getMapAttrNameToValueValue().get("keyAttribute"));
    System.out.println("/////////////////////////////");
    Record record6 = itprojectDup.next();
    System.out.println("record6" + record6);
    assertEquals(value, record6.getMapAttrNameToValueValue().get("keyAttribute"));

    it = ra.getProjectIterator(subspace, tx, false, "keyAttribute");
    Iterator itproject = ra.project(it, "keyAttribute", true);
    Record record3 = itproject.next();
    assertEquals(value, record3.getMapAttrNameToValueValue().get("keyAttribute"));
    Record record4 = itproject.next();
    assertEquals(null, record4);
  }

  @Test
  public void testReadAndWriteRecord(){
    Record record = new Record();
    String key = "key";
    Long value = 1L;
    String key2 = "key2";
    String value2 = "value2";
    record.setAttrNameAndValue(key, value);
    record.setAttrNameAndValue(key2, value2);




    Database db = FDBHelper.initialization();
    Transaction tx = FDBHelper.openTransaction(db);
    DirectorySubspace subspace = FDBHelper.createOrOpenSubspace(tx, Collections.singletonList("test"));

    FDBHelper.setSubspaceKV(subspace, tx, key, value);
    Long result = (Long) FDBHelper.getSubspaceKV(subspace, tx, key);
    assertEquals(value, result);

    FDBHelper.setSubspaceKV(subspace, tx, "key2", value);

    RelationalAlgebraOperatorsImpl ra = new RelationalAlgebraOperatorsImpl();
    Iterator it = ra.getProjectIterator(subspace, tx, false, "keyAttribute");
    record = it.next();

    assertEquals(value, record.getMapAttrNameToValueValue().get("keyAttribute"));

//    Record record2 = it.next();
//    assertEquals(null, record2);

    System.out.println("/////////////////////////////");

    it = ra.getProjectIterator(subspace, tx, false, "keyAttribute");
    Iterator itprojectDup = ra.project(it, "keyAttribute", false);
    Record record5 = itprojectDup.next();
    assertEquals(value, record5.getMapAttrNameToValueValue().get("keyAttribute"));
    System.out.println("/////////////////////////////");
    Record record6 = itprojectDup.next();
    System.out.println("record6" + record6);
    assertEquals(value, record6.getMapAttrNameToValueValue().get("keyAttribute"));

    it = ra.getProjectIterator(subspace, tx, false, "keyAttribute");
    Iterator itproject = ra.project(it, "keyAttribute", true);
    Record record3 = itproject.next();
    assertEquals(value, record3.getMapAttrNameToValueValue().get("keyAttribute"));
    Record record4 = itproject.next();
    assertEquals(null, record4);
  }

  @Test
  public void TestPacking(){
    Database db = FDBHelper.initialization();
    Transaction tx = FDBHelper.openTransaction(db);
    DirectorySubspace subspace = FDBHelper.createOrOpenSubspace(tx, Collections.singletonList("test"));

    int hashCode = 999;
    byte[] abyte = subspace.pack("a");
    byte[] key = Tuple.fromBytes(abyte).add(hashCode).pack();

    byte[] key2 = subspace.pack(new Tuple().add("a").add(hashCode));

    System.out.println("key: " + Utils.byteArray2String(key));
    System.out.println("key2: " + Utils.byteArray2String(key2));
    System.out.println("prefix: " + Utils.byteArray2String(abyte));

    assertArrayEquals(key, key2);


  }


  @Test
  public void TestReadAndWriteRecord2(){
    Database db = FDBHelper.initialization();
    FDBHelper.clear(db);
    Transaction tx = FDBHelper.openTransaction(db);
    DirectorySubspace subspace = FDBHelper.createOrOpenSubspace(tx, Collections.singletonList("test"));
    Set<Record> expected = new HashSet<>();
    for (int i = 0; i < 100; i++) {
      Record dep = getExpectedDepartmentRecord(i);
      FDBHelper.setSubspaceRecord(subspace, tx, dep);
      expected.add(dep);
    }

    Iterator it = FDBHelper.getSubspaceRecordIterator(subspace, tx);
    Set<Record> actual = new HashSet<>();
    for (int i = 0; i < 100; i++){
      Record foo = it.next();
      assertNotNull(foo);
      actual.add(foo);
      System.out.println("foo " + foo);
    }


  }
  @Test
  public void TestReadAndWriteRecord(){
    Database db = FDBHelper.initialization();
    FDBHelper.clear(db);
    Transaction tx = FDBHelper.openTransaction(db);
    DirectorySubspace subspace = FDBHelper.createOrOpenSubspace(tx, Collections.singletonList("test"));

    Record record = new Record();
    record.setAttrNameAndValue("ID", 1L);
    record.setAttrNameAndValue("Age", 10L);

    System.out.println("Record hash code " + record.hashCode());

    FDBHelper.setSubspaceRecord(subspace, tx, record);
    Iterator it = FDBHelper.getSubspaceRecordIterator(subspace, tx);
    Record record2 = it.next();
    assertEquals(record, record2);

    Record record3 = it.next();
    assertEquals(null, record3);

  }

  public static String EmployeeTableName = "Employee";
  public static String SSN = "SSN";
  public static String Name = "Name";
  public static String Email = "Email";
  public static String Age = "Age";
  public static String Address = "Address";
  public static String Salary = "Salary";

  public static String DepartmentTableName = "Department";
  public static String DNO = "DNO";
  public static String Floor = "Floor";

  public static String[] EmployeeTableAttributeNames =
          new String[]{SSN, DNO, Name, Email, Age, Address, Salary};
  public static AttributeType[] EmployeeTableAttributeTypes =
          new AttributeType[]{AttributeType.INT, AttributeType.INT, AttributeType.VARCHAR,
                  AttributeType.VARCHAR, AttributeType.INT, AttributeType.VARCHAR, AttributeType.INT};
  public static String[] EmployeeTablePKAttributes =
          new String[]{SSN};
  public static String[] EmployeeTableNonPKAttributeNames =
          new String[]{DNO, Name, Email, Age, Address, Salary};

  public static String[] DepartmentTableAttributeNames =
          new String[]{DNO, Name, Floor};
  public static AttributeType[] DepartmentTableAttributeTypes =
          new AttributeType[]{AttributeType.INT, AttributeType.VARCHAR, AttributeType.INT};
  public static String[] DepartmentTablePKAttributes =
          new String[]{DNO};
  public static String[] DepartmentTableNonPKAttributeNames =
          new String[]{Name, Floor};

  public static int initialNumberOfRecords = 100;
  public static int updatedNumberOfRecords = 100;
  public static int dnoLB = 20;
  public static int dnoUB = 80;
  public static int randSeed = 10;


  private TableManager tableManager;
  private Records records;
  private Indexes indexes;
  private RelationalAlgebraOperators relAlgOperators;

  private String getName(long i) {
    return "Name" + i;
  }

  private String getEmail(long i) {
    return "ABCDEFGH" + i + "@usc.edu";
  }

  private long getAge(long i) {
    return 20 + i / 10;
  }

  private String getAddress(long i) {
    return "ABCDEFGHIJKLMNOPQRSTUVWXYZ" + i;
  }

  private long getSalary(long i) {
    return i;
  }

  private String getDepartmentName(long i) {
    return "Department" + i;
  }

  private long getFloor(long i) {
    return i;
  }

  private long getDno(Random generator, long lowerBound, long upperBound) {
    long range = upperBound - lowerBound + 1;
    long randomLong = generator.nextLong() % range;

    if (randomLong < 0) {
      randomLong = -randomLong;
    }
    return randomLong + lowerBound;
  }

  private Record getExpectedDepartmentRecord(long dno) {
    Record rec = new Record();
    String name = getDepartmentName(dno);
    long floor = getFloor(dno);

    rec.setAttrNameAndValue(DNO, dno);
    rec.setAttrNameAndValue(Name, name);
    rec.setAttrNameAndValue(Floor, floor);

    return rec;
  }

  private Record getExpectedEmployeeRecord(long ssn, long dno) {
    Record rec = new Record();
    String name = getName(ssn);
    String email = getEmail(ssn);
    long age = getAge(ssn);
    String address = getAddress(ssn);
    long salary = getSalary(ssn);

    rec.setAttrNameAndValue(SSN, ssn);
    rec.setAttrNameAndValue(DNO, dno);
    rec.setAttrNameAndValue(Name, name);
    rec.setAttrNameAndValue(Email, email);
    rec.setAttrNameAndValue(Age, age);
    rec.setAttrNameAndValue(Address, address);
    rec.setAttrNameAndValue(Salary, salary);

    return rec;
  }

  private Record getExpectedJoinedEmpDepRecord(Record employee, Record department) {
    Record res = new Record();
    res.setAttrNameAndValue(SSN, employee.getValueForGivenAttrName(SSN));
    res.setAttrNameAndValue(Email, employee.getValueForGivenAttrName(Email));
    res.setAttrNameAndValue(Age, employee.getValueForGivenAttrName(Age));
    res.setAttrNameAndValue(EmployeeTableName + "." + Name, employee.getValueForGivenAttrName(Name));
    res.setAttrNameAndValue(EmployeeTableName + "." + DNO, employee.getValueForGivenAttrName(DNO));
    res.setAttrNameAndValue(Address, employee.getValueForGivenAttrName(Address));
    res.setAttrNameAndValue(Salary, employee.getValueForGivenAttrName(Salary));
    res.setAttrNameAndValue(Floor, department.getValueForGivenAttrName(Floor));
    res.setAttrNameAndValue(DepartmentTableName + "." + DNO, department.getValueForGivenAttrName(DNO));
    res.setAttrNameAndValue(DepartmentTableName + "." + Name, department.getValueForGivenAttrName(Name));

    return res;
  }


  @Test
  public void TestJoinF(){
    Record r1 = getExpectedEmployeeRecord(1000,1);
    Record r2 = getExpectedDepartmentRecord(1);
    Record r3 = getExpectedJoinedEmpDepRecord(r1, r2);

    ComparisonPredicate joinPredicate =
            new ComparisonPredicate(DNO, AttributeType.INT, ComparisonOperator.EQUAL_TO, DNO, AttributeType.INT, 1, AlgebraicOperator.PRODUCT);

    BiFunction<Record, Record, Record> function = Cursor.createPredicateJoinFunction(joinPredicate);
    Record r4 = function.apply(r1, r2);

    System.out.println("r3: " + r3);
    System.out.println("r4: " + r4);
    assertEquals(r3,r4);

  }

}
