// ORM class for table 'null'
// WARNING: This class is AUTO-GENERATED. Modify at your own risk.
//
// Debug information:
// Generated date: Tue Mar 27 17:48:14 GMT+00:00 2018
// For connector: org.apache.sqoop.manager.SQLServerManager
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.lib.db.DBWritable;
import com.cloudera.sqoop.lib.JdbcWritableBridge;
import com.cloudera.sqoop.lib.DelimiterSet;
import com.cloudera.sqoop.lib.FieldFormatter;
import com.cloudera.sqoop.lib.RecordParser;
import com.cloudera.sqoop.lib.BooleanParser;
import com.cloudera.sqoop.lib.BlobRef;
import com.cloudera.sqoop.lib.ClobRef;
import com.cloudera.sqoop.lib.LargeObjectLoader;
import com.cloudera.sqoop.lib.SqoopRecord;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

public class QueryResult extends SqoopRecord  implements DBWritable, Writable {
  private final int PROTOCOL_VERSION = 3;
  public int getClassFormatVersion() { return PROTOCOL_VERSION; }
  public static interface FieldSetterCommand {    void setField(Object value);  }  protected ResultSet __cur_result_set;
  private Map<String, FieldSetterCommand> setters = new HashMap<String, FieldSetterCommand>();
  private void init0() {
    setters.put("corp_seq", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        corp_seq = (Long)value;
      }
    });
    setters.put("id", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        id = (String)value;
      }
    });
    setters.put("seller", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        seller = (String)value;
      }
    });
    setters.put("name", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        name = (String)value;
      }
    });
    setters.put("delegate", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        delegate = (String)value;
      }
    });
    setters.put("addr", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        addr = (String)value;
      }
    });
    setters.put("email", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        email = (String)value;
      }
    });
    setters.put("corp_number", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        corp_number = (String)value;
      }
    });
    setters.put("cellular_phone1", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        cellular_phone1 = (String)value;
      }
    });
    setters.put("cellular_phone2", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        cellular_phone2 = (String)value;
      }
    });
    setters.put("cellular_phone3", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        cellular_phone3 = (String)value;
      }
    });
    setters.put("corp_phone1", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        corp_phone1 = (String)value;
      }
    });
    setters.put("corp_phone2", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        corp_phone2 = (String)value;
      }
    });
    setters.put("fax", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        fax = (String)value;
      }
    });
    setters.put("rtime", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        rtime = (java.sql.Timestamp)value;
      }
    });
    setters.put("certified", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        certified = (Integer)value;
      }
    });
  }
  public QueryResult() {
    init0();
  }
  private Long corp_seq;
  public Long get_corp_seq() {
    return corp_seq;
  }
  public void set_corp_seq(Long corp_seq) {
    this.corp_seq = corp_seq;
  }
  public QueryResult with_corp_seq(Long corp_seq) {
    this.corp_seq = corp_seq;
    return this;
  }
  private String id;
  public String get_id() {
    return id;
  }
  public void set_id(String id) {
    this.id = id;
  }
  public QueryResult with_id(String id) {
    this.id = id;
    return this;
  }
  private String seller;
  public String get_seller() {
    return seller;
  }
  public void set_seller(String seller) {
    this.seller = seller;
  }
  public QueryResult with_seller(String seller) {
    this.seller = seller;
    return this;
  }
  private String name;
  public String get_name() {
    return name;
  }
  public void set_name(String name) {
    this.name = name;
  }
  public QueryResult with_name(String name) {
    this.name = name;
    return this;
  }
  private String delegate;
  public String get_delegate() {
    return delegate;
  }
  public void set_delegate(String delegate) {
    this.delegate = delegate;
  }
  public QueryResult with_delegate(String delegate) {
    this.delegate = delegate;
    return this;
  }
  private String addr;
  public String get_addr() {
    return addr;
  }
  public void set_addr(String addr) {
    this.addr = addr;
  }
  public QueryResult with_addr(String addr) {
    this.addr = addr;
    return this;
  }
  private String email;
  public String get_email() {
    return email;
  }
  public void set_email(String email) {
    this.email = email;
  }
  public QueryResult with_email(String email) {
    this.email = email;
    return this;
  }
  private String corp_number;
  public String get_corp_number() {
    return corp_number;
  }
  public void set_corp_number(String corp_number) {
    this.corp_number = corp_number;
  }
  public QueryResult with_corp_number(String corp_number) {
    this.corp_number = corp_number;
    return this;
  }
  private String cellular_phone1;
  public String get_cellular_phone1() {
    return cellular_phone1;
  }
  public void set_cellular_phone1(String cellular_phone1) {
    this.cellular_phone1 = cellular_phone1;
  }
  public QueryResult with_cellular_phone1(String cellular_phone1) {
    this.cellular_phone1 = cellular_phone1;
    return this;
  }
  private String cellular_phone2;
  public String get_cellular_phone2() {
    return cellular_phone2;
  }
  public void set_cellular_phone2(String cellular_phone2) {
    this.cellular_phone2 = cellular_phone2;
  }
  public QueryResult with_cellular_phone2(String cellular_phone2) {
    this.cellular_phone2 = cellular_phone2;
    return this;
  }
  private String cellular_phone3;
  public String get_cellular_phone3() {
    return cellular_phone3;
  }
  public void set_cellular_phone3(String cellular_phone3) {
    this.cellular_phone3 = cellular_phone3;
  }
  public QueryResult with_cellular_phone3(String cellular_phone3) {
    this.cellular_phone3 = cellular_phone3;
    return this;
  }
  private String corp_phone1;
  public String get_corp_phone1() {
    return corp_phone1;
  }
  public void set_corp_phone1(String corp_phone1) {
    this.corp_phone1 = corp_phone1;
  }
  public QueryResult with_corp_phone1(String corp_phone1) {
    this.corp_phone1 = corp_phone1;
    return this;
  }
  private String corp_phone2;
  public String get_corp_phone2() {
    return corp_phone2;
  }
  public void set_corp_phone2(String corp_phone2) {
    this.corp_phone2 = corp_phone2;
  }
  public QueryResult with_corp_phone2(String corp_phone2) {
    this.corp_phone2 = corp_phone2;
    return this;
  }
  private String fax;
  public String get_fax() {
    return fax;
  }
  public void set_fax(String fax) {
    this.fax = fax;
  }
  public QueryResult with_fax(String fax) {
    this.fax = fax;
    return this;
  }
  private java.sql.Timestamp rtime;
  public java.sql.Timestamp get_rtime() {
    return rtime;
  }
  public void set_rtime(java.sql.Timestamp rtime) {
    this.rtime = rtime;
  }
  public QueryResult with_rtime(java.sql.Timestamp rtime) {
    this.rtime = rtime;
    return this;
  }
  private Integer certified;
  public Integer get_certified() {
    return certified;
  }
  public void set_certified(Integer certified) {
    this.certified = certified;
  }
  public QueryResult with_certified(Integer certified) {
    this.certified = certified;
    return this;
  }
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof QueryResult)) {
      return false;
    }
    QueryResult that = (QueryResult) o;
    boolean equal = true;
    equal = equal && (this.corp_seq == null ? that.corp_seq == null : this.corp_seq.equals(that.corp_seq));
    equal = equal && (this.id == null ? that.id == null : this.id.equals(that.id));
    equal = equal && (this.seller == null ? that.seller == null : this.seller.equals(that.seller));
    equal = equal && (this.name == null ? that.name == null : this.name.equals(that.name));
    equal = equal && (this.delegate == null ? that.delegate == null : this.delegate.equals(that.delegate));
    equal = equal && (this.addr == null ? that.addr == null : this.addr.equals(that.addr));
    equal = equal && (this.email == null ? that.email == null : this.email.equals(that.email));
    equal = equal && (this.corp_number == null ? that.corp_number == null : this.corp_number.equals(that.corp_number));
    equal = equal && (this.cellular_phone1 == null ? that.cellular_phone1 == null : this.cellular_phone1.equals(that.cellular_phone1));
    equal = equal && (this.cellular_phone2 == null ? that.cellular_phone2 == null : this.cellular_phone2.equals(that.cellular_phone2));
    equal = equal && (this.cellular_phone3 == null ? that.cellular_phone3 == null : this.cellular_phone3.equals(that.cellular_phone3));
    equal = equal && (this.corp_phone1 == null ? that.corp_phone1 == null : this.corp_phone1.equals(that.corp_phone1));
    equal = equal && (this.corp_phone2 == null ? that.corp_phone2 == null : this.corp_phone2.equals(that.corp_phone2));
    equal = equal && (this.fax == null ? that.fax == null : this.fax.equals(that.fax));
    equal = equal && (this.rtime == null ? that.rtime == null : this.rtime.equals(that.rtime));
    equal = equal && (this.certified == null ? that.certified == null : this.certified.equals(that.certified));
    return equal;
  }
  public boolean equals0(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof QueryResult)) {
      return false;
    }
    QueryResult that = (QueryResult) o;
    boolean equal = true;
    equal = equal && (this.corp_seq == null ? that.corp_seq == null : this.corp_seq.equals(that.corp_seq));
    equal = equal && (this.id == null ? that.id == null : this.id.equals(that.id));
    equal = equal && (this.seller == null ? that.seller == null : this.seller.equals(that.seller));
    equal = equal && (this.name == null ? that.name == null : this.name.equals(that.name));
    equal = equal && (this.delegate == null ? that.delegate == null : this.delegate.equals(that.delegate));
    equal = equal && (this.addr == null ? that.addr == null : this.addr.equals(that.addr));
    equal = equal && (this.email == null ? that.email == null : this.email.equals(that.email));
    equal = equal && (this.corp_number == null ? that.corp_number == null : this.corp_number.equals(that.corp_number));
    equal = equal && (this.cellular_phone1 == null ? that.cellular_phone1 == null : this.cellular_phone1.equals(that.cellular_phone1));
    equal = equal && (this.cellular_phone2 == null ? that.cellular_phone2 == null : this.cellular_phone2.equals(that.cellular_phone2));
    equal = equal && (this.cellular_phone3 == null ? that.cellular_phone3 == null : this.cellular_phone3.equals(that.cellular_phone3));
    equal = equal && (this.corp_phone1 == null ? that.corp_phone1 == null : this.corp_phone1.equals(that.corp_phone1));
    equal = equal && (this.corp_phone2 == null ? that.corp_phone2 == null : this.corp_phone2.equals(that.corp_phone2));
    equal = equal && (this.fax == null ? that.fax == null : this.fax.equals(that.fax));
    equal = equal && (this.rtime == null ? that.rtime == null : this.rtime.equals(that.rtime));
    equal = equal && (this.certified == null ? that.certified == null : this.certified.equals(that.certified));
    return equal;
  }
  public void readFields(ResultSet __dbResults) throws SQLException {
    this.__cur_result_set = __dbResults;
    this.corp_seq = JdbcWritableBridge.readLong(1, __dbResults);
    this.id = JdbcWritableBridge.readString(2, __dbResults);
    this.seller = JdbcWritableBridge.readString(3, __dbResults);
    this.name = JdbcWritableBridge.readString(4, __dbResults);
    this.delegate = JdbcWritableBridge.readString(5, __dbResults);
    this.addr = JdbcWritableBridge.readString(6, __dbResults);
    this.email = JdbcWritableBridge.readString(7, __dbResults);
    this.corp_number = JdbcWritableBridge.readString(8, __dbResults);
    this.cellular_phone1 = JdbcWritableBridge.readString(9, __dbResults);
    this.cellular_phone2 = JdbcWritableBridge.readString(10, __dbResults);
    this.cellular_phone3 = JdbcWritableBridge.readString(11, __dbResults);
    this.corp_phone1 = JdbcWritableBridge.readString(12, __dbResults);
    this.corp_phone2 = JdbcWritableBridge.readString(13, __dbResults);
    this.fax = JdbcWritableBridge.readString(14, __dbResults);
    this.rtime = JdbcWritableBridge.readTimestamp(15, __dbResults);
    this.certified = JdbcWritableBridge.readInteger(16, __dbResults);
  }
  public void readFields0(ResultSet __dbResults) throws SQLException {
    this.corp_seq = JdbcWritableBridge.readLong(1, __dbResults);
    this.id = JdbcWritableBridge.readString(2, __dbResults);
    this.seller = JdbcWritableBridge.readString(3, __dbResults);
    this.name = JdbcWritableBridge.readString(4, __dbResults);
    this.delegate = JdbcWritableBridge.readString(5, __dbResults);
    this.addr = JdbcWritableBridge.readString(6, __dbResults);
    this.email = JdbcWritableBridge.readString(7, __dbResults);
    this.corp_number = JdbcWritableBridge.readString(8, __dbResults);
    this.cellular_phone1 = JdbcWritableBridge.readString(9, __dbResults);
    this.cellular_phone2 = JdbcWritableBridge.readString(10, __dbResults);
    this.cellular_phone3 = JdbcWritableBridge.readString(11, __dbResults);
    this.corp_phone1 = JdbcWritableBridge.readString(12, __dbResults);
    this.corp_phone2 = JdbcWritableBridge.readString(13, __dbResults);
    this.fax = JdbcWritableBridge.readString(14, __dbResults);
    this.rtime = JdbcWritableBridge.readTimestamp(15, __dbResults);
    this.certified = JdbcWritableBridge.readInteger(16, __dbResults);
  }
  public void loadLargeObjects(LargeObjectLoader __loader)
      throws SQLException, IOException, InterruptedException {
  }
  public void loadLargeObjects0(LargeObjectLoader __loader)
      throws SQLException, IOException, InterruptedException {
  }
  public void write(PreparedStatement __dbStmt) throws SQLException {
    write(__dbStmt, 0);
  }

  public int write(PreparedStatement __dbStmt, int __off) throws SQLException {
    JdbcWritableBridge.writeLong(corp_seq, 1 + __off, -5, __dbStmt);
    JdbcWritableBridge.writeString(id, 2 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(seller, 3 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(name, 4 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(delegate, 5 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(addr, 6 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(email, 7 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(corp_number, 8 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(cellular_phone1, 9 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(cellular_phone2, 10 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(cellular_phone3, 11 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(corp_phone1, 12 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(corp_phone2, 13 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(fax, 14 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeTimestamp(rtime, 15 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeInteger(certified, 16 + __off, -6, __dbStmt);
    return 16;
  }
  public void write0(PreparedStatement __dbStmt, int __off) throws SQLException {
    JdbcWritableBridge.writeLong(corp_seq, 1 + __off, -5, __dbStmt);
    JdbcWritableBridge.writeString(id, 2 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(seller, 3 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(name, 4 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(delegate, 5 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(addr, 6 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(email, 7 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(corp_number, 8 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(cellular_phone1, 9 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(cellular_phone2, 10 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(cellular_phone3, 11 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(corp_phone1, 12 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(corp_phone2, 13 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(fax, 14 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeTimestamp(rtime, 15 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeInteger(certified, 16 + __off, -6, __dbStmt);
  }
  public void readFields(DataInput __dataIn) throws IOException {
this.readFields0(__dataIn);  }
  public void readFields0(DataInput __dataIn) throws IOException {
    if (__dataIn.readBoolean()) { 
        this.corp_seq = null;
    } else {
    this.corp_seq = Long.valueOf(__dataIn.readLong());
    }
    if (__dataIn.readBoolean()) { 
        this.id = null;
    } else {
    this.id = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.seller = null;
    } else {
    this.seller = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.name = null;
    } else {
    this.name = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.delegate = null;
    } else {
    this.delegate = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.addr = null;
    } else {
    this.addr = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.email = null;
    } else {
    this.email = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.corp_number = null;
    } else {
    this.corp_number = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.cellular_phone1 = null;
    } else {
    this.cellular_phone1 = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.cellular_phone2 = null;
    } else {
    this.cellular_phone2 = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.cellular_phone3 = null;
    } else {
    this.cellular_phone3 = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.corp_phone1 = null;
    } else {
    this.corp_phone1 = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.corp_phone2 = null;
    } else {
    this.corp_phone2 = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.fax = null;
    } else {
    this.fax = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.rtime = null;
    } else {
    this.rtime = new Timestamp(__dataIn.readLong());
    this.rtime.setNanos(__dataIn.readInt());
    }
    if (__dataIn.readBoolean()) { 
        this.certified = null;
    } else {
    this.certified = Integer.valueOf(__dataIn.readInt());
    }
  }
  public void write(DataOutput __dataOut) throws IOException {
    if (null == this.corp_seq) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.corp_seq);
    }
    if (null == this.id) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, id);
    }
    if (null == this.seller) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, seller);
    }
    if (null == this.name) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, name);
    }
    if (null == this.delegate) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, delegate);
    }
    if (null == this.addr) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, addr);
    }
    if (null == this.email) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, email);
    }
    if (null == this.corp_number) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, corp_number);
    }
    if (null == this.cellular_phone1) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, cellular_phone1);
    }
    if (null == this.cellular_phone2) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, cellular_phone2);
    }
    if (null == this.cellular_phone3) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, cellular_phone3);
    }
    if (null == this.corp_phone1) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, corp_phone1);
    }
    if (null == this.corp_phone2) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, corp_phone2);
    }
    if (null == this.fax) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, fax);
    }
    if (null == this.rtime) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.rtime.getTime());
    __dataOut.writeInt(this.rtime.getNanos());
    }
    if (null == this.certified) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.certified);
    }
  }
  public void write0(DataOutput __dataOut) throws IOException {
    if (null == this.corp_seq) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.corp_seq);
    }
    if (null == this.id) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, id);
    }
    if (null == this.seller) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, seller);
    }
    if (null == this.name) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, name);
    }
    if (null == this.delegate) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, delegate);
    }
    if (null == this.addr) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, addr);
    }
    if (null == this.email) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, email);
    }
    if (null == this.corp_number) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, corp_number);
    }
    if (null == this.cellular_phone1) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, cellular_phone1);
    }
    if (null == this.cellular_phone2) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, cellular_phone2);
    }
    if (null == this.cellular_phone3) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, cellular_phone3);
    }
    if (null == this.corp_phone1) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, corp_phone1);
    }
    if (null == this.corp_phone2) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, corp_phone2);
    }
    if (null == this.fax) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, fax);
    }
    if (null == this.rtime) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.rtime.getTime());
    __dataOut.writeInt(this.rtime.getNanos());
    }
    if (null == this.certified) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.certified);
    }
  }
  private static final DelimiterSet __outputDelimiters = new DelimiterSet((char) 1, (char) 10, (char) 0, (char) 0, false);
  public String toString() {
    return toString(__outputDelimiters, true);
  }
  public String toString(DelimiterSet delimiters) {
    return toString(delimiters, true);
  }
  public String toString(boolean useRecordDelim) {
    return toString(__outputDelimiters, useRecordDelim);
  }
  public String toString(DelimiterSet delimiters, boolean useRecordDelim) {
    StringBuilder __sb = new StringBuilder();
    char fieldDelim = delimiters.getFieldsTerminatedBy();
    __sb.append(FieldFormatter.escapeAndEnclose(corp_seq==null?"null":"" + corp_seq, delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, droppingdelimiters \n,\r,\01 from strings
    __sb.append(FieldFormatter.hiveStringDropDelims(id==null?"null":id, delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, droppingdelimiters \n,\r,\01 from strings
    __sb.append(FieldFormatter.hiveStringDropDelims(seller==null?"null":seller, delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, droppingdelimiters \n,\r,\01 from strings
    __sb.append(FieldFormatter.hiveStringDropDelims(name==null?"null":name, delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, droppingdelimiters \n,\r,\01 from strings
    __sb.append(FieldFormatter.hiveStringDropDelims(delegate==null?"null":delegate, delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, droppingdelimiters \n,\r,\01 from strings
    __sb.append(FieldFormatter.hiveStringDropDelims(addr==null?"null":addr, delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, droppingdelimiters \n,\r,\01 from strings
    __sb.append(FieldFormatter.hiveStringDropDelims(email==null?"null":email, delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, droppingdelimiters \n,\r,\01 from strings
    __sb.append(FieldFormatter.hiveStringDropDelims(corp_number==null?"null":corp_number, delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, droppingdelimiters \n,\r,\01 from strings
    __sb.append(FieldFormatter.hiveStringDropDelims(cellular_phone1==null?"null":cellular_phone1, delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, droppingdelimiters \n,\r,\01 from strings
    __sb.append(FieldFormatter.hiveStringDropDelims(cellular_phone2==null?"null":cellular_phone2, delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, droppingdelimiters \n,\r,\01 from strings
    __sb.append(FieldFormatter.hiveStringDropDelims(cellular_phone3==null?"null":cellular_phone3, delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, droppingdelimiters \n,\r,\01 from strings
    __sb.append(FieldFormatter.hiveStringDropDelims(corp_phone1==null?"null":corp_phone1, delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, droppingdelimiters \n,\r,\01 from strings
    __sb.append(FieldFormatter.hiveStringDropDelims(corp_phone2==null?"null":corp_phone2, delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, droppingdelimiters \n,\r,\01 from strings
    __sb.append(FieldFormatter.hiveStringDropDelims(fax==null?"null":fax, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(rtime==null?"null":"" + rtime, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(certified==null?"null":"" + certified, delimiters));
    if (useRecordDelim) {
      __sb.append(delimiters.getLinesTerminatedBy());
    }
    return __sb.toString();
  }
  public void toString0(DelimiterSet delimiters, StringBuilder __sb, char fieldDelim) {
    __sb.append(FieldFormatter.escapeAndEnclose(corp_seq==null?"null":"" + corp_seq, delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, droppingdelimiters \n,\r,\01 from strings
    __sb.append(FieldFormatter.hiveStringDropDelims(id==null?"null":id, delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, droppingdelimiters \n,\r,\01 from strings
    __sb.append(FieldFormatter.hiveStringDropDelims(seller==null?"null":seller, delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, droppingdelimiters \n,\r,\01 from strings
    __sb.append(FieldFormatter.hiveStringDropDelims(name==null?"null":name, delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, droppingdelimiters \n,\r,\01 from strings
    __sb.append(FieldFormatter.hiveStringDropDelims(delegate==null?"null":delegate, delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, droppingdelimiters \n,\r,\01 from strings
    __sb.append(FieldFormatter.hiveStringDropDelims(addr==null?"null":addr, delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, droppingdelimiters \n,\r,\01 from strings
    __sb.append(FieldFormatter.hiveStringDropDelims(email==null?"null":email, delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, droppingdelimiters \n,\r,\01 from strings
    __sb.append(FieldFormatter.hiveStringDropDelims(corp_number==null?"null":corp_number, delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, droppingdelimiters \n,\r,\01 from strings
    __sb.append(FieldFormatter.hiveStringDropDelims(cellular_phone1==null?"null":cellular_phone1, delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, droppingdelimiters \n,\r,\01 from strings
    __sb.append(FieldFormatter.hiveStringDropDelims(cellular_phone2==null?"null":cellular_phone2, delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, droppingdelimiters \n,\r,\01 from strings
    __sb.append(FieldFormatter.hiveStringDropDelims(cellular_phone3==null?"null":cellular_phone3, delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, droppingdelimiters \n,\r,\01 from strings
    __sb.append(FieldFormatter.hiveStringDropDelims(corp_phone1==null?"null":corp_phone1, delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, droppingdelimiters \n,\r,\01 from strings
    __sb.append(FieldFormatter.hiveStringDropDelims(corp_phone2==null?"null":corp_phone2, delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, droppingdelimiters \n,\r,\01 from strings
    __sb.append(FieldFormatter.hiveStringDropDelims(fax==null?"null":fax, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(rtime==null?"null":"" + rtime, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(certified==null?"null":"" + certified, delimiters));
  }
  private static final DelimiterSet __inputDelimiters = new DelimiterSet((char) 1, (char) 10, (char) 0, (char) 0, false);
  private RecordParser __parser;
  public void parse(Text __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(CharSequence __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(byte [] __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(char [] __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(ByteBuffer __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(CharBuffer __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  private void __loadFromFields(List<String> fields) {
    Iterator<String> __it = fields.listIterator();
    String __cur_str = null;
    try {
    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.corp_seq = null; } else {
      this.corp_seq = Long.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.id = null; } else {
      this.id = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.seller = null; } else {
      this.seller = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.name = null; } else {
      this.name = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.delegate = null; } else {
      this.delegate = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.addr = null; } else {
      this.addr = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.email = null; } else {
      this.email = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.corp_number = null; } else {
      this.corp_number = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.cellular_phone1 = null; } else {
      this.cellular_phone1 = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.cellular_phone2 = null; } else {
      this.cellular_phone2 = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.cellular_phone3 = null; } else {
      this.cellular_phone3 = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.corp_phone1 = null; } else {
      this.corp_phone1 = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.corp_phone2 = null; } else {
      this.corp_phone2 = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.fax = null; } else {
      this.fax = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.rtime = null; } else {
      this.rtime = java.sql.Timestamp.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.certified = null; } else {
      this.certified = Integer.valueOf(__cur_str);
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  private void __loadFromFields0(Iterator<String> __it) {
    String __cur_str = null;
    try {
    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.corp_seq = null; } else {
      this.corp_seq = Long.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.id = null; } else {
      this.id = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.seller = null; } else {
      this.seller = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.name = null; } else {
      this.name = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.delegate = null; } else {
      this.delegate = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.addr = null; } else {
      this.addr = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.email = null; } else {
      this.email = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.corp_number = null; } else {
      this.corp_number = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.cellular_phone1 = null; } else {
      this.cellular_phone1 = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.cellular_phone2 = null; } else {
      this.cellular_phone2 = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.cellular_phone3 = null; } else {
      this.cellular_phone3 = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.corp_phone1 = null; } else {
      this.corp_phone1 = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.corp_phone2 = null; } else {
      this.corp_phone2 = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.fax = null; } else {
      this.fax = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.rtime = null; } else {
      this.rtime = java.sql.Timestamp.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.certified = null; } else {
      this.certified = Integer.valueOf(__cur_str);
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  public Object clone() throws CloneNotSupportedException {
    QueryResult o = (QueryResult) super.clone();
    o.rtime = (o.rtime != null) ? (java.sql.Timestamp) o.rtime.clone() : null;
    return o;
  }

  public void clone0(QueryResult o) throws CloneNotSupportedException {
    o.rtime = (o.rtime != null) ? (java.sql.Timestamp) o.rtime.clone() : null;
  }

  public Map<String, Object> getFieldMap() {
    Map<String, Object> __sqoop$field_map = new HashMap<String, Object>();
    __sqoop$field_map.put("corp_seq", this.corp_seq);
    __sqoop$field_map.put("id", this.id);
    __sqoop$field_map.put("seller", this.seller);
    __sqoop$field_map.put("name", this.name);
    __sqoop$field_map.put("delegate", this.delegate);
    __sqoop$field_map.put("addr", this.addr);
    __sqoop$field_map.put("email", this.email);
    __sqoop$field_map.put("corp_number", this.corp_number);
    __sqoop$field_map.put("cellular_phone1", this.cellular_phone1);
    __sqoop$field_map.put("cellular_phone2", this.cellular_phone2);
    __sqoop$field_map.put("cellular_phone3", this.cellular_phone3);
    __sqoop$field_map.put("corp_phone1", this.corp_phone1);
    __sqoop$field_map.put("corp_phone2", this.corp_phone2);
    __sqoop$field_map.put("fax", this.fax);
    __sqoop$field_map.put("rtime", this.rtime);
    __sqoop$field_map.put("certified", this.certified);
    return __sqoop$field_map;
  }

  public void getFieldMap0(Map<String, Object> __sqoop$field_map) {
    __sqoop$field_map.put("corp_seq", this.corp_seq);
    __sqoop$field_map.put("id", this.id);
    __sqoop$field_map.put("seller", this.seller);
    __sqoop$field_map.put("name", this.name);
    __sqoop$field_map.put("delegate", this.delegate);
    __sqoop$field_map.put("addr", this.addr);
    __sqoop$field_map.put("email", this.email);
    __sqoop$field_map.put("corp_number", this.corp_number);
    __sqoop$field_map.put("cellular_phone1", this.cellular_phone1);
    __sqoop$field_map.put("cellular_phone2", this.cellular_phone2);
    __sqoop$field_map.put("cellular_phone3", this.cellular_phone3);
    __sqoop$field_map.put("corp_phone1", this.corp_phone1);
    __sqoop$field_map.put("corp_phone2", this.corp_phone2);
    __sqoop$field_map.put("fax", this.fax);
    __sqoop$field_map.put("rtime", this.rtime);
    __sqoop$field_map.put("certified", this.certified);
  }

  public void setField(String __fieldName, Object __fieldVal) {
    if (!setters.containsKey(__fieldName)) {
      throw new RuntimeException("No such field:"+__fieldName);
    }
    setters.get(__fieldName).setField(__fieldVal);
  }

}
