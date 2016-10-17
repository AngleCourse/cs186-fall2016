package edu.berkeley.cs186.database.table;

import edu.berkeley.cs186.database.datatypes.*;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Iterator;

/**
 * The Schema of a particular table.
 *
 * Properties:
 * `fields`: an ordered list of column names
 * `fieldTypes`: an ordered list of data types corresponding to the columns
 * `size`: physical size (in bytes) of a record conforming to this schema
 */
public class Schema {
  private List<String> fields;
  private List<DataType> fieldTypes;
  private int size;

  public Schema(List<String> fields, List<DataType> fieldTypes) {
    assert(fields.size() == fieldTypes.size());

    this.fields = fields;
    this.fieldTypes = fieldTypes;
    this.size = 0;

    for (DataType dt : fieldTypes) {
      this.size += dt.getSize();
    }
  }

  /**
   * Verifies that a list of DataTypes corresponds to this schema. A list of
   * DataTypes corresponds to this schema if the number of DataTypes in the
   * list equals the number of columns in this schema, and if each DataType has
   * the same type and size as the columns in this schema.
   *
   * @param values the list of values to check
   * @return a new Record with the DataTypes specified
   * @throws SchemaException if the values specified don't conform to this Schema
   */
  public Record verify(List<DataType> values) throws SchemaException {
    if(this.fieldTypes.size() != values.size()){
        throw new SchemaException("different length");
    }
    Iterator<DataType> it_values = values.iterator();
    Iterator<DataType> it = this.fieldTypes.iterator();
    while(it.hasNext()){
        DataType vcopy = it.next();
        DataType v = it_values.next();
        if(v.type() != vcopy.type() || v.getSize() != vcopy.getSize()){
            throw new SchemaException("wrong DataType. Expected " + 
                    v.toString() + ", got " + vcopy.toString());
        }
    }
    return  new Record(values);
  }

  /**
   * Serializes the provided record into a byte[]. Uses the DataTypes's
   * serialization methods. A serialized record is represented as the
   * concatenation of each serialized DataType. This method assumes that the
   * input record corresponds to this schema.
   *
   * @param record the record to encode
   * @return the encoded record as a byte[]
   */
  public byte[] encode(Record record) {
    int recordSize = 0;
    for(DataType v: record.getValues()){
        recordSize += v.getSize();
    }
    byte[] data = new byte[recordSize];
    int index = 0;
    for(DataType v: record.getValues()){
        for(byte b: v.getBytes()){
            data[index++] = b;
        }
    }
    return data;
  }

  /**
   * Takes a byte[] and decodes it into a Record. This method assumes that the
   * input byte[] represents a record that corresponds to this schema.
   *
   * @param input the byte array to decode
   * @return the decoded Record
   */
  public Record decode(byte[] input) {
    ArrayList<DataType> values = new ArrayList<DataType>();
    int index = 0;
    Iterator<DataType> it = this.fieldTypes.iterator();
    DataType dt;
    while(index < input.length){
        dt = it.next();
        switch(dt.type()){
            case BOOL:
                values.add(new BoolDataType(input[index] != 0));
                break;
            case INT:
                values.add(new IntDataType(
                            ByteBuffer.wrap(Arrays.copyOfRange(input, index, 
                                    index+dt.getSize())).getInt()));
                break;
            case FLOAT:
                values.add(new FloatDataType(
                            ByteBuffer.wrap(Arrays.copyOfRange(input, index,
                                    index+dt.getSize())).getFloat()));
                break;
            case STRING:
                values.add(new StringDataType(new String(
                                Arrays.copyOfRange(input, index, 
                                    index+dt.getSize())), dt.getSize()));
                break;
        }
        index += dt.getSize();
    }
    return new Record(values);
  }

  public int getEntrySize() {
    return this.size;
  }

  public List<String> getFieldNames() {
    return this.fields;
  }

  public List<DataType> getFieldTypes() {
    return this.fieldTypes;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof Schema)) {
      return false;
    }

    Schema otherSchema = (Schema) other;

    if (this.fields.size() != otherSchema.fields.size()) {
      return false;
    }

    for (int i = 0; i < this.fields.size(); i++) {
      DataType thisType = this.fieldTypes.get(i);
      DataType otherType = this.fieldTypes.get(i);

      if (thisType.type() != otherType.type()) {
        return false;
      }

      if (thisType.equals(DataType.Types.STRING) && thisType.getSize() != otherType.getSize()) {
        return false;
      }
    }

    return true;
  }
}
