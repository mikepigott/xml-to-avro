/**
 * Autogenerated by Avro
 * 
 * DO NOT EDIT DIRECTLY
 */
package avro.complex_schema;  
@SuppressWarnings("all")
/**  This is a map containing a map of a union of firstMap and secondMap. The maps are generated as all three have a single required attribute of type ID. The inner two maps become a union because a union of two map types is not allowed. Likewise, the map itself must become a union of two other types.  */
@org.apache.avro.specific.AvroGenerated
public class allTheThings extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"allTheThings\",\"namespace\":\"avro.complex_schema\",\"doc\":\" This is a map containing a map of a union of firstMap and secondMap. The maps are generated as all three have a single required attribute of type ID. The inner two maps become a union because a union of two map types is not allowed. Likewise, the map itself must become a union of two other types. \",\"fields\":[{\"name\":\"truth\",\"type\":\"boolean\"},{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"listOfNumbers\",\"type\":{\"type\":\"array\",\"items\":[\"int\",{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"scale\":0,\"precision\":34}]}},{\"name\":\"allTheThings\",\"type\":{\"type\":\"array\",\"items\":[{\"type\":\"map\",\"values\":[{\"type\":\"record\",\"name\":\"firstMap\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"firstMap\",\"type\":{\"type\":\"array\",\"items\":[{\"type\":\"record\",\"name\":\"value\",\"fields\":[{\"name\":\"value\",\"type\":[{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"scale\":0,\"precision\":34},\"null\"],\"doc\":\"Simple type {http://www.w3.org/2001/XMLSchema}decimal\"}]}]},\"doc\":\"Children of {urn:avro:complex_schema}firstMap\"}]},{\"type\":\"record\",\"name\":\"secondMap\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"value\",\"type\":\"string\"},{\"name\":\"secondMap\",\"type\":\"null\",\"doc\":\"This element contains no attributes and no children.\"}]}]}]},\"doc\":\"Children of {urn:avro:complex_schema}allTheThings\"}]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }
  @Deprecated public boolean truth;
  @Deprecated public java.lang.CharSequence id;
  @Deprecated public java.util.List<java.lang.Object> listOfNumbers;
  /** Children of {urn:avro:complex_schema}allTheThings */
  @Deprecated public java.util.List<java.lang.Object> allTheThings;

  /**
   * Default constructor.  Note that this does not initialize fields
   * to their default values from the schema.  If that is desired then
   * one should use <code>newBuilder()</code>. 
   */
  public allTheThings() {}

  /**
   * All-args constructor.
   */
  public allTheThings(java.lang.Boolean truth, java.lang.CharSequence id, java.util.List<java.lang.Object> listOfNumbers, java.util.List<java.lang.Object> allTheThings) {
    this.truth = truth;
    this.id = id;
    this.listOfNumbers = listOfNumbers;
    this.allTheThings = allTheThings;
  }

  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call. 
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return truth;
    case 1: return id;
    case 2: return listOfNumbers;
    case 3: return allTheThings;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
  // Used by DatumReader.  Applications should not call. 
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: truth = (java.lang.Boolean)value$; break;
    case 1: id = (java.lang.CharSequence)value$; break;
    case 2: listOfNumbers = (java.util.List<java.lang.Object>)value$; break;
    case 3: allTheThings = (java.util.List<java.lang.Object>)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  /**
   * Gets the value of the 'truth' field.
   */
  public java.lang.Boolean getTruth() {
    return truth;
  }

  /**
   * Sets the value of the 'truth' field.
   * @param value the value to set.
   */
  public void setTruth(java.lang.Boolean value) {
    this.truth = value;
  }

  /**
   * Gets the value of the 'id' field.
   */
  public java.lang.CharSequence getId() {
    return id;
  }

  /**
   * Sets the value of the 'id' field.
   * @param value the value to set.
   */
  public void setId(java.lang.CharSequence value) {
    this.id = value;
  }

  /**
   * Gets the value of the 'listOfNumbers' field.
   */
  public java.util.List<java.lang.Object> getListOfNumbers() {
    return listOfNumbers;
  }

  /**
   * Sets the value of the 'listOfNumbers' field.
   * @param value the value to set.
   */
  public void setListOfNumbers(java.util.List<java.lang.Object> value) {
    this.listOfNumbers = value;
  }

  /**
   * Gets the value of the 'allTheThings' field.
   * Children of {urn:avro:complex_schema}allTheThings   */
  public java.util.List<java.lang.Object> getAllTheThings() {
    return allTheThings;
  }

  /**
   * Sets the value of the 'allTheThings' field.
   * Children of {urn:avro:complex_schema}allTheThings   * @param value the value to set.
   */
  public void setAllTheThings(java.util.List<java.lang.Object> value) {
    this.allTheThings = value;
  }

  /** Creates a new allTheThings RecordBuilder */
  public static avro.complex_schema.allTheThings.Builder newBuilder() {
    return new avro.complex_schema.allTheThings.Builder();
  }
  
  /** Creates a new allTheThings RecordBuilder by copying an existing Builder */
  public static avro.complex_schema.allTheThings.Builder newBuilder(avro.complex_schema.allTheThings.Builder other) {
    return new avro.complex_schema.allTheThings.Builder(other);
  }
  
  /** Creates a new allTheThings RecordBuilder by copying an existing allTheThings instance */
  public static avro.complex_schema.allTheThings.Builder newBuilder(avro.complex_schema.allTheThings other) {
    return new avro.complex_schema.allTheThings.Builder(other);
  }
  
  /**
   * RecordBuilder for allTheThings instances.
   */
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<allTheThings>
    implements org.apache.avro.data.RecordBuilder<allTheThings> {

    private boolean truth;
    private java.lang.CharSequence id;
    private java.util.List<java.lang.Object> listOfNumbers;
    private java.util.List<java.lang.Object> allTheThings;

    /** Creates a new Builder */
    private Builder() {
      super(avro.complex_schema.allTheThings.SCHEMA$);
    }
    
    /** Creates a Builder by copying an existing Builder */
    private Builder(avro.complex_schema.allTheThings.Builder other) {
      super(other);
      if (isValidValue(fields()[0], other.truth)) {
        this.truth = data().deepCopy(fields()[0].schema(), other.truth);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.id)) {
        this.id = data().deepCopy(fields()[1].schema(), other.id);
        fieldSetFlags()[1] = true;
      }
      if (isValidValue(fields()[2], other.listOfNumbers)) {
        this.listOfNumbers = data().deepCopy(fields()[2].schema(), other.listOfNumbers);
        fieldSetFlags()[2] = true;
      }
      if (isValidValue(fields()[3], other.allTheThings)) {
        this.allTheThings = data().deepCopy(fields()[3].schema(), other.allTheThings);
        fieldSetFlags()[3] = true;
      }
    }
    
    /** Creates a Builder by copying an existing allTheThings instance */
    private Builder(avro.complex_schema.allTheThings other) {
            super(avro.complex_schema.allTheThings.SCHEMA$);
      if (isValidValue(fields()[0], other.truth)) {
        this.truth = data().deepCopy(fields()[0].schema(), other.truth);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.id)) {
        this.id = data().deepCopy(fields()[1].schema(), other.id);
        fieldSetFlags()[1] = true;
      }
      if (isValidValue(fields()[2], other.listOfNumbers)) {
        this.listOfNumbers = data().deepCopy(fields()[2].schema(), other.listOfNumbers);
        fieldSetFlags()[2] = true;
      }
      if (isValidValue(fields()[3], other.allTheThings)) {
        this.allTheThings = data().deepCopy(fields()[3].schema(), other.allTheThings);
        fieldSetFlags()[3] = true;
      }
    }

    /** Gets the value of the 'truth' field */
    public java.lang.Boolean getTruth() {
      return truth;
    }
    
    /** Sets the value of the 'truth' field */
    public avro.complex_schema.allTheThings.Builder setTruth(boolean value) {
      validate(fields()[0], value);
      this.truth = value;
      fieldSetFlags()[0] = true;
      return this; 
    }
    
    /** Checks whether the 'truth' field has been set */
    public boolean hasTruth() {
      return fieldSetFlags()[0];
    }
    
    /** Clears the value of the 'truth' field */
    public avro.complex_schema.allTheThings.Builder clearTruth() {
      fieldSetFlags()[0] = false;
      return this;
    }

    /** Gets the value of the 'id' field */
    public java.lang.CharSequence getId() {
      return id;
    }
    
    /** Sets the value of the 'id' field */
    public avro.complex_schema.allTheThings.Builder setId(java.lang.CharSequence value) {
      validate(fields()[1], value);
      this.id = value;
      fieldSetFlags()[1] = true;
      return this; 
    }
    
    /** Checks whether the 'id' field has been set */
    public boolean hasId() {
      return fieldSetFlags()[1];
    }
    
    /** Clears the value of the 'id' field */
    public avro.complex_schema.allTheThings.Builder clearId() {
      id = null;
      fieldSetFlags()[1] = false;
      return this;
    }

    /** Gets the value of the 'listOfNumbers' field */
    public java.util.List<java.lang.Object> getListOfNumbers() {
      return listOfNumbers;
    }
    
    /** Sets the value of the 'listOfNumbers' field */
    public avro.complex_schema.allTheThings.Builder setListOfNumbers(java.util.List<java.lang.Object> value) {
      validate(fields()[2], value);
      this.listOfNumbers = value;
      fieldSetFlags()[2] = true;
      return this; 
    }
    
    /** Checks whether the 'listOfNumbers' field has been set */
    public boolean hasListOfNumbers() {
      return fieldSetFlags()[2];
    }
    
    /** Clears the value of the 'listOfNumbers' field */
    public avro.complex_schema.allTheThings.Builder clearListOfNumbers() {
      listOfNumbers = null;
      fieldSetFlags()[2] = false;
      return this;
    }

    /** Gets the value of the 'allTheThings' field */
    public java.util.List<java.lang.Object> getAllTheThings() {
      return allTheThings;
    }
    
    /** Sets the value of the 'allTheThings' field */
    public avro.complex_schema.allTheThings.Builder setAllTheThings(java.util.List<java.lang.Object> value) {
      validate(fields()[3], value);
      this.allTheThings = value;
      fieldSetFlags()[3] = true;
      return this; 
    }
    
    /** Checks whether the 'allTheThings' field has been set */
    public boolean hasAllTheThings() {
      return fieldSetFlags()[3];
    }
    
    /** Clears the value of the 'allTheThings' field */
    public avro.complex_schema.allTheThings.Builder clearAllTheThings() {
      allTheThings = null;
      fieldSetFlags()[3] = false;
      return this;
    }

    @Override
    public allTheThings build() {
      try {
        allTheThings record = new allTheThings();
        record.truth = fieldSetFlags()[0] ? this.truth : (java.lang.Boolean) defaultValue(fields()[0]);
        record.id = fieldSetFlags()[1] ? this.id : (java.lang.CharSequence) defaultValue(fields()[1]);
        record.listOfNumbers = fieldSetFlags()[2] ? this.listOfNumbers : (java.util.List<java.lang.Object>) defaultValue(fields()[2]);
        record.allTheThings = fieldSetFlags()[3] ? this.allTheThings : (java.util.List<java.lang.Object>) defaultValue(fields()[3]);
        return record;
      } catch (Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }
}
