/**
 * Copyright 2014 Mike Pigott
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package mpigott.sql.xml;

import java.sql.Types;
import java.util.HashMap;
import java.util.Set;

import javax.xml.namespace.QName;

import org.apache.ws.commons.schema.constants.Constants;

/**
 * Represents an SQL data type.
 *
 * @author  Mike Pigott
 */
public enum SqlType {
  ARRAY(Types.ARRAY),
  BIGINT(Types.BIGINT, Constants.XSD_LONG),
  BINARY(Types.BINARY, Constants.XSD_BASE64, Constants.XSD_HEXBIN),
  BIT(Types.BIT, Constants.XSD_BOOLEAN),
  BLOB(Types.BLOB, Constants.XSD_BASE64, Constants.XSD_HEXBIN),
  CHAR(Types.CHAR, Constants.XSD_STRING),
  CLOB(Types.CLOB, Constants.XSD_STRING),
  DATALINK(Types.DATALINK, Constants.XSD_ANYURI),
  DATE(Types.DATE, Constants.XSD_DATE),
  DECIMAL(Types.DECIMAL, Constants.XSD_DECIMAL),
  DISTINCT(Types.DISTINCT),
  DOUBLE(Types.DOUBLE, Constants.XSD_DOUBLE),
  FLOAT(Types.FLOAT, Constants.XSD_FLOAT),
  INTEGER(Types.INTEGER, Constants.XSD_INT),
  JAVA_OBJECT(Types.JAVA_OBJECT),
  LONGNVARCHAR(Types.LONGNVARCHAR, Constants.XSD_STRING),
  LONGVARBINARY(Types.LONGVARBINARY, Constants.XSD_HEXBIN, Constants.XSD_BASE64),
  LONGVARCHAR(Types.LONGVARCHAR, Constants.XSD_STRING),
  OTHER(Types.OTHER),
  REAL(Types.REAL, Constants.XSD_FLOAT),
  REF(Types.REF),
  ROWID(Types.ROWID, Constants.XSD_ID),
  SMALLINT(Types.SMALLINT, Constants.XSD_SHORT),
  SQLXML(Types.SQLXML, Constants.XSD_ANYTYPE),
  STRUCT(Types.STRUCT),
  TIME(Types.TIME, Constants.XSD_TIME),
  TINYINT(Types.TINYINT, Constants.XSD_BYTE),
  VARBINARY(Types.VARBINARY, Constants.XSD_BASE64, Constants.XSD_HEXBIN),
  VARCHAR(Types.VARCHAR, Constants.XSD_STRING);

  private static HashMap<QName, SqlType> sqlTypesByQName =
      new HashMap<QName, SqlType>();

  private final int jdbcType;
  private final QName[] xmlTypes;

  static {
    sqlTypesByQName.put(Constants.XSD_ANYURI, DATALINK);
    sqlTypesByQName.put(Constants.XSD_ANYTYPE, SQLXML);
    sqlTypesByQName.put(Constants.XSD_BASE64, BLOB);
    sqlTypesByQName.put(Constants.XSD_BOOLEAN, BIT);
    sqlTypesByQName.put(Constants.XSD_BYTE, TINYINT);
    sqlTypesByQName.put(Constants.XSD_DATE, DATE);
    sqlTypesByQName.put(Constants.XSD_DECIMAL, DECIMAL);
    sqlTypesByQName.put(Constants.XSD_DOUBLE, DOUBLE);
    sqlTypesByQName.put(Constants.XSD_FLOAT, FLOAT);
    sqlTypesByQName.put(Constants.XSD_HEXBIN, BLOB);
    sqlTypesByQName.put(Constants.XSD_ID, ROWID);
    sqlTypesByQName.put(Constants.XSD_INT, INTEGER);
    sqlTypesByQName.put(Constants.XSD_LONG, BIGINT);
    sqlTypesByQName.put(Constants.XSD_SHORT, SMALLINT);
    sqlTypesByQName.put(Constants.XSD_STRING, VARCHAR);
    sqlTypesByQName.put(Constants.XSD_TIME, TIME);
    sqlTypesByQName.put(Constants.XSD_UNSIGNEDBYTE, SMALLINT);
    sqlTypesByQName.put(Constants.XSD_UNSIGNEDINT, BIGINT);
    sqlTypesByQName.put(Constants.XSD_UNSIGNEDSHORT, INTEGER);
  }

  public static Set<QName> getRecognizedTypes() {
    return sqlTypesByQName.keySet();
  }

  public static SqlType getSqlTypeFor(QName qName) {
    return sqlTypesByQName.get(qName);
  }

  private SqlType(int jdbcType, QName... xmlTypes) {
    this.jdbcType = jdbcType;
    this.xmlTypes = xmlTypes;
  }

  public int getJdbcType() {
    return jdbcType;
  }

  public QName[] getXmlType() {
    return xmlTypes;
  }
}
