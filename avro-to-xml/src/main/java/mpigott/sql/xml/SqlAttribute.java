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

/**
 * Represents an attribute in a relational database's table.
 *
 * @author  Mike Pigott
 */
public final class SqlAttribute {
  public enum Type {
    BIGINT,            // XSD_LONG
    INT,               // XSD_INT
    SMALLINT,          // XSD_SHORT
    TINYINT,           // XSD_BYTE
    DECIMAL,           // XSD_DECIMAL
    NUMERIC,           // XSD_DECIMAL
    FLOAT,             // XSD_FLOAT
    REAL,              // XSD_DOUBLE
    DATETIME,          // XSD_DATETIME
    SMALLDATETIME,     // XSD_DATETIME
    DATE,              // XSD_DATE
    TIME,              // XSD_TIME
    CHAR,              // XSD_STRING
    VARCHAR,           // XSD_STRING
    TEXT,              // XSD_STRING
    NCHAR,             // XSD_STRING (Unicode)
    NVARCHAR,          // XSD_STRING (Unicode)
    NTEXT,             // XSD_STRING (Unicode)
    BINARY,            // XSD_BASE64 or XSD_HEXBIN
    VARBINARY,         // XSD_BASE64 or XSD_HEXBIN
    IMAGE,             // XSD_BASE64 or XSD_HEXBIN
    SQL_VARIANT,       // Union; specific to SQL Server?
    TIMESTAMP,         // XSD_DATETIME
    UNIQUE_ID,         // GUID
    XML                // xsd:any :-)
  }

  Type type;
  String name;
  boolean isNullable;
  // Other nice things.
}
