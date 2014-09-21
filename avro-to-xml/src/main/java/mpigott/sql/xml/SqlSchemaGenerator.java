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

import javax.xml.namespace.QName;

import org.apache.ws.commons.schema.XmlSchemaCollection;
import org.apache.ws.commons.schema.docpath.XmlSchemaStateMachineNode;

/**
 * Builds an SQL schema from an XML Schema.
 *
 * @author  Mike Pigott
 */
public class SqlSchemaGenerator {

  public SqlSchemaGenerator(XmlSchemaCollection xmlSchemaCollection) {
    
  }

  public XmlSchemaStateMachineNode createStateMachineFor(QName root) {
    return null;
  }

  public SqlSchema createSchemaFrom(XmlSchemaStateMachineNode root) {
    return new SqlSchema();
  }
}
