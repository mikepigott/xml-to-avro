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

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.List;
import java.util.Map;

import javax.xml.namespace.QName;
import javax.xml.transform.stream.StreamSource;

import org.apache.ws.commons.schema.XmlSchemaCollection;
import org.apache.ws.commons.schema.XmlSchemaElement;
import org.apache.ws.commons.schema.docpath.XmlSchemaStateMachineGenerator;
import org.apache.ws.commons.schema.docpath.XmlSchemaStateMachineNode;
import org.apache.ws.commons.schema.walker.XmlSchemaWalker;
import org.w3c.dom.Document;

/**
 * Builds SQL from an XML.
 *
 * @author  Mike Pigott
 */
public class SqlGenerator {
  private XmlSchemaCollection xmlSchemaCollection;
  XmlSchemaStateMachineNode rootNode;
  Map<QName, XmlSchemaStateMachineNode> stateMachineNodesByQName;

  public SqlGenerator(SqlXmlConfig config) throws IOException {
    xmlSchemaCollection = new XmlSchemaCollection();

    for (StreamSource source : config.getSources()) {
      xmlSchemaCollection.read(source);
    }

    XmlSchemaStateMachineGenerator stateMachineGen =
        new XmlSchemaStateMachineGenerator();

    XmlSchemaWalker walker =
        new XmlSchemaWalker(xmlSchemaCollection, stateMachineGen);

    XmlSchemaElement rootElem =
        xmlSchemaCollection.getElementByQName(config.getRootTagName());

    walker.walk(rootElem);

    rootNode = stateMachineGen.getStartNode();
    stateMachineNodesByQName = stateMachineGen.getStateMachineNodesByQName();
  }

  // ****
  // TODO: Make this the public interface, but delegate
  //       the work to package-protected classes.
  //
  // TODO: Do we want to return the statements to run,
  //       or should we just run them ourselves?
  //
  // TODO: How should this fit in with Apache Camel, where
  //       we only need to generate the SQL statements and
  //       populate the headers with their values?
  //
  // TODO: Determine what the XML Schema recognized types are for SQL.
  // ****

  public List<PreparedStatement> createSchema(Connection conn) {
    return null;
  }

  public List<PreparedStatement> createInsertStatements(
      Connection conn,
      Document doc) {

    return null;
  }

  public List<PreparedStatement> createUpdateStatements(
      Connection conn,
      Document doc,
      Number primaryKey) {
    return null;
  }

  public List<PreparedStatement> createDeleteStatements(
      Connection conn,
      Number primaryKey) {
    return null;
  }

  
}
