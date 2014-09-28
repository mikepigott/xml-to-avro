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
import java.util.List;
import java.util.Map;

import javax.xml.namespace.QName;
import javax.xml.transform.stream.StreamSource;

import org.apache.ws.commons.schema.XmlSchemaCollection;
import org.apache.ws.commons.schema.XmlSchemaElement;
import org.apache.ws.commons.schema.docpath.XmlSchemaStateMachineGenerator;
import org.apache.ws.commons.schema.docpath.XmlSchemaStateMachineNode;
import org.apache.ws.commons.schema.walker.XmlSchemaAttrInfo;
import org.apache.ws.commons.schema.walker.XmlSchemaTypeInfo;
import org.apache.ws.commons.schema.walker.XmlSchemaWalker;

/**
 * Builds SQL from an XML.
 *
 * @author  Mike Pigott
 */
public class SqlSchemaGenerator {

  private static final String CAMEL_CASE_REGEX =
      "(?<!(^|[A-Z]))(?=[A-Z])|(?<!^)(?=[A-Z][a-z])";

  XmlSchemaCollection xmlSchemaCollection;
  XmlSchemaStateMachineNode rootNode;
  Map<QName, XmlSchemaStateMachineNode> stateMachineNodesByQName;

  public SqlSchemaGenerator(SqlXmlConfig config) throws IOException {
    xmlSchemaCollection = new XmlSchemaCollection();

    for (StreamSource source : config.getSources()) {
      xmlSchemaCollection.read(source);

      // Close the streams.
      if (source.getInputStream() != null) {
        try {
          source.getInputStream().close();
        } catch (IOException ioe) {
          ioe.printStackTrace();
        }
      }

      if (source.getReader() != null) {
        try {
          source.getReader().close();
        } catch (IOException ioe) {
          ioe.printStackTrace();
        }
      }
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

  public XmlSchemaStateMachineNode getStateMachine() {
    return rootNode;
  }

  public Map<QName, XmlSchemaStateMachineNode> getStateMachineNodesByQName() {
    return stateMachineNodesByQName;
  }

  public SqlSchema generate(SqlXmlConfig config) throws IOException {
    SqlSchema sqlSchema = new SqlSchema();

    final List<XmlSchemaAttrInfo> rootAttrs = rootNode.getAttributes();

    if ((rootAttrs != null) && !rootAttrs.isEmpty()) {
      for (XmlSchemaStateMachineNode child : rootNode.getPossibleNextStates()) {
        createSchemaFor(sqlSchema, child, null, null);
      }
    } else {
      createSchemaFor(sqlSchema, rootNode, null, null);
    }

    return sqlSchema;
  }

  private void createSchemaFor(
      SqlSchema schema,
      XmlSchemaStateMachineNode node,
      SqlTable parentTable,
      SqlRelationship relationshipToParent) {

    switch (node.getNodeType()) {
    case ELEMENT:
      {
        if ((parentTable == null)
            || (node
                  .getElementType()
                  .getType()
                  .equals(XmlSchemaTypeInfo.Type.COMPLEX))
            || !relationshipToParent.equals(SqlRelationship.ONE_TO_ONE)) {
          createTablesFor(schema, node, parentTable, relationshipToParent);
        } else {
          final SqlAttribute attr =
              new SqlAttribute(
                  getSqlNameFor(node.getElement().getQName()),
                  node.getElementType());
          parentTable.addAttribute(attr);
        }
        break;
      }
    case SUBSTITUTION_GROUP:
    case CHOICE:
      {
        /* TODO: These are one-to-many relationships,
         *       unless there is only one choice.
         */
        break;
      }
    case SEQUENCE:
    case ALL:
      {
        /* TODO: These are one-to-one relationships,
         *       unless their children are of other
         *       group types.
         */
        break;
      }
    case ANY:
      {
        // TODO: These are SQLANY fields in the parent table.
        break;
      }
    default:
    }
  }

  private void createTablesFor(SqlSchema schema,
      XmlSchemaStateMachineNode node,
      SqlTable parentTable,
      SqlRelationship relationshipToParent) {

    final SqlTable table =
        new SqlTable(getSqlNameFor( node.getElement().getQName() ), null);

    if (parentTable != null) {
      parentTable.addRelationship(relationshipToParent, table);
    }
  }

  private static String getSqlNameFor(QName qName) {
    StringBuilder sqlName = new StringBuilder( qName.getLocalPart().length() );

    final String[] parts = qName.getLocalPart().split(CAMEL_CASE_REGEX);

    for (int partIdx = 0; partIdx < parts.length - 1; ++partIdx) {
      sqlName.append(parts[partIdx]).append('_');
    }
    sqlName.append(parts[parts.length - 1]);

    // TODO: handle all other special characters.

    return sqlName.toString();
  }

  
}
