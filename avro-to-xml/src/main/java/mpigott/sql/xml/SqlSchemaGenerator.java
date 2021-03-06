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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.xml.namespace.QName;
import javax.xml.transform.stream.StreamSource;

import org.apache.ws.commons.schema.XmlSchemaCollection;
import org.apache.ws.commons.schema.XmlSchemaElement;
import org.apache.ws.commons.schema.XmlSchemaUse;
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

    walker.setUserRecognizedTypes(SqlType.getRecognizedTypes());

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
        createSchemaFor(sqlSchema, child, null, null, null);
      }
    } else {
      createSchemaFor(sqlSchema, rootNode, null, null, null);
    }

    return sqlSchema;
  }

  private void createSchemaFor(
      SqlSchema schema,
      XmlSchemaStateMachineNode node,
      SqlTable parentTable,
      SqlRelationship relationshipToParent,
      List<Integer> pathFromParent) {

    final boolean isElement =
        node.getNodeType().equals(XmlSchemaStateMachineNode.Type.ELEMENT);

    final boolean isElementOrAny =
        (isElement
            || node.getNodeType().equals(XmlSchemaStateMachineNode.Type.ANY));

    final boolean newTableRequired =
        isElementOrAny
        && ((parentTable == null)
            || (isElement
                && node
                    .getElementType()
                    .getType()
                    .equals(XmlSchemaTypeInfo.Type.COMPLEX))
            || !relationshipToParent.equals(SqlRelationship.ONE_TO_ONE));

    switch (node.getNodeType()) {
    case ELEMENT:
      {
        if (newTableRequired) {
          createTablesFor(
              schema,
              node,
              parentTable,
              relationshipToParent,
              pathFromParent);
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
        if (newTableRequired) {
          createTablesFor(
              schema,
              node,
              parentTable,
              relationshipToParent,
              pathFromParent);
        } else {
          parentTable.addAttribute(new SqlAttribute(node.getAny()));
        }
        break;
      }
    default:
    }
  }

  private void createTablesFor(SqlSchema schema,
      XmlSchemaStateMachineNode node,
      SqlTable parentTable,
      SqlRelationship relationshipToParent,
      List<Integer> pathFromParent) {

    // TODO: Handle ANYs.

    final SqlTable table =
        new SqlTable(
            getSqlNameFor( node.getElement().getQName() ),
            pathFromParent);

    if (parentTable != null) {
      parentTable.addRelationship(relationshipToParent, table);
    }

    final List<Integer> pathFromNewTable = new ArrayList<Integer>();

    for (int nextPathIdx = 0;
        nextPathIdx < node.getPossibleNextStates().size();
        ++nextPathIdx) {

      pathFromNewTable.add(nextPathIdx);

      createSchemaFor(
          schema,
          node,
          table,
          SqlRelationship.ONE_TO_ONE,
          pathFromNewTable);

      pathFromNewTable.remove(pathFromNewTable.size() - 1);
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

  private static void validateXmlTypeForSqlAttribute(
      XmlSchemaTypeInfo xmlTypeInfo) {

    switch(xmlTypeInfo.getType()) {
    case COMPLEX:
      throw new IllegalArgumentException(
          "Cannot create an SqlAttribute from a complex element.");
    case UNION:
      throw new IllegalArgumentException(
          "Cannot create an SqlAttribute from a union of types.");
    case LIST:
      {
        final XmlSchemaTypeInfo.Type listType =
            xmlTypeInfo.getChildTypes().get(0).getType();
        if (listType.equals(XmlSchemaTypeInfo.Type.UNION)) {
          throw new IllegalArgumentException(
              "Cannot create an SqlAttribute from "
              + "a list of union of types.");
        }
      }
      /* falls through */
    default:
      /* falls through */
    }
  }

  private static SqlAttribute createAttribute(
      XmlSchemaStateMachineNode stateMachine) {

    if ((stateMachine.getMinOccurs() < 0L)
        || (stateMachine.getMinOccurs() > stateMachine.getMaxOccurs())) {
      throw new IllegalStateException(
          "Min occurs ("
          + stateMachine.getMinOccurs()
          + ") must be greater than zero and less-than-or-equal-to max occurs ("
          + stateMachine.getMaxOccurs()
          + ").");
    }

    if (stateMachine.getMaxOccurs() > 1L) {
      throw new IllegalArgumentException(
          "Cannot create an SqlAttribute from an element or any with a "
          + "max-occurs greater than one.");
    }

    // Sanity checks.
    switch (stateMachine.getNodeType()) {
    case ANY:
      {
        // This will just be an SQLXML type, only minor checking required.
        return new SqlAttribute(
            "any",
            SqlType.SQLXML,
            false,
            (stateMachine.getMinOccurs() == 0));
      }
    case ELEMENT:
      {
        // Must not be complex, and must not have any attributes.
        if ((stateMachine.getAttributes() != null)
            && !stateMachine.getAttributes().isEmpty()) {
          throw new IllegalArgumentException(
              "Cannot create an SqlAttribute from an element with attributes.");
        }

        break;
      }
    default:
      throw new IllegalArgumentException(
          "Cannot create an SqlAttribute from a " + stateMachine.getNodeType());
    }

    // Now build the attribute from the element.
    return createAttribute(
        getSqlNameFor(stateMachine.getElement().getQName()),
        stateMachine.getElementType(),
        stateMachine.getMinOccurs() == 0);
  }

  private static SqlAttribute createAttribute(XmlSchemaAttrInfo attribute) {
    final XmlSchemaUse attrUse =
        attribute.getAttribute().getUse();

    switch (attrUse) {
    case PROHIBITED:
      throw new IllegalArgumentException(
          "Cannot create an SqlAttribute for a prohibited XML attribute");
    case NONE:
      throw new IllegalArgumentException(
          "Cannot create an SqlAttribute for an "
          + "XML attribute with no known use.");
    default:
      /* falls through */
    }

    return createAttribute(
        getSqlNameFor(attribute.getAttribute().getQName()),
        attribute.getType(),
        attrUse.equals(XmlSchemaUse.OPTIONAL));
  }

  private static SqlAttribute createAttribute(
      String name,
      XmlSchemaTypeInfo xmlTypeInfo,
      boolean isOptional) {

    validateXmlTypeForSqlAttribute(xmlTypeInfo);

    final boolean isArray =
        xmlTypeInfo
          .getType()
          .equals(XmlSchemaTypeInfo.Type.LIST);

    QName xmlTypeToConvert = xmlTypeInfo.getUserRecognizedType();
    if (isArray) {
      xmlTypeToConvert =
          xmlTypeInfo
            .getChildTypes()
            .get(0)
            .getUserRecognizedType();
    }

    final SqlType sqlType = SqlType.getSqlTypeFor(xmlTypeToConvert);

    return new SqlAttribute(
        name,
        sqlType,
        isArray,
        isOptional);
  }
}
