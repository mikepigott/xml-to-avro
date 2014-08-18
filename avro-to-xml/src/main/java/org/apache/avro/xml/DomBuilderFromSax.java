/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.avro.xml;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.xml.namespace.QName;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.ws.commons.schema.XmlSchemaCollection;
import org.apache.ws.commons.schema.XmlSchemaElement;
import org.apache.ws.commons.schema.constants.Constants;
import org.w3c.dom.DOMException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

/**
 * Builds an XML {@link org.w3c.dom.Document}
 * from an XML Schema during a SAX walk.
 */
final class DomBuilderFromSax extends DefaultHandler {

  private static final String XSI_NS =
      "http://www.w3.org/2001/XMLSchema-instance";
  private static final String XSI_SCHEMALOC = "schemaLocation";
  private static final String XSI_NIL = "nil";

  private Document document;
  private StringBuilder content;
  private Map<String, String> namespaceToLocationMapping;
  private List<String> newPrefixes;
  private XmlSchemaNamespaceContext nsContext;

  private Map<QName, XmlSchemaElement> elementsByQName;

  private final ArrayList<Element> elementStack;
  private final DocumentBuilder docBuilder;
  private final XmlSchemaCollection schemas;
  private final Set<String> globalNamespaces;

  DomBuilderFromSax() throws ParserConfigurationException {
    this(null);
  }

  /**
   * Creates a new <code>DocumentBuilderFromSax</code>.
   *
   * @throws ParserConfigurationException If unable to create a
   *                                      {@link DocumentBuilder}. 
   */
  DomBuilderFromSax(XmlSchemaCollection xmlSchemaCollection)
      throws ParserConfigurationException {

    if (xmlSchemaCollection == null) {
      throw new IllegalArgumentException(
          "xmlSchemaCollection cannot be null.");
    }

    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
    factory.setNamespaceAware(true);

    docBuilder = factory.newDocumentBuilder();
    elementStack = new ArrayList<Element>();
    newPrefixes = new ArrayList<String>();
    nsContext = new XmlSchemaNamespaceContext();

    document = null;
    content = null;
    namespaceToLocationMapping = null;
    elementsByQName = null;
    schemas = xmlSchemaCollection;

    globalNamespaces = new HashSet<String>();
    globalNamespaces.add("http://www.w3.org/2001/XMLSchema-instance");
  }

  /**
   * @see org.xml.sax.helpers.DefaultHandler#startDocument()
   */
  @Override
  public void startDocument() throws SAXException {
    document = docBuilder.newDocument();
    document.setXmlStandalone(true);
  }

  @Override
  public void startPrefixMapping(String prefix, String uri)
      throws SAXException {

    nsContext.addNamespace(prefix, uri);
    newPrefixes.add(prefix);
  }

  @Override
  public void endPrefixMapping(String prefix) throws SAXException {
    nsContext.removeNamespace(prefix);
  }

  /**
   * @see DefaultHandler#startElement(String, String, String, Attributes)
   */
  @Override
  public void startElement(
      String uri,
      String localName,
      String qName,
      Attributes atts) throws SAXException {

    addContentToCurrentElement(false);

    final Element element =
        document.createElementNS(uri.isEmpty() ? null : uri, qName);

    // Define New Prefixes
    for (String newPrefix : newPrefixes) {
      final String namespace = nsContext.getNamespaceURI(newPrefix);
      if (namespace == null) {
        throw new SAXException("Prefix " + newPrefix + " is not recognized.");
      }
      String qualifiedName = null;
      if ( !newPrefix.isEmpty() ) {
        qualifiedName = Constants.XMLNS_ATTRIBUTE + ':' + newPrefix;
      } else {
        qualifiedName = Constants.XMLNS_ATTRIBUTE;
      }

      try {
        element.setAttributeNS(
            Constants.XMLNS_ATTRIBUTE_NS_URI,
            qualifiedName,
            namespace);
      } catch (DOMException e) {
        throw new IllegalStateException(
            "Cannot add namespace attribute ns=\""
            + Constants.XMLNS_ATTRIBUTE_NS_URI
            + "\", qn=\""
            + qualifiedName
            + "\", value=\""
            + namespace
            + "\" to element \""
            + qName
            + "\".",
            e);
      }
    }
    newPrefixes.clear();

    // Add Attributes
    for (int attrIndex = 0; attrIndex < atts.getLength(); ++attrIndex) {
      String attrUri = atts.getURI(attrIndex);
      if ( attrUri.isEmpty() ) {
        attrUri = null;
      }

      boolean isGlobal = globalNamespaces.contains(attrUri);
      if ((attrUri != null) && !isGlobal) {

        /* TODO: This is not quite correct.  The default namespace does not
         *       apply to attributes - so if an element defines an attribute,
         *       it is considered to have no namespace, and likewise should
         *       not have a prefix.
         *
         *       However, that no-namespace attribute may conflict with an
         *       attribute of the same name in the global namespace.  The only
         *       way to resolve this would be to keep track of where we are in
         *       the schema, and then walk the element's type hierarchy to find
         *       its attributes.  Then we would be able to determine if an
         *       attribute is in the global namespace by calling
         *       XmlSchemaAttribute.isTopLevel().
         *
         *       This cannot be done using XmlSchemaWalker (and by extension,
         *       XmlSchemaStateMachineNodes) because XmlSchemaWalker creates
         *       copies of the existing XmlSchemaAttributes in the schema in
         *       order to properly merge a globally-defined attribute with its
         *       references defined in the element type hierarchies.
         *
         *       The tests can be easily modified to check this behavior; just
         *       rename avro:mapId to avro:id in test_schema.xsd and update its
         *       implementation documents test1_root.xml, test2_children.xml,
         *       and test3_grandchildren.xml.  The attribute name avro:id will
         *       conflict with the id attribute defined in avro:root.
         */
        final QName attrQName =
            new QName(attrUri, atts.getLocalName(attrIndex));

        if (schemas.getAttributeByQName(attrQName) != null) {
          isGlobal = true;
        }
      }

      final String attrValue = atts.getValue(attrIndex);

      if (!isGlobal) {
        element.setAttribute(atts.getLocalName(attrIndex), attrValue);
      } else {
        element.setAttributeNS(attrUri, atts.getQName(attrIndex), attrValue);
      }

    }

    // Update the Parent Element
    if ( !elementStack.isEmpty() ) {
      elementStack.get(elementStack.size() - 1).appendChild(element);
    } else {
      addNamespaceLocationMappings(element);
      document.appendChild(element);
    }

    elementStack.add(element);
  }

  /**
   * 
   * @see DefaultHandler#characters(char[], int, int)
   */
  @Override
  public void characters(char[] ch, int start, int length)
      throws SAXException {

    if (content == null) {
      content = new StringBuilder();
    }
    content.append(ch, start, length);
  }

  /**
   * @see DefaultHandler#endElement(String, String, String)
   */
  @Override
  public void endElement(
      String uri,
      String localName,
      String qName)
      throws SAXException {

    addContentToCurrentElement(true);

    if ( elementStack.isEmpty() ) {
      StringBuilder errMsg = new StringBuilder("Attempted to end element {");
      errMsg.append(uri).append('}').append(localName);
      errMsg.append(", but the stack is empty!");
      throw new IllegalStateException( errMsg.toString() );
    }

    final Element element = elementStack.remove(elementStack.size() - 1);

    final String ns = ( uri.isEmpty() ) ? null : uri;

    final boolean namespacesMatch =
        (((ns == null) && (element.getNamespaceURI() == null))
            || ((ns != null) && ns.equals(element.getNamespaceURI())));

    if (!namespacesMatch
        || !element.getLocalName().equals(localName) ) {
      StringBuilder errMsg = new StringBuilder("Attempted to end element {");
      errMsg.append(ns).append('}').append(localName).append(", but found {");
      errMsg.append( element.getNamespaceURI() ).append('}');
      errMsg.append( element.getLocalName() ).append(" on the stack instead!");
      throw new IllegalStateException( errMsg.toString() );
    }
  }

  /**
   * @see org.xml.sax.helpers.DefaultHandler#endDocument()
   */
  @Override
  public void endDocument() throws SAXException {
    if (!elementStack.isEmpty()) {
      StringBuilder errMsg = new StringBuilder("Ending an XML document with ");
      errMsg.append( elementStack.size() ).append(" elements still open.");

      elementStack.clear();

      throw new IllegalStateException(  errMsg.toString() );
    }
  }

  private void addContentToCurrentElement(boolean isEnd) {
    if ((content == null) || (content.length() == 0)) {
      /* If we reached the end of the element, check if we received any
       * content.  If not, and if the element is nillable, write a nil
       * attribute.
       */
      if (isEnd && !elementStack.isEmpty() && (schemas != null)) {
        final Element currElem  = elementStack.get(elementStack.size() - 1);
        if (currElem.getChildNodes().getLength() == 0) {
          final QName elemQName =
              new QName(currElem.getNamespaceURI(), currElem.getLocalName());

          XmlSchemaElement schemaElem = null;

          if (elementsByQName != null) {
            schemaElem = elementsByQName.get(elemQName);
          }

          if (schemaElem == null) {
            schemaElem = schemas.getElementByQName(elemQName);
          }

          if ((schemaElem != null) && schemaElem.isNillable()) {
            currElem.setAttributeNS(XSI_NS, XSI_NIL, "true");
          }
        }
      }
      return;
    }

    if ( elementStack.isEmpty() ) {
      StringBuilder errMsg = new StringBuilder("Attempted to add content \"");
      errMsg.append( content.toString() ).append("\", but there were no ");
      errMsg.append("elements in the stack!");
      throw new IllegalStateException( errMsg.toString() );
    }

    elementStack
      .get(elementStack.size() - 1)
      .appendChild( document.createTextNode( content.toString() ) );

    content.delete(0, content.length());
  }

  Document getDocument() {
    return document;
  }

  Map<String, String> getNamespaceToLocationMapping() {
    return namespaceToLocationMapping;
  }

  void setNamespaceToLocationMapping(Map<String, String> nsToLocMapping) {
    namespaceToLocationMapping = nsToLocMapping;
  }

  Map<QName, XmlSchemaElement> getElementsByQName() {
    return elementsByQName;
  }

  void setElementsByQName(Map<QName, XmlSchemaElement> elemsByQName) {
    elementsByQName = elemsByQName;
  }

  private void addNamespaceLocationMappings(Element rootElement) {
    if ((namespaceToLocationMapping == null)
        || namespaceToLocationMapping.isEmpty()
        || rootElement.hasAttributeNS(XSI_NS, XSI_SCHEMALOC)) {

      /* There are no namesapces mappings to add,
       * or a namespace mapping already exists.
       */
      return;
    }

    StringBuilder schemaList = new StringBuilder();
    for (Map.Entry<String, String> e : namespaceToLocationMapping.entrySet()) {
      schemaList.append( e.getKey() ).append(' ').append( e.getValue() );
      schemaList.append(' ');
    }
    schemaList.delete(schemaList.length() - 1, schemaList.length());

    rootElement.setAttributeNS(XSI_NS, XSI_SCHEMALOC, schemaList.toString());
  }
}
