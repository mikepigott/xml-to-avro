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

package mpigott.avro.xml;

import java.util.ArrayList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.ws.commons.schema.XmlSchemaCollection;
import org.w3c.dom.DOMImplementation;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

/**
 * Builds an XML {@link org.w3c.dom.Document}
 * from an XML Schema during a SAX walk.
 *
 * @author  Mike Pigott
 */
final class DomBuilderFromSax extends DefaultHandler {

  /**
   * Creates a new <code>DocumentBuilderFromSax</code>.
   *
   * @throws ParserConfigurationException If unable to create a
   *                                      {@link DocumentBuilder}. 
   */
  public DomBuilderFromSax() throws ParserConfigurationException {
    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
    factory.setNamespaceAware(true);

    docBuilder = factory.newDocumentBuilder();
    elementStack = new ArrayList<Element>();

    document = null;
    content = null;
  }

  /**
   * @see org.xml.sax.helpers.DefaultHandler#startDocument()
   */
  @Override
  public void startDocument() throws SAXException {
    document = docBuilder.newDocument();
    document.setXmlStandalone(true);
  }

  /**
   * @see org.xml.sax.helpers.DefaultHandler#startElement(java.lang.String, java.lang.String, java.lang.String, org.xml.sax.Attributes)
   */
  @Override
  public void startElement(
      String uri,
      String localName,
      String qName,
      Attributes atts) throws SAXException {

    addContentToCurrentElement();

    final Element element =
        document.createElementNS(uri.isEmpty() ? null : uri, localName);

    if ( !elementStack.isEmpty() ) {
      elementStack.get(elementStack.size() - 1).appendChild(element);
    }

    elementStack.add(element);
  }

  /**
   * 
   * @see org.xml.sax.helpers.DefaultHandler#characters(char[], int, int)
   */
  @Override
  public void characters(char[] ch, int start, int length) throws SAXException {
    if (content == null) {
      content = new StringBuilder();
    }
    content.append(ch, start, length);
  }

  /**
   * @see org.xml.sax.helpers.DefaultHandler#endElement(java.lang.String, java.lang.String, java.lang.String)
   */
  @Override
  public void endElement(
      String uri,
      String localName,
      String qName)
      throws SAXException {

    addContentToCurrentElement();

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

  private void addContentToCurrentElement() {
    if ((content == null) || (content.length() == 0)) {
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

  private Document document;
  private StringBuilder content;

  private final ArrayList<Element> elementStack;
  private final DocumentBuilder docBuilder;
}
