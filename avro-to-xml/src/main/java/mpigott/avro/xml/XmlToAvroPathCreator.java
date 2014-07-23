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

import java.util.List;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

/**
 * Performs a SAX-based walk through the XML document, determining the
 * interpretation ("path") that best matches both the XML Schema and the
 * Avro Schema.
 *
 * @author  Mike Pigott
 */
final class XmlToAvroPathCreator extends DefaultHandler {

  /**
   * 
   */
  XmlToAvroPathCreator(SchemaStateMachineNode root) {
    // TODO Auto-generated constructor stub
  }

  @Override
  public void startDocument() throws SAXException { }

  @Override
  public void startElement(
      String uri,
      String localName,
      String qName,
      Attributes atts) throws SAXException {
  }

  @Override
  public void characters(
      char[] ch,
      int start,
      int length)
      throws SAXException {
  }

  @Override
  public void endElement(
      String uri,
      String localName,
      String qName)
      throws SAXException
  {
  }

  @Override
  public void endDocument() throws SAXException {
  }

  private SchemaStateMachineNode rootNode;
  private List<DocumentPathNode> path;
}
