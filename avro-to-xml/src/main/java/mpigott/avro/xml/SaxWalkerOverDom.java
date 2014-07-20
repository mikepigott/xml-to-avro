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
import java.util.List;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.Text;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

/**
 * Walks over a {@link Document} in a SAX style,
 * notifying listeners with SAX events.
 *
 * <p>
 * Because the document has already been processed, only the following
 * methods in the {@link ContentHandler} will be called:
 * 
 * <ul>
 *   <li>{@link ContentHandler#startDocument()}</li>
 *   <li>{@link ContentHandler#startElement(String, String, String, org.xml.sax.Attributes)}</li>
 *   <li>{@link ContentHandler#characters(char[], int, int)}</li>
 *   <li>{@link ContentHandler#endElement(String, String, String)}</li>
 *   <li>{@link ContentHandler#endDocument()}</li>
 * </ul>
 * </p>
 *
 * @author  Mike Pigott
 */
final class SaxWalkerOverDom {

  private static class DomAttrsAsSax implements org.xml.sax.Attributes {

    DomAttrsAsSax(NamedNodeMap domAttrs) throws SAXException {
      attrMap = domAttrs;
    }

    @Override
    public int getLength() {
      if (attrMap == null) {
        return 0;
      } else {
        return attrMap.getLength();
      }
    }

    @Override
    public String getURI(int index) {
      if ((attrMap == null) || (attrMap.getLength() <= index)) {
        return null;
      } else {
        return attrMap.item(index).getNamespaceURI();
      }
    }

    @Override
    public String getLocalName(int index) {
      if ((attrMap == null) || (attrMap.getLength() <= index)) {
        return null;
      } else {
        return attrMap.item(index).getLocalName();
      }
    }

    @Override
    public String getQName(int index) {
      if ((attrMap == null) || (attrMap.getLength() <= index)) {
        return null;
      } else {
        return attrMap.item(index).getNodeName();
      }
    }

    @Override
    public String getType(int index) {
      if ((attrMap == null) || (attrMap.getLength() <= index)) {
        return null;
      } else {
        return "CDATA"; // We do not know the type information.
      }
    }

    @Override
    public String getValue(int index) {
      if ((attrMap == null) || (attrMap.getLength() <= index)) {
        return null;
      } else {
        return attrMap.item(index).getNodeValue();
      }
    }

    @Override
    public int getIndex(String uri, String localName) {
      if ((attrMap == null) || (uri == null) || (localName == null)) {
        return -1;
      }

      for (int index = 0; index < attrMap.getLength(); ++index) {
        if ( uri.equals( attrMap.item(index).getNamespaceURI() )
            && localName.equals( attrMap.item(index).getLocalName() )) {
          return index;
        }
      }

      return -1;
    }

    @Override
    public int getIndex(String qName) {
      if ((attrMap == null) || (qName == null)) {
        return -1;
      }

      for (int index = 0; index < attrMap.getLength(); ++index) {
        if ( qName.equals( attrMap.item(index).getNodeName() ) ) {
          return index;
        }
      }

      return -1;
    }

    @Override
    public String getType(String uri, String localName) {
      if ((attrMap == null) || (uri == null) || (localName == null)) {
        return null;
      } else {
        final Node node = attrMap.getNamedItemNS(uri, localName);
        return (node == null) ? null : "CDATA";
      }
    }

    @Override
    public String getType(String qName) {
      if ((attrMap == null) || (qName == null)) {
        return null;
      } else {
        final Node node = attrMap.getNamedItem(qName);
        return (node == null) ? null : "CDATA";
      }
    }

    @Override
    public String getValue(String uri, String localName) {
      if ((attrMap == null) || (uri == null) || (localName == null)) {
        return null;
      } else {
        final Node node = attrMap.getNamedItemNS(uri, localName);
        return (node == null) ? null : node.getNodeValue();
      }
    }

    @Override
    public String getValue(String qName) {
      if ((attrMap == null) || (qName == null)) {
        return null;
      } else {
        final Node node = attrMap.getNamedItem(qName);
        return (node == null) ? null : node.getNodeValue();
      }
    }

    private final NamedNodeMap attrMap;
  }

  /**
   * Constructs a new <code>SaxWalkerOverDom</code>.
   */
  public SaxWalkerOverDom() {
    listeners = null;
  }

  /**
   * Constructs a new <code>SaxWalkerOverDom</code> with
   * the provided {@link ContentHandler} to send SAX events.
   *
   * @param contentHandler The content handler to send events to.
   */
  public SaxWalkerOverDom(ContentHandler contentHandler) {
    listeners = new ArrayList<ContentHandler>(1);
    listeners.add(contentHandler);
  }

  /**
   * Constructs a new <code>SaxWalkerOverDom</code>, taking ownership
   * of the list of {@link ContentHandler}s to send events to.
   *
   * @param contentHandlers The list of content handlers to send events to.
   */
  public SaxWalkerOverDom(List<ContentHandler> contentHandlers) {
    listeners = contentHandlers;
  }

  /**
   * Adds the provided {@link ContentHandler} to the list of content
   * handlers to send events to.  If this content handler was already
   * added, it will be sent events twice (or more often).
   *
   * @param contentHandler The content handler to send events to.
   */
  public void addContentHandler(ContentHandler contentHandler) {
    if (listeners == null) {
      listeners = new ArrayList<ContentHandler>(1);
    }
    listeners.add(contentHandler);
  }

  /**
   * Removes the first instance of the provided {@link ContentHandler}
   * from the set of handlers to send events to.  If the content handler
   * was added more than once, it will continue to receive events.
   *
   * @param contentHandler The content handler to stop sending events to.
   * @return <code>true</code> if it was found, <code>false</code> if not.
   */
  public boolean removeContentHandler(ContentHandler contentHandler) {
    if (listeners != null) {
      return listeners.remove(contentHandler);
    }
    return false;
  }

  /**
   * Walks the provided {@link Document}, sending events to all of the
   * {@link ContentHandler}s as it traverses.  If there are no content
   * handlers, this method is a no-op.
   *
   * @param document The {@link Document} to traverse.
   * @param systemId The system ID of this {@link Document}.
   * @throws SAXException if an exception occurs when notifying the handlers.
   */
  public void walk(Document document) throws SAXException {
    if ((listeners == null) || listeners.isEmpty()) {
      return;
    }

    for (ContentHandler listener : listeners) {
      listener.startDocument();
    }

    walk( document.getDocumentElement() );

    for (ContentHandler listener : listeners) {
      listener.endDocument();
    }
  }

  private void walk(Element element) throws SAXException {
    DomAttrsAsSax attrs = new DomAttrsAsSax( element.getAttributes() );

    for (ContentHandler listener : listeners) {
      listener.startElement(
          convertNullToEmptyString(element.getNamespaceURI()),
          convertNullToEmptyString(element.getLocalName()),
          convertNullToEmptyString(element.getNodeName()),
          attrs);
    }

    NodeList children = element.getChildNodes();

    for (int childIndex = 0; childIndex < children.getLength(); ++childIndex) {
      Node node = children.item(childIndex);
      if (node instanceof Element) {
        walk((Element) node);
      } else if (node instanceof Text) {
        walk((Text) node);
      } else if (node instanceof org.w3c.dom.Comment) {
        // Ignored.
      } else {
        throw new SAXException("Unrecognized child of " + element.getTagName() + " of type " + node.getClass().getName());
      }
    }

    for (ContentHandler listener : listeners) {
      listener.endElement(
          convertNullToEmptyString(element.getNamespaceURI()),
          convertNullToEmptyString(element.getLocalName()),
          convertNullToEmptyString(element.getNodeName()));
    }
  }

  private void walk(Text text) throws SAXException {
    /* TODO: getData() may throw a org.w3c.dom.DOMException if the actual text
     * data is too large to fit into a single DOMString (the DOM impl's
     * internal storage of text data).  If that's the case, substringData()
     * must be called to retrieve the data in pieces.
     *
     * The documentation does not supply information on the maximum DOMString
     * size; it appears to require trial & error.
     */
    if (text.getLength() > 0) {
      char[] data = text.getData().toCharArray();
      for (ContentHandler listener : listeners) {
        listener.characters(data, 0, data.length);
      }
    }
  }

  private static String convertNullToEmptyString(String input) {
    if (input == null) {
      return "";
    }
    return input;
  }

  private List<ContentHandler> listeners;
}
