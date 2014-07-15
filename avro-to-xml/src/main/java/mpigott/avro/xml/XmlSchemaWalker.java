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
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.xml.namespace.QName;

import org.apache.ws.commons.schema.XmlSchema;
import org.apache.ws.commons.schema.XmlSchemaAll;
import org.apache.ws.commons.schema.XmlSchemaChoice;
import org.apache.ws.commons.schema.XmlSchemaCollection;
import org.apache.ws.commons.schema.XmlSchemaElement;
import org.apache.ws.commons.schema.XmlSchemaParticle;
import org.apache.ws.commons.schema.XmlSchemaSequence;
import org.apache.ws.commons.schema.XmlSchemaType;

/**
 * Walks an {@link XmlSchema} from a starting {@link XmlSchemaElement},
 * notifying attached visitors as it decends.  Can be configured for
 * either a depth-first or a breadth-first walk.
 *
 * @author  Mike Pigott
 */
final class XmlSchemaWalker {

  /**
   * Initializes the {@link XmlSchemaWalker} with the {@link XmlScheamCollection}
   * to reference when following an {@link XmlSchemaElement}.
   */
  XmlSchemaWalker(XmlSchemaCollection xmlSchemas) {
    if (xmlSchemas == null) {
      throw new IllegalArgumentException("Input XmlSchemaCollection cannot be null.");
    }

    schemas = xmlSchemas;
    visitors = new ArrayList<XmlSchemaVisitor>(1);

    schemasByNamespace = new HashMap<String, XmlSchema>();
    elemsBySubstGroup = new HashMap<QName, List<XmlSchemaElement>>();

    for (XmlSchema schema : schemas.getXmlSchemas()) {
      schemasByNamespace.put(schema.getTargetNamespace(), schema);

      for (XmlSchemaElement elem : schema.getElements().values()) {
        if (elem.getSubstitutionGroup() != null) {
          List<XmlSchemaElement> elems = elemsBySubstGroup.get( elem.getSubstitutionGroup() );
          if (elems == null) {
            elems = new ArrayList<XmlSchemaElement>();
            elemsBySubstGroup.put(elem.getSubstitutionGroup(), elems);
          }
          elems.add(elem);
        }
      }
    }
  }

  XmlSchemaWalker(XmlSchemaCollection xmlSchemas, XmlSchemaVisitor visitor) {
    this(xmlSchemas);
    if (visitor != null) {
      visitors.add(visitor);
    }
  }

  XmlSchemaWalker addVisitor(XmlSchemaVisitor visitor) {
    visitors.add(visitor);
    return this;
  }

  XmlSchemaWalker removeVisitor(XmlSchemaVisitor visitor) {
    if (visitor != null) {
      visitors.remove(visitor);
    }
    return this;
  }

  // Depth-first search.  Visitors will build a stack of XmlSchemaParticle.
  void walk(XmlSchemaElement element) {
    element = getElement(element);

    /* If this element is the root of a
     * substitution group, notify the visitors.
     */
    List<XmlSchemaElement> substitutes = null;
    if ( elemsBySubstGroup.containsKey(element.getQName()) ) {
      substitutes = elemsBySubstGroup.get( element.getQName() );

      for (XmlSchemaVisitor visitor : visitors) {
        visitor.onEnterSubstitutionGroup(element);
      }
    }

    XmlSchemaType schemaType = element.getSchemaType();
    if (schemaType == null) {
      final QName typeQName = element.getSchemaTypeName();
      XmlSchema schema = schemasByNamespace.get( typeQName.getNamespaceURI() );
      schemaType = schema.getTypeByName(typeQName);
    }

    XmlSchemaScope scope =
      new XmlSchemaScope(element, schemasByNamespace, elemsBySubstGroup);

    // 1. Fetch all attributes as a List<XmlSchemaAttribute>.

    // 2. for each visitor, call visitor.startElement(element, type, attributes);
    for (XmlSchemaVisitor visitor : visitors) {
      visitor.onEnterElement(element, null, null);
    }

    // 3. Walk the child groups and elements (if any), either breadth-first or depth-first.

    // 4. On the way back up, call visitor.endElement(element, type, attributes);
    for (XmlSchemaVisitor visitor : visitors) {
      visitor.onExitElement(element, null, null);
    }

    // Now handle substitute elements, if any.
    if (substitutes != null) {
      for (XmlSchemaElement substitute : substitutes) {
        walk(substitute);
      }

      for (XmlSchemaVisitor visitor : visitors) {
        visitor.onExitSubstitutionGroup(element);
      }
    }
  }

  private void walk(XmlSchemaAll allGroup) {
    // For each visitor, call visitor.startAll(allGroup)
    // Walk the child elements.
    // On the way back up, call visitor.endAll(allGroup)
  }

  private void walk(XmlSchemaSequence seq) {
    
  }

  private void walk(XmlSchemaChoice choice) {
    
  }

  /**
   * If the provided {@link XmlSchemaElement} is a reference, track down the
   * original.  Otherwise, just return the provided <code>element</code>.
   *
   * @param element The element to get the definition of.
   * @return The real {@link XmlSchemaElement}.
   */
  private XmlSchemaElement getElement(XmlSchemaElement element) {
    if ( element.isRef() ) {
      if (element.getRef().getTarget() != null) {
        element = element.getRef().getTarget();
      } else {
        final QName elemQName = element.getRefBase().getTargetQName();
        XmlSchema schema = schemasByNamespace.get( elemQName.getNamespaceURI() );
        element = schema.getElementByName(elemQName);
      }
    }
    return element;
  }

  private XmlSchemaCollection schemas;
  private ArrayList<XmlSchemaVisitor> visitors;
  private Map<QName, List<XmlSchemaElement>> elemsBySubstGroup;
  private Map<String, XmlSchema> schemasByNamespace;
}
