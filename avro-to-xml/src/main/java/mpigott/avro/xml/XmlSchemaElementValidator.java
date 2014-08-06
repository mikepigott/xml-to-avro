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

import java.util.HashMap;
import java.util.List;

import javax.xml.namespace.QName;

import org.apache.ws.commons.schema.XmlSchemaAttribute;
import org.apache.ws.commons.schema.XmlSchemaUse;
import org.xml.sax.Attributes;

/**
 * Methods to confirm that an XML element and
 * its attributes all conform to its XML Schema.
 *
 * @author  Mike Pigott
 */
public final class XmlSchemaElementValidator {

  static void validateAttributes(
      XmlSchemaStateMachineNode state,
      Attributes attrs) {

    if ((state == null)
        || (attrs == null)
        || !state
              .getNodeType()
              .equals(XmlSchemaStateMachineNode.Type.ELEMENT)) {
      throw new IllegalArgumentException("Niether state nor attrs can be null, and state must be of an SchemaStateMachineNode.Type.ELEMENT node, not " + ((state == null) ? null : state.getNodeType()));
    }

    final QName elemQName = state.getElement().getQName();

    final List<XmlSchemaStateMachineNode.Attribute> attributes =
        state.getAttributes();

    for (XmlSchemaStateMachineNode.Attribute attribute : attributes) {
      final XmlSchemaAttribute xmlSchemaAttr = attribute.getAttribute();
      final QName attrName = xmlSchemaAttr.getQName();
      final XmlSchemaUse use = xmlSchemaAttr.getUse();

      String value =
          attrs.getValue(attrName.getNamespaceURI(), attrName.getLocalPart());

      if (value == null) {
        // A namespace is not always available.
        value = attrs.getValue("", attrName.getLocalPart());
      }

      // Confirm the attribute is used correctly.
      switch (use) {
      case NONE:
      case OPTIONAL:
        break;
      case PROHIBITED:
        if ((value != null) && !value.isEmpty()) {
          throw new IllegalArgumentException("Attribute " + attrName + " was declared 'prohibited' by " + elemQName + " and cannot have a value.");
        }
        break;
      case REQUIRED:
        if ((value == null) || value.isEmpty()) {
          throw new IllegalArgumentException("Attribute " + attrName + " was declared 'required' by " + elemQName + " and must have a value.");
        }
        break;
      default:
        throw new IllegalArgumentException("Attribute " + attrName + " has an unrecognized usage of " + use + ".");
      }

      /* If the value is null or empty there is no
       * further validation we can perform here.
       */
      if ((value == null) || value.isEmpty()) {
        continue;
      }

      final XmlSchemaTypeInfo attrTypeInfo = attribute.getType();
      final HashMap<XmlSchemaRestriction.Type, List<XmlSchemaRestriction>>
        facets = attrTypeInfo.getFacets();
    }

  }

  static void validateContent(
      XmlSchemaStateMachineNode state,
      String elementContent) {
    
  }
}
