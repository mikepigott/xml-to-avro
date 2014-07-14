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
import java.util.List;
import java.util.Map;

import javax.xml.namespace.QName;

import org.apache.ws.commons.schema.XmlSchema;
import org.apache.ws.commons.schema.XmlSchemaCollection;
import org.apache.ws.commons.schema.XmlSchemaElement;

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
        visitors = null;

        elemsBySubstGroup = new HashMap<QName, List<XmlSchemaElement>>();
        for (XmlSchema schema : schemas.getXmlSchemas()) {
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
            visitors = new ArrayList<XmlSchemaVisitor>(1);
            visitors.add(visitor);
        }
    }

    XmlSchemaWalker addVisitor(XmlSchemaVisitor visitor) {
        if (visitors == null) {
            visitors = new ArrayList<XmlSchemaVisitor>(1);
        }
        visitors.add(visitor);
        return this;
    }

    void walk(XmlSchemaElement element, boolean depthFirstWalk) {
        XmlSchemaScope scope =
            new XmlSchemaScope(element, schemasByNamespace, elemsBySubstGroup);

        // 1. Fetch all attributes as a List<XmlSchemaAttribute>.
        // 2. for each visitor, call visitor.startElement(element, attributes);
        // 3. Walk the child groups and elements, either breadth-first or depth-first.
    }

    private XmlSchemaCollection schemas;
    private ArrayList<XmlSchemaVisitor> visitors;
    private Map<QName, List<XmlSchemaElement>> elemsBySubstGroup;
    private Map<String, XmlSchema> schemasByNamespace;
}
