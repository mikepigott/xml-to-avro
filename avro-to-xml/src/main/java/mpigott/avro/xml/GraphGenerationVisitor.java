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

import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import org.antlr.stringtemplate.StringTemplate;
import org.antlr.stringtemplate.StringTemplateGroup;
import org.apache.ws.commons.schema.XmlSchemaAll;
import org.apache.ws.commons.schema.XmlSchemaAny;
import org.apache.ws.commons.schema.XmlSchemaAnyAttribute;
import org.apache.ws.commons.schema.XmlSchemaAttribute;
import org.apache.ws.commons.schema.XmlSchemaChoice;
import org.apache.ws.commons.schema.XmlSchemaElement;
import org.apache.ws.commons.schema.XmlSchemaParticle;
import org.apache.ws.commons.schema.XmlSchemaSequence;

/**
 * Generates a graph in the GraphVis format
 * based on the XML Schema's structure.
 *
 * @author  Mike Pigott
 */
public class GraphGenerationVisitor implements XmlSchemaVisitor {

  private static class StackEntry {
    StackEntry(XmlSchemaParticle particle, String nodeName) {
      this.particle = particle;
      this.nodeName = nodeName;
    }

    final XmlSchemaParticle particle;
    final String nodeName;
  }

  /**
   * Constructs a new visitor.
   */
  public GraphGenerationVisitor() throws IOException {
    this("C:\\Users\\Mike Pigott\\Google Drive\\workspace\\edgar_xbrl\\src\\main\\resources\\DOT.stg");
  }

  public GraphGenerationVisitor(String templLocation) throws IOException {
    FileReader fr = null;
    try {
      fr = new FileReader(templLocation);
      templates = new StringTemplateGroup(fr);
    } finally {
      try {
        if (fr != null) {
          fr.close();
        }
      } catch (IOException ioe) {
        ioe.printStackTrace();
      }
    }

    stack = new ArrayList<StackEntry>();
    counter = 0;
    nodes = new ArrayList<StringTemplate>();
    edges = new ArrayList<StringTemplate>();
  }

  /**
   * @see mpigott.avro.xml.XmlSchemaVisitor#onEnterElement(org.apache.ws.commons.schema.XmlSchemaElement, mpigott.avro.xml.XmlSchemaTypeInfo)
   */
  @Override
  public void onEnterElement(
      XmlSchemaElement element,
      XmlSchemaTypeInfo typeInfo,
      boolean previouslyVisited) {

    StringBuilder name = new StringBuilder( element.getQName().toString() );
    if (typeInfo != null) {
      name.append(": ").append( typeInfo.getType() ).append(" ").append( typeInfo.getBaseType() );
    }

    String elemNodeName = getElemNodeName(counter++);

    if (!stack.isEmpty()) {
      String parentNodeName = stack.get(stack.size() - 1).nodeName;
      edges.add( getEdgeSt(parentNodeName, elemNodeName) );
    }
    nodes.add( getNodeSt(elemNodeName, name.toString()) );

    stack.add(new StackEntry(element, elemNodeName));
  }

  /**
   * @see mpigott.avro.xml.XmlSchemaVisitor#onExitElement(org.apache.ws.commons.schema.XmlSchemaElement, mpigott.avro.xml.XmlSchemaTypeInfo)
   */
  @Override
  public void onExitElement(XmlSchemaElement element, XmlSchemaTypeInfo typeInfo, boolean previouslyVisited) {
    getTopEntry(element, true);
  }

  /**
   * @see mpigott.avro.xml.XmlSchemaVisitor#onVisitAttribute(org.apache.ws.commons.schema.XmlSchemaElement, org.apache.ws.commons.schema.XmlSchemaAttribute, mpigott.avro.xml.XmlSchemaTypeInfo)
   */
  @Override
  public void onVisitAttribute(XmlSchemaElement element,
      XmlSchemaAttribute attribute, XmlSchemaTypeInfo attributeType) {

    StackEntry top = getTopEntry(element, false);

    StringBuilder name = new StringBuilder( attribute.getQName().toString() );
    name.append(": ").append( attributeType.getType() );
    name.append(" ").append( attributeType.getBaseType() );

    String attrNodeName = getAttrNodeName(counter++);

    nodes.add( getNodeSt(attrNodeName, name.toString()) );
    edges.add( getEdgeSt(top.nodeName, attrNodeName) );
  }

  /**
   * @see mpigott.avro.xml.XmlSchemaVisitor#onEnterSubstitutionGroup(org.apache.ws.commons.schema.XmlSchemaElement)
   */
  @Override
  public void onEnterSubstitutionGroup(XmlSchemaElement base) {
    StackEntry entry = null;
    if (!stack.isEmpty()) {
      entry = getTopEntry(null, false);
    }

    String groupName = getGroupNodeName(counter++);

    if (entry != null) {
      edges.add( getEdgeSt(entry.nodeName, groupName) );
    }

    nodes.add( getNodeSt(groupName, "Substitution Group for " + base.getQName()) );

    stack.add( new StackEntry(base, groupName) );
  }

  /**
   * @see mpigott.avro.xml.XmlSchemaVisitor#onExitSubstitutionGroup(org.apache.ws.commons.schema.XmlSchemaElement)
   */
  @Override
  public void onExitSubstitutionGroup(XmlSchemaElement base) {
    getTopEntry(base, true);
  }

  /**
   * @see mpigott.avro.xml.XmlSchemaVisitor#onEnterAllGroup(org.apache.ws.commons.schema.XmlSchemaAll)
   */
  @Override
  public void onEnterAllGroup(XmlSchemaAll all) {
    StackEntry topEntry = getTopEntry(null, false);
    String allName = getGroupNodeName(counter++);

    edges.add( getEdgeSt(topEntry.nodeName, allName) );
    nodes.add( getNodeSt(allName, "All"));

    stack.add( new StackEntry(all, allName) );
  }

  /**
   * @see mpigott.avro.xml.XmlSchemaVisitor#onExitAllGroup(org.apache.ws.commons.schema.XmlSchemaAll)
   */
  @Override
  public void onExitAllGroup(XmlSchemaAll all) {
    getTopEntry(all, true);
  }

  /**
   * @see mpigott.avro.xml.XmlSchemaVisitor#onEnterChoiceGroup(org.apache.ws.commons.schema.XmlSchemaChoice)
   */
  @Override
  public void onEnterChoiceGroup(XmlSchemaChoice choice) {
    StackEntry topEntry = getTopEntry(null, false);
    String name = getGroupNodeName(counter++);
    edges.add( getEdgeSt(topEntry.nodeName, name) );
    nodes.add( getNodeSt(name, "Choice") );
    stack.add( new StackEntry(choice, name) );
  }

  /**
   * @see mpigott.avro.xml.XmlSchemaVisitor#onExitChoiceGroup(org.apache.ws.commons.schema.XmlSchemaChoice)
   */
  @Override
  public void onExitChoiceGroup(XmlSchemaChoice choice) {
    getTopEntry(choice, true);
  }

  /**
   * @see mpigott.avro.xml.XmlSchemaVisitor#onEnterSequenceGroup(org.apache.ws.commons.schema.XmlSchemaSequence)
   */
  @Override
  public void onEnterSequenceGroup(XmlSchemaSequence seq) {
    StackEntry topEntry = getTopEntry(null, false);
    String name = getGroupNodeName(counter++);
    edges.add( getEdgeSt(topEntry.nodeName, name) );
    nodes.add( getNodeSt(name, "Sequence") );
    stack.add( new StackEntry(seq, name) );
  }

  /**
   * @see mpigott.avro.xml.XmlSchemaVisitor#onExitSequenceGroup(org.apache.ws.commons.schema.XmlSchemaSequence)
   */
  @Override
  public void onExitSequenceGroup(XmlSchemaSequence seq) {
    getTopEntry(seq, true);
  }

  @Override
  public void onVisitAny(XmlSchemaAny any) {
    StackEntry topEntry = getTopEntry(null, false);
    String anyNodeName = getElemNodeName(counter++);

    StringBuilder name = new StringBuilder("Any [Namespaces: {");
    name.append( any.getNamespace() ).append("}, Process Content: \'");
    name.append( any.getProcessContent().name() ).append("\']");

    edges.add( getEdgeSt(topEntry.nodeName, anyNodeName) );
    nodes.add( getNodeSt(anyNodeName, name.toString()) );
  }

  @Override
  public void onVisitAnyAttribute(
      XmlSchemaElement element,
      XmlSchemaAnyAttribute anyAttr) {

    StackEntry topEntry = getTopEntry(element, false);
    String anyAttrName = getAttrNodeName(counter++);

    StringBuilder name = new StringBuilder("Any Attribute [Namespaces: {");
    name.append( anyAttr.getNamespace() ).append("}, Process Content: \'");
    name.append( anyAttr.getProcessContent().name() ).append("\']");

    edges.add( getEdgeSt(topEntry.nodeName, anyAttrName) );
    nodes.add( getNodeSt(anyAttrName, name.toString()) );
  }

  private StackEntry getTopEntry(XmlSchemaParticle expected, boolean remove) {
    if (stack.isEmpty()) {
      throw new IllegalStateException("Attempted to pop an " + expected.getClass().getName() + " off of an empty stack.");
    }

    StackEntry top = null;

    if (remove) {
      top = stack.remove(stack.size() - 1);
    } else {
      top = stack.get(stack.size() - 1);
    }

    if ((expected != null) && (top.particle != expected)) {
      throw new IllegalStateException("Attempted to pop a " + expected.getClass().getName() + " when the stack contained a " + top.particle.getClass().getName() + " (" + top.nodeName + ")");
    }


    return top;
  }

  public boolean clear() {
    final boolean wasEmpty = stack.isEmpty();
    stack.clear();
    nodes.clear();
    edges.clear();
    return wasEmpty;
  }

  public String toString() {
    StringTemplate fileSt = templates.getInstanceOf("file");
    fileSt.setAttribute("gname", "xml_schema");
    fileSt.setAttribute("nodes", nodes);
    fileSt.setAttribute("edges", edges);
    return fileSt.toString();
  }

  private StringTemplate getEdgeSt(String from, String to) {
    StringTemplate edgeSt = templates.getInstanceOf("edge");
    edgeSt.setAttribute("from", from);
    edgeSt.setAttribute("to", to);
    return edgeSt;
  }

  private StringTemplate getNodeSt(String name, String text) {
    StringTemplate tmpl = templates.getInstanceOf("node");
    tmpl.setAttribute("name", name);
    tmpl.setAttribute("text", text.replace('\"', '\''));
    return tmpl;
  }

  private String getElemNodeName(int num) {
    return "elem" + num;
  }

  private String getAttrNodeName(int num) {
    return "attr" + num;
  }

  private String getGroupNodeName(int num) {
    return "group" + num;
  }

  private StringTemplateGroup templates;
  private ArrayList<StackEntry> stack;
  private ArrayList<StringTemplate> nodes;
  private ArrayList<StringTemplate> edges;
  private int counter;
}
