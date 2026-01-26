/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.m3ql.plan.nodes;

import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.FunctionNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.ValueNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.visitor.M3PlanVisitor;

/**
 * Unit tests for TagSubPlanNode.
 */
public class TagSubPlanNodeTests extends BasePlanNodeTests {

    /**
     * Test basic creation with ID, tag name, search pattern, and replacement.
     */
    public void testTagSubPlanNodeCreation() {
        TagSubPlanNode node = new TagSubPlanNode(1, "env", "^prod-(.*)$", "production-$1");

        assertEquals(1, node.getId());
        assertEquals("env", node.getTagName());
        assertEquals("^prod-(.*)$", node.getSearchPattern());
        assertEquals("production-$1", node.getReplacement());
        assertEquals("TAG_SUB(tag=env,search=^prod-(.*)$,replace=production-$1)", node.getExplainName());
        assertTrue(node.getChildren().isEmpty());
    }

    /**
     * Test creation with simple replacement.
     */
    public void testTagSubPlanNodeWithSimpleReplacement() {
        TagSubPlanNode node = new TagSubPlanNode(1, "service", "-v[0-9]+$", "");

        assertEquals("service", node.getTagName());
        assertEquals("-v[0-9]+$", node.getSearchPattern());
        assertEquals("", node.getReplacement());
        assertEquals("TAG_SUB(tag=service,search=-v[0-9]+$,replace=)", node.getExplainName());
    }

    /**
     * Test creation with backreference in replacement.
     */
    public void testTagSubPlanNodeWithBackreference() {
        TagSubPlanNode node = new TagSubPlanNode(1, "host", "^([a-z]+-[a-z]+-[0-9]+)-.*$", "$1");

        assertEquals("host", node.getTagName());
        assertEquals("^([a-z]+-[a-z]+-[0-9]+)-.*$", node.getSearchPattern());
        assertEquals("$1", node.getReplacement());
        assertEquals("TAG_SUB(tag=host,search=^([a-z]+-[a-z]+-[0-9]+)-.*$,replace=$1)", node.getExplainName());
    }

    /**
     * Test getTagName() returns correct value.
     */
    public void testGetTagName() {
        TagSubPlanNode node = new TagSubPlanNode(1, "region", "pattern", "replacement");

        assertEquals("region", node.getTagName());
    }

    /**
     * Test getSearchPattern() returns correct value.
     */
    public void testGetSearchPattern() {
        TagSubPlanNode node = new TagSubPlanNode(1, "tag", "search.*pattern", "replace");

        assertEquals("search.*pattern", node.getSearchPattern());
    }

    /**
     * Test getReplacement() returns correct value.
     */
    public void testGetReplacement() {
        TagSubPlanNode node = new TagSubPlanNode(1, "tag", "pattern", "replacement-$1");

        assertEquals("replacement-$1", node.getReplacement());
    }

    /**
     * Test getExplainName() format with different combinations.
     */
    public void testGetExplainName() {
        // Simple pattern without backreferences
        TagSubPlanNode node1 = new TagSubPlanNode(1, "status", "error", "failed");
        assertEquals("TAG_SUB(tag=status,search=error,replace=failed)", node1.getExplainName());

        // Pattern with backreferences
        TagSubPlanNode node2 = new TagSubPlanNode(2, "env", "^(prod)-(\\w+)$", "$1_$2");
        assertEquals("TAG_SUB(tag=env,search=^(prod)-(\\w+)$,replace=$1_$2)", node2.getExplainName());

        // Empty replacement
        TagSubPlanNode node3 = new TagSubPlanNode(3, "version", "v[0-9]+", "");
        assertEquals("TAG_SUB(tag=version,search=v[0-9]+,replace=)", node3.getExplainName());
    }

    /**
     * Test visitor accept() method with mock visitor.
     */
    public void testTagSubPlanNodeVisitorAccept() {
        TagSubPlanNode node = new TagSubPlanNode(1, "env", "pattern", "replacement");
        TestMockVisitor visitor = new TestMockVisitor();

        String result = node.accept(visitor);
        assertEquals("visit TagSubPlanNode", result);
    }

    /**
     * Test factory method with valid arguments.
     */
    public void testTagSubPlanNodeFactoryMethod() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("tagSub");
        functionNode.addChildNode(new ValueNode("\"env\""));
        functionNode.addChildNode(new ValueNode("\"^prod-(.*)$\""));
        functionNode.addChildNode(new ValueNode("\"production-$1\""));

        TagSubPlanNode node = TagSubPlanNode.of(functionNode);

        assertEquals("env", node.getTagName());
        assertEquals("^prod-(.*)$", node.getSearchPattern());
        assertEquals("production-$1", node.getReplacement());
    }

    /**
     * Test factory method with quoted string values.
     */
    public void testTagSubPlanNodeFactoryMethodWithQuotedValues() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("tagSub");
        functionNode.addChildNode(new ValueNode("\"service\""));
        functionNode.addChildNode(new ValueNode("\"-v[0-9]+$\""));
        functionNode.addChildNode(new ValueNode("\"\""));

        TagSubPlanNode node = TagSubPlanNode.of(functionNode);

        assertEquals("service", node.getTagName());
        assertEquals("-v[0-9]+$", node.getSearchPattern());
        assertEquals("", node.getReplacement());
    }

    /**
     * Test factory method with unquoted values (should work).
     */
    public void testTagSubPlanNodeFactoryMethodWithUnquotedValues() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("tagSub");
        functionNode.addChildNode(new ValueNode("host"));
        functionNode.addChildNode(new ValueNode("pattern"));
        functionNode.addChildNode(new ValueNode("replacement"));

        TagSubPlanNode node = TagSubPlanNode.of(functionNode);

        assertEquals("host", node.getTagName());
        assertEquals("pattern", node.getSearchPattern());
        assertEquals("replacement", node.getReplacement());
    }

    /**
     * Test factory method with less than 3 arguments should throw exception.
     */
    public void testTagSubPlanNodeFactoryMethodWithTooFewArguments() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("tagSub");
        functionNode.addChildNode(new ValueNode("\"env\""));
        functionNode.addChildNode(new ValueNode("\"pattern\""));

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> TagSubPlanNode.of(functionNode));
        assertEquals("TagSub function requires exactly 3 arguments: tag name, search pattern, and replacement", exception.getMessage());
    }

    /**
     * Test factory method with only tag name should throw exception.
     */
    public void testTagSubPlanNodeFactoryMethodWithOnlyTagName() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("tagSub");
        functionNode.addChildNode(new ValueNode("\"env\""));

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> TagSubPlanNode.of(functionNode));
        assertEquals("TagSub function requires exactly 3 arguments: tag name, search pattern, and replacement", exception.getMessage());
    }

    /**
     * Test factory method with no arguments should throw exception.
     */
    public void testTagSubPlanNodeFactoryMethodWithNoArguments() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("tagSub");

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> TagSubPlanNode.of(functionNode));
        assertEquals("TagSub function requires exactly 3 arguments: tag name, search pattern, and replacement", exception.getMessage());
    }

    /**
     * Test factory method with more than 3 arguments should throw exception.
     */
    public void testTagSubPlanNodeFactoryMethodWithTooManyArguments() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("tagSub");
        functionNode.addChildNode(new ValueNode("\"env\""));
        functionNode.addChildNode(new ValueNode("\"pattern\""));
        functionNode.addChildNode(new ValueNode("\"replacement\""));
        functionNode.addChildNode(new ValueNode("\"extra\""));

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> TagSubPlanNode.of(functionNode));
        assertEquals("TagSub function requires exactly 3 arguments: tag name, search pattern, and replacement", exception.getMessage());
    }

    /**
     * Test factory method with non-value node as first argument should throw exception.
     */
    public void testTagSubPlanNodeFactoryMethodWithNonValueNodeAsFirstArg() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("tagSub");
        functionNode.addChildNode(new FunctionNode()); // Invalid: function node instead of value
        functionNode.addChildNode(new ValueNode("\"pattern\""));
        functionNode.addChildNode(new ValueNode("\"replacement\""));

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> TagSubPlanNode.of(functionNode));
        assertEquals("First argument must be a value representing tag name", exception.getMessage());
    }

    /**
     * Test factory method with non-value node as second argument should throw exception.
     */
    public void testTagSubPlanNodeFactoryMethodWithNonValueNodeAsSecondArg() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("tagSub");
        functionNode.addChildNode(new ValueNode("\"env\""));
        functionNode.addChildNode(new FunctionNode()); // Invalid: function node instead of value
        functionNode.addChildNode(new ValueNode("\"replacement\""));

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> TagSubPlanNode.of(functionNode));
        assertEquals("Second argument must be a value representing search pattern", exception.getMessage());
    }

    /**
     * Test factory method with non-value node as third argument should throw exception.
     */
    public void testTagSubPlanNodeFactoryMethodWithNonValueNodeAsThirdArg() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("tagSub");
        functionNode.addChildNode(new ValueNode("\"env\""));
        functionNode.addChildNode(new ValueNode("\"pattern\""));
        functionNode.addChildNode(new FunctionNode()); // Invalid: function node instead of value

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> TagSubPlanNode.of(functionNode));
        assertEquals("Third argument must be a value representing replacement string", exception.getMessage());
    }

    /**
     * Mock visitor for testing visitor pattern.
     */
    private static class TestMockVisitor extends M3PlanVisitor<String> {
        @Override
        public String process(M3PlanNode planNode) {
            return "process " + planNode.getClass().getSimpleName();
        }

        @Override
        public String visit(TagSubPlanNode planNode) {
            return "visit TagSubPlanNode";
        }
    }
}
