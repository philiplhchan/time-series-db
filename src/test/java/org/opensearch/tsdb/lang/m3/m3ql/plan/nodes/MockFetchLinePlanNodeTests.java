/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.m3ql.plan.nodes;

import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.FunctionNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.TagKeyNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.TagValueNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.ValueNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.visitor.M3PlanVisitor;

import java.util.Map;

/**
 * Unit tests for MockFetchLinePlanNode.
 */
public class MockFetchLinePlanNodeTests extends BasePlanNodeTests {

    public void testMockFetchLinePlanNodeCreation() {
        double value = 10.0;
        Map<String, String> tags = Map.of("name", "test");

        MockFetchLinePlanNode node = new MockFetchLinePlanNode(1, value, tags);

        assertEquals(value, node.getValue(), 0.001);
        assertEquals(tags, node.getTags());
        assertEquals("MOCK_FETCH_LINE(value=10.0, tags={name=test})", node.getExplainName());
    }

    public void testMockFetchLinePlanNodeConstructorValidation() {
        // Null tags should be handled gracefully
        MockFetchLinePlanNode node = new MockFetchLinePlanNode(1, 10.0, null);
        assertNotNull(node.getTags());
        assertTrue(node.getTags().isEmpty());
    }

    public void testMockFetchLinePlanNodeVisitorAccept() {
        MockFetchLinePlanNode node = new MockFetchLinePlanNode(1, 10.0, Map.of("name", "test"));
        TestMockLineVisitor visitor = new TestMockLineVisitor();

        String result = node.accept(visitor);
        assertEquals("visit MockFetchLinePlanNode", result);
    }

    public void testMockFetchLinePlanNodeFactoryMethodWithValueOnly() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("mockFetchLine");
        functionNode.addChildNode(new ValueNode("42.5"));

        MockFetchLinePlanNode planNode = MockFetchLinePlanNode.of(functionNode);

        assertEquals(42.5, planNode.getValue(), 0.001);
        assertTrue(planNode.getTags().isEmpty());
    }

    public void testMockFetchLinePlanNodeFactoryMethodWithTags() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("mockFetchLine");
        functionNode.addChildNode(new ValueNode("7.5"));

        // Add tag: name="test_line"
        TagKeyNode tagKey1 = new TagKeyNode();
        tagKey1.setKeyName("name");
        tagKey1.addChildNode(new TagValueNode("test_line"));
        functionNode.addChildNode(tagKey1);

        // Add tag: dc="us-west"
        TagKeyNode tagKey2 = new TagKeyNode();
        tagKey2.setKeyName("dc");
        tagKey2.addChildNode(new TagValueNode("us-west"));
        functionNode.addChildNode(tagKey2);

        MockFetchLinePlanNode planNode = MockFetchLinePlanNode.of(functionNode);

        assertEquals(7.5, planNode.getValue(), 0.001);
        assertEquals(Map.of("name", "test_line", "dc", "us-west"), planNode.getTags());
    }

    public void testMockFetchLinePlanNodeFactoryMethodWithNegativeValue() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("mockFetchLine");
        functionNode.addChildNode(new ValueNode("-5.5"));

        MockFetchLinePlanNode planNode = MockFetchLinePlanNode.of(functionNode);

        assertEquals(-5.5, planNode.getValue(), 0.001);
    }

    public void testMockFetchLinePlanNodeFactoryMethodValidation() {
        // Null function node
        expectThrows(IllegalArgumentException.class, () -> MockFetchLinePlanNode.of(null));

        // No arguments
        FunctionNode noArgs = new FunctionNode();
        noArgs.setFunctionName("mockFetchLine");
        expectThrows(IllegalArgumentException.class, () -> MockFetchLinePlanNode.of(noArgs));

        // Invalid numeric value
        FunctionNode invalidValue = new FunctionNode();
        invalidValue.setFunctionName("mockFetchLine");
        invalidValue.addChildNode(new ValueNode("not-a-number"));
        expectThrows(IllegalArgumentException.class, () -> MockFetchLinePlanNode.of(invalidValue));

        // Wrong node type for value
        FunctionNode wrongType = new FunctionNode();
        wrongType.setFunctionName("mockFetchLine");
        wrongType.addChildNode(new FunctionNode());
        expectThrows(IllegalArgumentException.class, () -> MockFetchLinePlanNode.of(wrongType));
    }

    public void testMockFetchLinePlanNodeFactoryMethodTagValidation() {
        // Empty key name in tag
        FunctionNode emptyKey = new FunctionNode();
        emptyKey.setFunctionName("mockFetchLine");
        emptyKey.addChildNode(new ValueNode("10.0"));
        TagKeyNode emptyTagKey = new TagKeyNode();
        emptyTagKey.setKeyName("");
        emptyTagKey.addChildNode(new TagValueNode("value"));
        emptyKey.addChildNode(emptyTagKey);
        expectThrows(IllegalArgumentException.class, () -> MockFetchLinePlanNode.of(emptyKey));

        // TagKeyNode with no children
        FunctionNode noChildren = new FunctionNode();
        noChildren.setFunctionName("mockFetchLine");
        noChildren.addChildNode(new ValueNode("10.0"));
        TagKeyNode noChildrenKey = new TagKeyNode();
        noChildrenKey.setKeyName("env");
        noChildren.addChildNode(noChildrenKey);
        expectThrows(IllegalArgumentException.class, () -> MockFetchLinePlanNode.of(noChildren));

        // TagKeyNode with multiple children
        FunctionNode multiChildren = new FunctionNode();
        multiChildren.setFunctionName("mockFetchLine");
        multiChildren.addChildNode(new ValueNode("10.0"));
        TagKeyNode multiKey = new TagKeyNode();
        multiKey.setKeyName("env");
        multiKey.addChildNode(new TagValueNode("prod"));
        multiKey.addChildNode(new TagValueNode("dev"));
        multiChildren.addChildNode(multiKey);
        expectThrows(IllegalArgumentException.class, () -> MockFetchLinePlanNode.of(multiChildren));

        // TagKeyNode with invalid child type
        FunctionNode wrongChild = new FunctionNode();
        wrongChild.setFunctionName("mockFetchLine");
        wrongChild.addChildNode(new ValueNode("10.0"));
        TagKeyNode wrongChildKey = new TagKeyNode();
        wrongChildKey.setKeyName("env");
        wrongChildKey.addChildNode(new FunctionNode());
        wrongChild.addChildNode(wrongChildKey);
        expectThrows(IllegalArgumentException.class, () -> MockFetchLinePlanNode.of(wrongChild));
    }

    public void testMockFetchLinePlanNodeFactoryIntegration() {
        // Test that M3PlanNodeFactory correctly creates MockFetchLinePlanNode
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("mockFetchLine");
        functionNode.addChildNode(new ValueNode("25.0"));

        M3PlanNode result = org.opensearch.tsdb.lang.m3.m3ql.plan.M3PlanNodeFactory.create(functionNode);

        assertNotNull("M3PlanNodeFactory should not return null", result);
        assertTrue("Result should be MockFetchLinePlanNode", result instanceof MockFetchLinePlanNode);
        MockFetchLinePlanNode mockFetchLineNode = (MockFetchLinePlanNode) result;
        assertEquals(25.0, mockFetchLineNode.getValue(), 0.001);
    }

    private static class TestMockLineVisitor extends M3PlanVisitor<String> {
        @Override
        public String visit(MockFetchLinePlanNode planNode) {
            return "visit MockFetchLinePlanNode";
        }

        @Override
        public String process(M3PlanNode planNode) {
            return "process called";
        }
    }
}
