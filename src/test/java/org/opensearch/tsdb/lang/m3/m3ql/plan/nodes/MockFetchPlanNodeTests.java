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

import java.util.List;
import java.util.Map;

/**
 * Unit tests for MockFetchPlanNode.
 */
public class MockFetchPlanNodeTests extends BasePlanNodeTests {

    public void testMockFetchPlanNodeCreation() {
        List<Double> values = List.of(1.0, 2.0, 3.0);
        Map<String, String> tags = Map.of("name", "test");

        MockFetchPlanNode node = new MockFetchPlanNode(1, values, tags);

        assertEquals(values, node.getValues());
        assertEquals(tags, node.getTags());
        assertEquals("MOCK_FETCH(values=[1.0, 2.0, 3.0], tags={name=test})", node.getExplainName());
    }

    public void testMockFetchPlanNodeConstructorValidation() {
        // Null values
        expectThrows(IllegalArgumentException.class, () -> new MockFetchPlanNode(1, null, Map.of()));

        // Empty values
        expectThrows(IllegalArgumentException.class, () -> new MockFetchPlanNode(1, List.of(), Map.of()));

        // Null tags should be handled gracefully
        MockFetchPlanNode node = new MockFetchPlanNode(1, List.of(1.0, 2.0), null);
        assertNotNull(node.getTags());
        assertTrue(node.getTags().isEmpty());
    }

    public void testMockFetchPlanNodeVisitorAccept() {
        MockFetchPlanNode node = new MockFetchPlanNode(1, List.of(1.0, 2.0), Map.of("name", "test"));
        TestMockVisitor visitor = new TestMockVisitor();

        String result = node.accept(visitor);
        assertEquals("visit MockFetchPlanNode", result);
    }

    public void testMockFetchPlanNodeFactoryMethodWithSingleValue() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("mockFetch");
        functionNode.addChildNode(new ValueNode("42.5"));

        MockFetchPlanNode planNode = MockFetchPlanNode.of(functionNode);

        assertEquals(List.of(42.5), planNode.getValues());
        assertTrue(planNode.getTags().isEmpty());
    }

    public void testMockFetchPlanNodeFactoryMethodWithMultipleValues() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("mockFetch");

        // Multiple ValueNodes for comma-separated values (as parser creates them)
        functionNode.addChildNode(new ValueNode("1.0"));
        functionNode.addChildNode(new ValueNode("2.0"));
        functionNode.addChildNode(new ValueNode("3.0"));
        functionNode.addChildNode(new ValueNode("4.0"));

        MockFetchPlanNode planNode = MockFetchPlanNode.of(functionNode);

        assertEquals(List.of(1.0, 2.0, 3.0, 4.0), planNode.getValues());
        assertTrue(planNode.getTags().isEmpty());
    }

    public void testMockFetchPlanNodeFactoryMethodWithTags() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("mockFetch");

        // Add values as multiple ValueNodes
        functionNode.addChildNode(new ValueNode("1.0"));
        functionNode.addChildNode(new ValueNode("2.0"));

        // Add tag: name="test_series"
        TagKeyNode tagKey1 = new TagKeyNode();
        tagKey1.setKeyName("name");
        tagKey1.addChildNode(new TagValueNode("test_series"));
        functionNode.addChildNode(tagKey1);

        // Add tag: dc="us-east"
        TagKeyNode tagKey2 = new TagKeyNode();
        tagKey2.setKeyName("dc");
        tagKey2.addChildNode(new TagValueNode("us-east"));
        functionNode.addChildNode(tagKey2);

        MockFetchPlanNode planNode = MockFetchPlanNode.of(functionNode);

        assertEquals(List.of(1.0, 2.0), planNode.getValues());
        assertEquals(Map.of("name", "test_series", "dc", "us-east"), planNode.getTags());
    }

    public void testMockFetchPlanNodeFactoryMethodValidation() {
        // Null function node
        expectThrows(IllegalArgumentException.class, () -> MockFetchPlanNode.of(null));

        // No arguments
        FunctionNode noArgs = new FunctionNode();
        noArgs.setFunctionName("mockFetch");
        expectThrows(IllegalArgumentException.class, () -> MockFetchPlanNode.of(noArgs));

        // Invalid numeric value in multiple ValueNodes
        FunctionNode invalidMultiValue = new FunctionNode();
        invalidMultiValue.setFunctionName("mockFetch");
        invalidMultiValue.addChildNode(new ValueNode("1.0"));
        invalidMultiValue.addChildNode(new ValueNode("invalid"));
        expectThrows(IllegalArgumentException.class, () -> MockFetchPlanNode.of(invalidMultiValue));

        // Invalid numeric value in ValueNode
        FunctionNode invalidValue = new FunctionNode();
        invalidValue.setFunctionName("mockFetch");
        invalidValue.addChildNode(new ValueNode("not-a-number"));
        expectThrows(IllegalArgumentException.class, () -> MockFetchPlanNode.of(invalidValue));

        // Wrong node type for values
        FunctionNode wrongType = new FunctionNode();
        wrongType.setFunctionName("mockFetch");
        wrongType.addChildNode(new FunctionNode());
        expectThrows(IllegalArgumentException.class, () -> MockFetchPlanNode.of(wrongType));

        // Empty key name
        FunctionNode emptyKey = new FunctionNode();
        emptyKey.setFunctionName("mockFetch");
        emptyKey.addChildNode(new ValueNode("1.0"));
        TagKeyNode tagKey = new TagKeyNode();
        tagKey.setKeyName("");
        tagKey.addChildNode(new TagValueNode("value"));
        emptyKey.addChildNode(tagKey);
        expectThrows(IllegalArgumentException.class, () -> MockFetchPlanNode.of(emptyKey));

        // TagKeyNode with no children
        FunctionNode noChildren = new FunctionNode();
        noChildren.setFunctionName("mockFetch");
        noChildren.addChildNode(new ValueNode("1.0"));
        TagKeyNode emptyTagKey = new TagKeyNode();
        emptyTagKey.setKeyName("mykey");
        noChildren.addChildNode(emptyTagKey);
        expectThrows(IllegalArgumentException.class, () -> MockFetchPlanNode.of(noChildren));

        // TagKeyNode with multiple children
        FunctionNode multiChildren = new FunctionNode();
        multiChildren.setFunctionName("mockFetch");
        multiChildren.addChildNode(new ValueNode("1.0"));
        TagKeyNode multiTagKey = new TagKeyNode();
        multiTagKey.setKeyName("mykey");
        multiTagKey.addChildNode(new TagValueNode("value1"));
        multiTagKey.addChildNode(new TagValueNode("value2"));
        multiChildren.addChildNode(multiTagKey);
        expectThrows(IllegalArgumentException.class, () -> MockFetchPlanNode.of(multiChildren));

        // TagKeyNode with non-TagValueNode child
        FunctionNode wrongChild = new FunctionNode();
        wrongChild.setFunctionName("mockFetch");
        wrongChild.addChildNode(new ValueNode("1.0"));
        TagKeyNode wrongTagKey = new TagKeyNode();
        wrongTagKey.setKeyName("mykey");
        wrongTagKey.addChildNode(new FunctionNode());
        wrongChild.addChildNode(wrongTagKey);
        expectThrows(IllegalArgumentException.class, () -> MockFetchPlanNode.of(wrongChild));
    }

    public void testMockFetchPlanNodeFactoryIntegration() {
        // Test that M3PlanNodeFactory correctly creates MockFetchPlanNode
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("mockFetch");
        functionNode.addChildNode(new ValueNode("5.0"));
        functionNode.addChildNode(new ValueNode("10.0"));

        M3PlanNode result = org.opensearch.tsdb.lang.m3.m3ql.plan.M3PlanNodeFactory.create(functionNode);

        assertNotNull("M3PlanNodeFactory should not return null", result);
        assertTrue("Result should be MockFetchPlanNode", result instanceof MockFetchPlanNode);
        MockFetchPlanNode mockFetchNode = (MockFetchPlanNode) result;
        assertEquals(List.of(5.0, 10.0), mockFetchNode.getValues());
    }

    private static class TestMockVisitor extends M3PlanVisitor<String> {
        @Override
        public String visit(MockFetchPlanNode planNode) {
            return "visit MockFetchPlanNode";
        }

        @Override
        public String process(M3PlanNode planNode) {
            return "process called";
        }
    }
}
