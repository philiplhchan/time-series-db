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
 * Unit tests for AliasPlanNode.
 */
public class AliasPlanNodeTests extends BasePlanNodeTests {

    public void testAliasPlanNodeCreation() {
        String alias = "cpu.{{host}}";
        AliasPlanNode node = new AliasPlanNode(1, alias);

        assertEquals(1, node.getId());
        assertEquals(alias, node.getAlias());
        assertEquals("ALIAS(cpu.{{host}})", node.getExplainName());
        assertTrue(node.getChildren().isEmpty());
    }

    public void testAliasPlanNodeWithSimpleAlias() {
        String alias = "simple_alias";
        AliasPlanNode node = new AliasPlanNode(1, alias);

        assertEquals(alias, node.getAlias());
        assertEquals("ALIAS(simple_alias)", node.getExplainName());
    }

    public void testAliasPlanNodeVisitorAccept() {
        AliasPlanNode node = new AliasPlanNode(1, "test_alias");
        TestMockVisitor visitor = new TestMockVisitor();

        String result = node.accept(visitor);
        assertEquals("visit AliasPlanNode", result);
    }

    public void testAliasPlanNodeFactoryMethod() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("alias");
        functionNode.addChildNode(new ValueNode("\"cpu.{{host}}\""));

        AliasPlanNode node = AliasPlanNode.of(functionNode);

        assertEquals("cpu.{{host}}", node.getAlias());
    }

    public void testAliasPlanNodeFactoryMethodStripsQuotes() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("alias");
        functionNode.addChildNode(new ValueNode("\"memory.used\""));

        AliasPlanNode node = AliasPlanNode.of(functionNode);

        assertEquals("memory.used", node.getAlias());
    }

    public void testAliasPlanNodeFactoryMethodWithoutQuotes() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("alias");
        functionNode.addChildNode(new ValueNode("disk.usage"));

        AliasPlanNode node = AliasPlanNode.of(functionNode);

        assertEquals("disk.usage", node.getAlias());
    }

    public void testAliasPlanNodeFactoryMethodWithNoArgument() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("alias");

        AliasPlanNode node = AliasPlanNode.of(functionNode);
        assertEquals("", node.getAlias());
    }

    public void testAliasPlanNodeFactoryMethodThrowsOnMultipleArguments() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("alias");
        functionNode.addChildNode(new ValueNode("arg1"));
        functionNode.addChildNode(new ValueNode("arg2"));

        expectThrows(IllegalArgumentException.class, () -> AliasPlanNode.of(functionNode));
    }

    public void testAliasPlanNodeFactoryMethodThrowsOnNonValueNode() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("alias");

        functionNode.addChildNode(new FunctionNode());

        expectThrows(IllegalArgumentException.class, () -> AliasPlanNode.of(functionNode));
    }

    private static class TestMockVisitor extends M3PlanVisitor<String> {
        @Override
        public String process(M3PlanNode planNode) {
            return "process called";
        }

        @Override
        public String visit(AliasPlanNode planNode) {
            return "visit AliasPlanNode";
        }
    }
}
