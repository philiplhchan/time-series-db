/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.m3ql.plan.nodes;

import org.opensearch.tsdb.lang.m3.common.Utils;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.FunctionNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.M3ASTNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.ValueNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.M3PlannerContext;
import org.opensearch.tsdb.lang.m3.m3ql.plan.visitor.M3PlanVisitor;

import java.util.List;
import java.util.Locale;

/**
 * AliasPlanNode represents a node in the M3QL plan that renames series in a series list.
 * Can add tag values with {{}} syntax.
 */
public class AliasPlanNode extends M3PlanNode {
    private final String alias;

    /**
     * Constructor for AliasPlanNode.
     *
     * @param id node id
     * @param alias the new name for the series (can include {{tag}} interpolation)
     */
    public AliasPlanNode(int id, String alias) {
        super(id);
        this.alias = alias;
    }

    @Override
    public <T> T accept(M3PlanVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public String getExplainName() {
        return String.format(Locale.ROOT, "ALIAS(%s)", alias);
    }

    /**
     * Returns the alias string for this node.
     * @return alias string
     */
    public String getAlias() {
        return alias;
    }

    /**
     * Factory method to create an AliasPlanNode from a FunctionNode.
     * Expects the function node to have exactly one child that is a ValueNode representing the alias.
     *
     * @param functionNode the function node to convert
     * @return AliasPlanNode instance
     * @throws IllegalArgumentException if the function node does not have exactly one ValueNode child
     */
    public static AliasPlanNode of(FunctionNode functionNode) {
        List<M3ASTNode> childNodes = functionNode.getChildren();
        if (childNodes.size() > 1) {
            throw new IllegalArgumentException("Alias function expects no more than one argument");
        } else if (childNodes.isEmpty()) {
            // allow no argument, treat as empty string
            return new AliasPlanNode(M3PlannerContext.generateId(), "");
        }

        M3ASTNode child = childNodes.getFirst();
        if (!(child instanceof ValueNode valueNode)) {
            throw new IllegalArgumentException("Alias function expects a value");
        }

        return new AliasPlanNode(M3PlannerContext.generateId(), Utils.stripDoubleQuotes(valueNode.getValue()));
    }
}
