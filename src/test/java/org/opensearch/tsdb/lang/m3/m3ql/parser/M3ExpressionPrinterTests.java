/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.m3ql.parser;

import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.tsdb.lang.m3.M3TestUtils;
import org.opensearch.tsdb.lang.m3.m3ql.parser.generated.M3QLParser;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;

public class M3ExpressionPrinterTests extends OpenSearchTestCase {

    public void testRoundTrip() {
        String[] queries = {
            "fetch city_id:1 | transformNull | moving 1m sum | sum region | avg",
            "fetch city_name:\"San Francisco\" host:{host1,host2} | sum merchantID | transformNull | moving 1m sum",
            "a = fetch city_id:1 | transformNull; b = fetch city_id:2 | transformNull; a | asPercent(b)" };

        for (String query : queries) {
            try {
                org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.RootNode astRoot = M3QLParser.parse(query, false);
                M3QLExpressionPrinter printer = new M3QLExpressionPrinter();
                String outputExpression = astRoot.accept(printer);
                String expectedQuery = query.replaceAll("\\s+", " ").trim();
                assertEquals("Output m3ql expression does not match", expectedQuery, outputExpression);
            } catch (Exception e) {
                fail("Failed to parse and round trip query: " + query + " with error: " + e.getMessage());
            }
        }
    }

    public void testExecuteAliasForExec() {
        // Test that 'execute' is an alias for 'exec' and produces the same AST
        // Using pipeline context similar to test case 6.m3ql
        String queryWithExec = "fetch name:def | exec(moving 1h avg | sum) | transformNull";
        String queryWithExecute = "fetch name:def | execute(moving 1h avg | sum) | transformNull";
        String queryWithExecuteWhitespace = "fetch name:def | execute   (moving 1h avg | sum) | transformNull";

        try (
            ByteArrayOutputStream baos1 = new ByteArrayOutputStream();
            ByteArrayOutputStream baos2 = new ByteArrayOutputStream();
            ByteArrayOutputStream baos3 = new ByteArrayOutputStream()
        ) {
            PrintStream ps1 = new PrintStream(baos1, true, StandardCharsets.UTF_8);
            PrintStream ps2 = new PrintStream(baos2, true, StandardCharsets.UTF_8);
            PrintStream ps3 = new PrintStream(baos3, true, StandardCharsets.UTF_8);

            M3TestUtils.printAST(M3QLParser.parse(queryWithExec, false), 0, ps1);
            M3TestUtils.printAST(M3QLParser.parse(queryWithExecute, false), 0, ps2);
            M3TestUtils.printAST(M3QLParser.parse(queryWithExecuteWhitespace, false), 0, ps3);

            String astExecStr = baos1.toString(StandardCharsets.UTF_8);
            String astExecuteStr = baos2.toString(StandardCharsets.UTF_8);
            String astExecuteWhitespaceStr = baos3.toString(StandardCharsets.UTF_8);

            assertEquals("exec and execute should produce the same AST", astExecStr, astExecuteStr);
            assertEquals("exec and execute (with whitespace) should produce the same AST", astExecStr, astExecuteWhitespaceStr);
        } catch (Exception e) {
            fail("Failed to parse queries with exec/execute: " + e.getMessage());
        }
    }
}
