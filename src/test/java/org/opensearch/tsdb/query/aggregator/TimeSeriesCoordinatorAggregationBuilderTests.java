/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.aggregator;

import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.search.aggregations.pipeline.PipelineAggregator;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.tsdb.lang.m3.stage.AsPercentStage;
import org.opensearch.tsdb.lang.m3.stage.ScaleStage;
import org.opensearch.tsdb.lang.m3.stage.SumStage;
import org.opensearch.tsdb.query.stage.PipelineStage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Unit tests for TimeSeriesCoordinatorAggregationBuilder.
 */
public class TimeSeriesCoordinatorAggregationBuilderTests extends OpenSearchTestCase {

    public void testConstructorBasic() {
        // Arrange
        List<PipelineStage> stages = List.of(new ScaleStage(2.0));
        Map<String, String> references = Map.of("input", "source_agg");
        String inputReference = "input";

        // Act
        TimeSeriesCoordinatorAggregationBuilder builder = new TimeSeriesCoordinatorAggregationBuilder(
            "test_coordinator",
            stages,
            new LinkedHashMap<>(), // No macros
            references,
            inputReference
        );

        // Assert
        assertEquals("test_coordinator", builder.getName());
        assertEquals("coordinator_pipeline", builder.getType());
        assertEquals(TimeSeriesCoordinatorAggregationBuilder.NAME, builder.getWriteableName());
    }

    public void testConstructorWithMacros() {
        // Arrange
        List<PipelineStage> stages = List.of(new ScaleStage(2.0));
        Map<String, String> references = Map.of("a", "agg_a", "b", "agg_b");
        String inputReference = "a";
        LinkedHashMap<String, TimeSeriesCoordinatorAggregator.MacroDefinition> macros = new LinkedHashMap<>();
        macros.put("macro1", new TimeSeriesCoordinatorAggregator.MacroDefinition("macro1", List.of(new ScaleStage(3.0)), "a"));

        // Act
        TimeSeriesCoordinatorAggregationBuilder builder = new TimeSeriesCoordinatorAggregationBuilder(
            "test_with_macros",
            stages,
            macros,
            references,
            inputReference
        );

        // Assert
        assertEquals("test_with_macros", builder.getName());
        assertEquals("coordinator_pipeline", builder.getType());
    }

    public void testSetMetadata() {
        // Arrange
        TimeSeriesCoordinatorAggregationBuilder builder = new TimeSeriesCoordinatorAggregationBuilder(
            "metadata_test",
            List.of(new ScaleStage(1.0)),
            new LinkedHashMap<>(), // No macros
            Map.of("a", "agg_a"),
            "a"
        );
        Map<String, Object> metadata = Map.of("key1", "value1", "key2", 42);

        // Act
        TimeSeriesCoordinatorAggregationBuilder result = builder.setMetadata(metadata);

        // Assert
        assertSame("setMetadata should return same instance", builder, result);
    }

    // ========== Additional Enhanced Tests ==========

    /**
     * Test XContent generation with stages and references.
     */
    public void testXContentGeneration() throws IOException {
        // Arrange
        List<PipelineStage> stages = List.of(new ScaleStage(2.0), new AsPercentStage("b"));
        Map<String, String> references = Map.of("a", "unfold_a", "b", "unfold_b");
        String inputReference = "a";

        TimeSeriesCoordinatorAggregationBuilder builder = new TimeSeriesCoordinatorAggregationBuilder(
            "test_xcontent",
            stages,
            new LinkedHashMap<>(), // No macros
            references,
            inputReference
        );

        // Act
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder();
        xContentBuilder.startObject();
        builder.internalXContent(xContentBuilder, null);
        xContentBuilder.endObject();

        // Assert
        String jsonString = xContentBuilder.toString();
        assertNotNull(jsonString);
        assertTrue(jsonString.contains("stages"));
        assertTrue(jsonString.contains("references"));
        assertTrue(jsonString.contains("inputReference"));
        assertTrue(jsonString.contains("unfold_a"));
        assertTrue(jsonString.contains("unfold_b"));
    }

    /**
     * Test XContent generation with null input reference.
     */
    public void testXContentGenerationNullInputReference() throws IOException {
        // Arrange
        List<PipelineStage> stages = List.of(new ScaleStage(2.0));
        Map<String, String> references = Map.of("a", "unfold_a");

        TimeSeriesCoordinatorAggregationBuilder builder = new TimeSeriesCoordinatorAggregationBuilder(
            "test_xcontent_null",
            stages,
            new LinkedHashMap<>(), // No macros
            references,
            null // null input reference
        );

        // Act
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder();
        xContentBuilder.startObject();
        builder.internalXContent(xContentBuilder, null);
        xContentBuilder.endObject();

        // Assert
        String jsonString = xContentBuilder.toString();
        assertNotNull(jsonString);
        assertTrue(jsonString.contains("stages"));
        assertTrue(jsonString.contains("references"));
        // Should not contain inputReference when null
        assertFalse(jsonString.contains("inputReference"));
    }

    /**
     * Test validation with null stages - constructor should allow it (validation happens in createInternal).
     */
    public void testValidationNullStagesBasic() throws Exception {
        // Arrange & Act - Constructor should handle null stages gracefully
        TimeSeriesCoordinatorAggregationBuilder builder = new TimeSeriesCoordinatorAggregationBuilder(
            "test_validation_null_stages",
            null, // null stages
            new LinkedHashMap<>(), // No macros
            Map.of("a", "unfold_a"),
            "a"
        );

        // Assert - Constructor completes without throwing
        assertEquals("test_validation_null_stages", builder.getName());
    }

    /**
     * Test validation with empty stages - constructor should allow it (validation happens in createInternal).
     */
    public void testValidationEmptyStagesBasic() throws Exception {
        // Arrange & Act - Constructor should handle empty stages gracefully
        TimeSeriesCoordinatorAggregationBuilder builder = new TimeSeriesCoordinatorAggregationBuilder(
            "test_validation_empty_stages",
            new ArrayList<>(), // empty stages
            new LinkedHashMap<>(), // No macros
            Map.of("a", "unfold_a"),
            "a"
        );

        // Assert - Constructor completes without throwing
        assertEquals("test_validation_empty_stages", builder.getName());
    }

    /**
     * Test validation with null references - constructor should allow it (validation happens in createInternal).
     */
    public void testValidationNullReferencesBasic() throws Exception {
        // Arrange & Act - Constructor should handle null references gracefully
        TimeSeriesCoordinatorAggregationBuilder builder = new TimeSeriesCoordinatorAggregationBuilder(
            "test_validation_null_refs",
            List.of(new ScaleStage(2.0)),
            new LinkedHashMap<>(), // No macros
            null, // null references
            "a"
        );

        // Assert - Constructor completes without throwing
        assertEquals("test_validation_null_refs", builder.getName());
    }

    /**
     * Test createInternal method.
     */
    public void testCreateInternal() throws Exception {
        // Arrange
        List<PipelineStage> stages = List.of(new ScaleStage(2.0));
        Map<String, String> references = Map.of("a", "unfold_a");
        String inputReference = "a";

        TimeSeriesCoordinatorAggregationBuilder builder = new TimeSeriesCoordinatorAggregationBuilder(
            "test_create_internal",
            stages,
            new LinkedHashMap<>(), // No macros
            references,
            inputReference
        );

        Map<String, Object> metadata = Map.of("key", "value");

        // Act
        PipelineAggregator aggregator = builder.createInternal(metadata);

        // Assert
        assertNotNull(aggregator);
        assertTrue(aggregator instanceof TimeSeriesCoordinatorAggregator);
        assertEquals("test_create_internal", aggregator.name());
    }

    /**
     * Test with null macro definitions in constructor.
     */
    public void testConstructorWithNullMacros() throws Exception {
        // Arrange
        List<PipelineStage> stages = List.of(new ScaleStage(2.0));
        Map<String, String> references = Map.of("a", "unfold_a");
        String inputReference = "a";

        // Act
        TimeSeriesCoordinatorAggregationBuilder builder = new TimeSeriesCoordinatorAggregationBuilder(
            "test_null_macros",
            stages,
            null, // null macros
            references,
            inputReference
        );

        // Assert
        assertEquals("test_null_macros", builder.getName());
        assertEquals("coordinator_pipeline", builder.getType());
    }

    /**
     * Test XContent generation with empty collections.
     */
    public void testXContentGenerationEmpty() throws IOException {
        // Arrange
        TimeSeriesCoordinatorAggregationBuilder builder = new TimeSeriesCoordinatorAggregationBuilder(
            "test_xcontent_empty",
            new ArrayList<>(),
            new LinkedHashMap<>(), // No macros
            new LinkedHashMap<>(),
            null
        );

        // Act
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder();
        xContentBuilder.startObject();
        builder.internalXContent(xContentBuilder, null);
        xContentBuilder.endObject();

        // Assert
        String jsonString = xContentBuilder.toString();
        assertNotNull(jsonString);
        assertTrue(jsonString.contains("stages"));
        assertTrue(jsonString.contains("references"));
        // Should contain empty arrays/objects
        assertTrue(jsonString.contains("[]"));
        assertTrue(jsonString.contains("{}"));
    }

    // ========== XContent Parser Tests ==========

    /**
     * Test parsing basic coordinator configuration
     */
    public void testParseBasicConfiguration() throws Exception {
        String json = """
            {
              "references": {
                "a": "agg1",
                "b": "agg2"
              },
              "inputReference": "a"
            }
            """;

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            parser.nextToken(); // Move to START_OBJECT

            TimeSeriesCoordinatorAggregationBuilder result = TimeSeriesCoordinatorAggregationBuilder.parse("coord_agg", parser);

            assertEquals("coord_agg", result.getName());
            assertTrue("Should have no stages by default", result.getStages().isEmpty());
            assertEquals("Should have inputReference", "a", result.getInputReference());
            assertEquals("Should have 2 references", 2, result.getReferences().size());
            assertEquals("Reference 'a' should point to 'agg1'", "agg1", result.getReferences().get("a"));
            assertEquals("Reference 'b' should point to 'agg2'", "agg2", result.getReferences().get("b"));
        }
    }

    /**
     * Test parsing coordinator with empty references and stages (defaults)
     */
    public void testParseDefaults() throws Exception {
        String json = "{}";

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            parser.nextToken(); // Move to START_OBJECT

            TimeSeriesCoordinatorAggregationBuilder result = TimeSeriesCoordinatorAggregationBuilder.parse("coord_default", parser);

            assertEquals("coord_default", result.getName());
            assertTrue("Should have no stages by default", result.getStages().isEmpty());
            assertTrue("Should have no references by default", result.getReferences().isEmpty());
            assertNull("Should have null inputReference by default", result.getInputReference());
        }
    }

    /**
     * Test parsing coordinator with unknown fields (should be skipped)
     */
    public void testParseWithUnknownFields() throws Exception {
        String json = """
            {
              "unknown_field": "value",
              "references": {
                "a": "agg1"
              },
              "unknown_object": {}
            }
            """;

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            parser.nextToken(); // Move to START_OBJECT

            TimeSeriesCoordinatorAggregationBuilder result = TimeSeriesCoordinatorAggregationBuilder.parse("coord_unknown", parser);

            assertEquals("coord_unknown", result.getName());
            assertTrue("Should have no stages", result.getStages().isEmpty());
            assertEquals("Should have 1 reference", 1, result.getReferences().size());
            assertEquals("Reference 'a' should point to 'agg1'", "agg1", result.getReferences().get("a"));
            assertNull("Should have null inputReference", result.getInputReference());
        }
    }

    /**
     * Test parsing coordinator with multiple stages and references
     */
    public void testParseComplexConfiguration() throws Exception {
        String json = """
            {
              "stages": [
                {
                  "type": "scale",
                  "factor": 1.5
                },
                {
                  "type": "sum",
                  "labels": ["region"]
                }
              ],
              "references": {
                "input": "metrics",
                "compare": "baseline"
              },
              "inputReference": "input"
            }
            """;

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            parser.nextToken(); // Move to START_OBJECT

            TimeSeriesCoordinatorAggregationBuilder result = TimeSeriesCoordinatorAggregationBuilder.parse("coord_complex", parser);

            assertEquals("coord_complex", result.getName());
            assertEquals("Should have 2 stages", 2, result.getStages().size());
            assertTrue("First stage should be ScaleStage", result.getStages().get(0) instanceof ScaleStage);
            assertTrue("Second stage should be SumStage", result.getStages().get(1) instanceof SumStage);
            assertEquals("Should have 2 references", 2, result.getReferences().size());
            assertEquals("Reference 'input' should point to 'metrics'", "metrics", result.getReferences().get("input"));
            assertEquals("Reference 'compare' should point to 'baseline'", "baseline", result.getReferences().get("compare"));
            assertEquals("Input reference should be 'input'", "input", result.getInputReference());
        }
    }

    /**
     * Test parsing coordinator with boolean values in stages.
     * This tests the fix that allows parsing boolean values in stage arguments.
     */
    public void testParseStagesWithBooleanValues() throws Exception {
        String json = """
            {
              "stages": [
                {
                  "type": "subtract",
                  "keep_nans": false,
                  "right_op_reference": "4"
                },
                {
                  "type": "value_filter",
                  "operator": "ge",
                  "target_value": 200000000
                }
              ],
              "references": {
                "0": "0_coordinator",
                "4": "4_coordinator"
              },
              "inputReference": "0"
            }
            """;

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            parser.nextToken(); // Move to START_OBJECT

            TimeSeriesCoordinatorAggregationBuilder result = TimeSeriesCoordinatorAggregationBuilder.parse("coord_with_boolean", parser);

            assertEquals("coord_with_boolean", result.getName());
            assertEquals("Should have 2 stages", 2, result.getStages().size());
            assertEquals("Should have 2 references", 2, result.getReferences().size());
            assertEquals("Reference '0' should point to '0_coordinator'", "0_coordinator", result.getReferences().get("0"));
            assertEquals("Reference '4' should point to '4_coordinator'", "4_coordinator", result.getReferences().get("4"));
            assertEquals("Input reference should be '0'", "0", result.getInputReference());
        }
    }

    /**
     * Test parsing a real-world complex aggregation configuration with multiple coordinator pipelines.
     * This is a comprehensive test that ensures the parser can handle complex nested structures
     * including boolean values, multiple references, and various stage types.
     */
    public void testParseRealWorldComplexAggregation() throws Exception {
        String json = """
            {
              "buckets_path": [],
              "stages": [
                {
                  "type": "subtract",
                  "keep_nans": false,
                  "right_op_reference": "4"
                },
                {
                  "type": "value_filter",
                  "operator": "ge",
                  "target_value": 200000000
                },
                {
                  "type": "remove_empty"
                },
                {
                  "type": "alias_by_tags",
                  "tag_names": [
                    "namespace"
                  ]
                }
              ],
              "references": {
                "0": "0_coordinator",
                "4": "4_coordinator"
              },
              "inputReference": "0"
            }
            """;

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            parser.nextToken(); // Move to START_OBJECT

            TimeSeriesCoordinatorAggregationBuilder result = TimeSeriesCoordinatorAggregationBuilder.parse("9", parser);

            // Verify basic properties
            assertEquals("9", result.getName());
            assertEquals("Should have 4 stages", 4, result.getStages().size());
            assertEquals("Should have 2 references", 2, result.getReferences().size());
            assertEquals("Input reference should be '0'", "0", result.getInputReference());

            // Verify references
            assertEquals("Reference '0' should point to '0_coordinator'", "0_coordinator", result.getReferences().get("0"));
            assertEquals("Reference '4' should point to '4_coordinator'", "4_coordinator", result.getReferences().get("4"));

            // Verify the builder can be used to create an aggregator (validation passes)
            assertNotNull("Should be able to create aggregator", result.createInternal(Map.of()));
        }
    }

    /**
     * Test error handling for invalid stage type
     */
    public void testParseInvalidStageType() throws Exception {
        String json = """
            {
              "buckets_path": [],
              "stages": [
                {
                  "type": "unknown_stage",
                  "tag_names": [true, false],
                  "invalid_range": [1,2,3],
                  "invalid_long_range": [2147483648]
                }
              ],
              "references": {
                "0": "0_coordinator",
                "4": "4_coordinator"
              },
              "inputReference": "0"
            }
            """;

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            parser.nextToken(); // Move to START_OBJECT

            // Should throw IllegalArgumentException for unknown stage type
            expectThrows(IllegalArgumentException.class, () -> TimeSeriesCoordinatorAggregationBuilder.parse("9", parser));
        }
    }

    /**
     * Test parsing stages with array arguments containing numbers.
     * This tests the new array parsing logic that handles VALUE_NUMBER tokens.
     */
    public void testParseStageWithNumericArrayArguments() throws Exception {
        String json = """
            {
              "buckets_path": [],
              "stages": [
                {
                  "type": "percentile_of_series",
                  "percentiles": [50.0, 95.0, 99.0],
                  "interpolate": true
                }
              ],
              "references": {},
              "inputReference": "0"
            }
            """;

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            parser.nextToken(); // Move to START_OBJECT

            TimeSeriesCoordinatorAggregationBuilder result = TimeSeriesCoordinatorAggregationBuilder.parse("array_test", parser);

            assertEquals("array_test", result.getName());
            assertEquals(1, result.getStages().size());
            assertTrue(
                "Stage should be PercentileOfSeriesStage",
                result.getStages().get(0) instanceof org.opensearch.tsdb.lang.m3.stage.PercentileOfSeriesStage
            );
        }
    }

    /**
     * Test parsing stages with integer arrays.
     * This specifically tests INTEGER number type handling in arrays.
     */
    public void testParseStageWithIntegerArrayArguments() throws Exception {
        String json = """
            {
              "buckets_path": [],
              "stages": [
                {
                  "type": "histogram_percentile",
                  "bucket_id": "le",
                  "bucket_range": "bucket_range",
                  "percentiles": [50, 95, 99]
                }
              ],
              "references": {},
              "inputReference": "0"
            }
            """;

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            parser.nextToken(); // Move to START_OBJECT

            TimeSeriesCoordinatorAggregationBuilder result = TimeSeriesCoordinatorAggregationBuilder.parse("int_array_test", parser);

            assertEquals("int_array_test", result.getName());
            assertEquals(1, result.getStages().size());
            assertTrue(
                "Stage should be HistogramPercentileStage",
                result.getStages().get(0) instanceof org.opensearch.tsdb.lang.m3.stage.HistogramPercentileStage
            );
        }
    }

    /**
     * Test parsing stages with boolean array arguments.
     * This tests the new array parsing logic that handles VALUE_BOOLEAN tokens.
     */
    public void testParseStageWithBooleanArrayArguments() throws Exception {
        String json = """
            {
              "buckets_path": [],
              "stages": [
                {
                  "type": "show_tags",
                  "show_keys": true,
                  "tags": ["host", "region"]
                }
              ],
              "references": {},
              "inputReference": "0"
            }
            """;

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            parser.nextToken(); // Move to START_OBJECT

            TimeSeriesCoordinatorAggregationBuilder result = TimeSeriesCoordinatorAggregationBuilder.parse("bool_test", parser);

            assertEquals("bool_test", result.getName());
            assertEquals(1, result.getStages().size());
            assertTrue(
                "Stage should be ShowTagsStage",
                result.getStages().get(0) instanceof org.opensearch.tsdb.lang.m3.stage.ShowTagsStage
            );
        }
    }

    /**
     * Test parsing stages with array containing boolean values.
     * This tests VALUE_BOOLEAN token handling inside arrays.
     */
    public void testParseStageWithBooleanArrayValues() throws Exception {
        // Even though most stages don't accept boolean arrays, we should be able to parse them
        // The stage factory will validate and reject if needed
        String json = """
            {
              "buckets_path": [],
              "stages": [
                {
                  "type": "scale",
                  "factor": 2.0,
                  "test_array": [true, false, true]
                }
              ],
              "references": {},
              "inputReference": "0"
            }
            """;

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            parser.nextToken(); // Move to START_OBJECT

            // This should parse successfully even though scale stage may not use test_array
            TimeSeriesCoordinatorAggregationBuilder result = TimeSeriesCoordinatorAggregationBuilder.parse("bool_array_test", parser);

            assertEquals("bool_array_test", result.getName());
            assertEquals(1, result.getStages().size());
        }
    }

    /**
     * Test parsing stages with array containing null values.
     * This tests that null values in arrays throw an error.
     */
    public void testParseStageWithNullInArray() throws Exception {
        String json = """
            {
              "buckets_path": [],
              "stages": [
                {
                  "type": "scale",
                  "factor": 2.0,
                  "test_array": ["text", 123, null]
                }
              ],
              "references": {},
              "inputReference": "0"
            }
            """;

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            parser.nextToken(); // Move to START_OBJECT

            // Should throw IllegalArgumentException for null in array
            IllegalArgumentException exception = expectThrows(
                IllegalArgumentException.class,
                () -> TimeSeriesCoordinatorAggregationBuilder.parse("null_array_test", parser)
            );

            assertTrue(
                "Exception should mention unsupported array element type",
                exception.getMessage().contains("Unsupported array element type")
            );
            assertTrue("Exception should mention VALUE_NULL", exception.getMessage().contains("VALUE_NULL"));
        }
    }

    /**
     * Test parsing stages with array containing nested arrays.
     * This tests that nested arrays throw an error.
     */
    public void testParseStageWithNestedArrayInArray() throws Exception {
        String json = """
            {
              "buckets_path": [],
              "stages": [
                {
                  "type": "scale",
                  "factor": 2.0,
                  "test_array": ["text", [1, 2, 3]]
                }
              ],
              "references": {},
              "inputReference": "0"
            }
            """;

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            parser.nextToken(); // Move to START_OBJECT

            // Should throw IllegalArgumentException for nested array
            IllegalArgumentException exception = expectThrows(
                IllegalArgumentException.class,
                () -> TimeSeriesCoordinatorAggregationBuilder.parse("nested_array_test", parser)
            );

            assertTrue(
                "Exception should mention unsupported array element type",
                exception.getMessage().contains("Unsupported array element type")
            );
            assertTrue("Exception should mention START_ARRAY", exception.getMessage().contains("START_ARRAY"));
        }
    }

    /**
     * Test parsing MockFetch with nested objects in tags map - should reject nested structures.
     * MockFetch is the only stage that accepts map arguments (for tags), but only flat maps are supported.
     * This enforces time series best practices: tags/labels must be flat key-value pairs, no nesting.
     */
    public void testParseStageWithNestedMapRejectsNesting() throws Exception {
        String json = """
            {
              "buckets_path": [],
              "stages": [
                {
                  "type": "mockFetch",
                  "values": [1.0, 2.0, 3.0],
                  "tags": {
                    "name": "series_a",
                    "nested": {
                      "deeply": "unsupported"
                    }
                  }
                }
              ],
              "references": {},
              "inputReference": "0"
            }
            """;

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            parser.nextToken(); // Move to START_OBJECT

            // Should throw IllegalArgumentException for nested object in flat map
            // Only primitive values (string, number, boolean) are allowed in map values
            IllegalArgumentException exception = expectThrows(
                IllegalArgumentException.class,
                () -> TimeSeriesCoordinatorAggregationBuilder.parse("flat_map_test", parser)
            );

            assertTrue(
                "Exception should mention unsupported map value type",
                exception.getMessage().contains("Unsupported map value type")
            );
        }
    }

    /**
     * Test parsing map with mixed value types (string, int, double, boolean).
     * Comprehensive test covering all supported map value types together.
     */
    public void testParseStageWithMixedMapValueTypes() throws Exception {
        String json = """
            {
              "stages": [
                {
                  "type": "mockFetch",
                  "values": [10.0, 20.0, 30.0],
                  "tags": {
                    "name": "test_series",
                    "datacenter": "us-west-2",
                    "port": 8080,
                    "timeout_ms": 5000,
                    "threshold": 98.5,
                    "ratio": 0.85,
                    "production": true,
                    "enabled": false
                  }
                }
              ],
              "references": {}
            }
            """;

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            parser.nextToken(); // Move to START_OBJECT

            TimeSeriesCoordinatorAggregationBuilder result = TimeSeriesCoordinatorAggregationBuilder.parse("mixed_map_test", parser);

            assertEquals("mixed_map_test", result.getName());
            assertEquals(1, result.getStages().size());
            assertTrue(
                "Stage should be MockFetchStage",
                result.getStages().get(0) instanceof org.opensearch.tsdb.lang.m3.stage.MockFetchStage
            );
        }
    }

    /**
     * Test parsing map with null value - should reject null in maps.
     */
    public void testParseStageWithMapNullValueThrows() throws Exception {
        String json = """
            {
              "stages": [
                {
                  "type": "mockFetch",
                  "values": [1.0, 2.0],
                  "tags": {
                    "name": "series_a",
                    "nullField": null
                  }
                }
              ],
              "references": {}
            }
            """;

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            parser.nextToken(); // Move to START_OBJECT

            IllegalArgumentException exception = expectThrows(
                IllegalArgumentException.class,
                () -> TimeSeriesCoordinatorAggregationBuilder.parse("map_null_test", parser)
            );

            assertTrue(
                "Exception should mention unsupported map value type",
                exception.getMessage().contains("Unsupported map value type")
            );
        }
    }

    /**
     * Test parsing map with array value - should reject arrays in maps.
     */
    public void testParseStageWithMapArrayValueThrows() throws Exception {
        String json = """
            {
              "stages": [
                {
                  "type": "mockFetch",
                  "values": [1.0, 2.0],
                  "tags": {
                    "name": "series_a",
                    "values": [1, 2, 3]
                  }
                }
              ],
              "references": {}
            }
            """;

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            parser.nextToken(); // Move to START_OBJECT

            IllegalArgumentException exception = expectThrows(
                IllegalArgumentException.class,
                () -> TimeSeriesCoordinatorAggregationBuilder.parse("map_array_test", parser)
            );

            assertTrue(
                "Exception should mention unsupported map value type",
                exception.getMessage().contains("Unsupported map value type")
            );
        }
    }

}
