/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.stage;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.AbstractWireSerializingTestCase;
import org.opensearch.tsdb.core.model.ByteLabels;
import org.opensearch.tsdb.core.model.FloatSample;
import org.opensearch.tsdb.core.model.Sample;
import org.opensearch.tsdb.query.aggregator.TimeSeries;
import org.opensearch.tsdb.query.stage.PipelineStage;
import org.opensearch.tsdb.query.stage.PipelineStageFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.opensearch.core.xcontent.ToXContent.EMPTY_PARAMS;
import static org.opensearch.tsdb.TestUtils.assertNullInputThrowsException;

public class TagSubStageTests extends AbstractWireSerializingTestCase<TagSubStage> {

    /**
     * Test case 1: Simple string replacement without regex.
     */
    public void testSimpleStringReplacement() {
        TagSubStage stage = new TagSubStage("env", "prod", "production");

        List<Sample> samples = List.of(new FloatSample(1000L, 10.0));
        ByteLabels labels = ByteLabels.fromStrings("env", "prod", "service", "api");
        TimeSeries timeSeries = new TimeSeries(samples, labels, 1000L, 1000L, 1000L, null);

        List<TimeSeries> result = stage.process(List.of(timeSeries));

        assertEquals(1, result.size());
        assertEquals("production", result.get(0).getLabels().get("env"));
        assertEquals("api", result.get(0).getLabels().get("service"));
    }

    /**
     * Test case 2: Regex replacement with backreference.
     * Replace "prod-east" with "production-east" using pattern "^prod-(.*)$" and replacement "production-$1".
     */
    public void testRegexWithBackreference() {
        TagSubStage stage = new TagSubStage("env", "^prod-(.*)$", "production-$1");

        List<Sample> samples = List.of(new FloatSample(1000L, 10.0));
        ByteLabels labels = ByteLabels.fromStrings("env", "prod-east", "service", "api");
        TimeSeries timeSeries = new TimeSeries(samples, labels, 1000L, 1000L, 1000L, null);

        List<TimeSeries> result = stage.process(List.of(timeSeries));

        assertEquals(1, result.size());
        assertEquals("production-east", result.get(0).getLabels().get("env"));
    }

    /**
     * Test case 3: Multiple backreferences.
     * Replace "prod-us-east" with "production_us_east" using pattern "^(\\w+)-(\\w+)-(\\w+)$".
     */
    public void testMultipleBackreferences() {
        TagSubStage stage = new TagSubStage("region", "^(\\w+)-(\\w+)-(\\w+)$", "$1_$2_$3");

        List<Sample> samples = List.of(new FloatSample(1000L, 10.0));
        ByteLabels labels = ByteLabels.fromStrings("region", "prod-us-east");
        TimeSeries timeSeries = new TimeSeries(samples, labels, 1000L, 1000L, 1000L, null);

        List<TimeSeries> result = stage.process(List.of(timeSeries));

        assertEquals(1, result.size());
        assertEquals("prod_us_east", result.get(0).getLabels().get("region"));
    }

    /**
     * Test case 4: Remove substring by replacing with empty string.
     * Remove version suffix like "-v123" from service names.
     */
    public void testRemoveSubstring() {
        TagSubStage stage = new TagSubStage("service", "-v[0-9]+$", "");

        List<Sample> samples = List.of(new FloatSample(1000L, 10.0));
        ByteLabels labels = ByteLabels.fromStrings("service", "api-v123");
        TimeSeries timeSeries = new TimeSeries(samples, labels, 1000L, 1000L, 1000L, null);

        List<TimeSeries> result = stage.process(List.of(timeSeries));

        assertEquals(1, result.size());
        assertEquals("api", result.get(0).getLabels().get("service"));
    }

    /**
     * Test case 5: Extract substring using regex group.
     * Extract region from "us-east-1-host123" to get "us-east-1".
     */
    public void testExtractSubstring() {
        TagSubStage stage = new TagSubStage("host", "^([a-z]+-[a-z]+-[0-9]+)-.*$", "$1");

        List<Sample> samples = List.of(new FloatSample(1000L, 10.0));
        ByteLabels labels = ByteLabels.fromStrings("host", "us-east-1-host123");
        TimeSeries timeSeries = new TimeSeries(samples, labels, 1000L, 1000L, 1000L, null);

        List<TimeSeries> result = stage.process(List.of(timeSeries));

        assertEquals(1, result.size());
        assertEquals("us-east-1", result.get(0).getLabels().get("host"));
    }

    /**
     * Test case 6: Non-matching pattern leaves tag unchanged.
     */
    public void testNonMatchingPatternLeavesTagUnchanged() {
        TagSubStage stage = new TagSubStage("env", "^prod-(.*)$", "production-$1");

        List<Sample> samples = List.of(new FloatSample(1000L, 10.0));
        ByteLabels labels = ByteLabels.fromStrings("env", "staging", "service", "api");
        TimeSeries timeSeries = new TimeSeries(samples, labels, 1000L, 1000L, 1000L, null);

        List<TimeSeries> result = stage.process(List.of(timeSeries));

        assertEquals(1, result.size());
        assertEquals("staging", result.get(0).getLabels().get("env"));
    }

    /**
     * Test case 7: Time series without the specified tag passes through unchanged.
     */
    public void testTimeSeriesWithoutSpecifiedTag() {
        TagSubStage stage = new TagSubStage("env", "prod", "production");

        List<Sample> samples = List.of(new FloatSample(1000L, 10.0));
        ByteLabels labels = ByteLabels.fromStrings("service", "api", "region", "us-east");
        TimeSeries timeSeries = new TimeSeries(samples, labels, 1000L, 1000L, 1000L, null);

        List<TimeSeries> result = stage.process(List.of(timeSeries));

        assertEquals(1, result.size());
        assertEquals(labels, result.get(0).getLabels());
    }

    /**
     * Test case 8: Time series with null labels passes through unchanged.
     */
    public void testTimeSeriesWithNullLabels() {
        TagSubStage stage = new TagSubStage("env", "prod", "production");

        List<Sample> samples = List.of(new FloatSample(1000L, 10.0));
        TimeSeries timeSeries = new TimeSeries(samples, null, 1000L, 1000L, 1000L, null);

        List<TimeSeries> result = stage.process(List.of(timeSeries));

        assertEquals(1, result.size());
        assertNull(result.get(0).getLabels());
    }

    /**
     * Test case 9: Multiple time series with different tag values.
     */
    public void testMultipleTimeSeriesWithDifferentValues() {
        TagSubStage stage = new TagSubStage("env", "^prod-(.*)$", "production-$1");

        List<TimeSeries> input = new ArrayList<>();

        // Series 1: env=prod-east (should be replaced)
        ByteLabels labels1 = ByteLabels.fromStrings("env", "prod-east");
        input.add(new TimeSeries(List.of(new FloatSample(1000L, 10.0)), labels1, 1000L, 1000L, 1000L, null));

        // Series 2: env=prod-west (should be replaced)
        ByteLabels labels2 = ByteLabels.fromStrings("env", "prod-west");
        input.add(new TimeSeries(List.of(new FloatSample(1000L, 20.0)), labels2, 1000L, 1000L, 1000L, null));

        // Series 3: env=staging (no match, unchanged)
        ByteLabels labels3 = ByteLabels.fromStrings("env", "staging");
        input.add(new TimeSeries(List.of(new FloatSample(1000L, 30.0)), labels3, 1000L, 1000L, 1000L, null));

        // Series 4: no env tag (unchanged)
        ByteLabels labels4 = ByteLabels.fromStrings("service", "api");
        input.add(new TimeSeries(List.of(new FloatSample(1000L, 40.0)), labels4, 1000L, 1000L, 1000L, null));

        List<TimeSeries> result = stage.process(input);

        assertEquals(4, result.size());
        assertEquals("production-east", result.get(0).getLabels().get("env"));
        assertEquals("production-west", result.get(1).getLabels().get("env"));
        assertEquals("staging", result.get(2).getLabels().get("env"));
        assertFalse(result.get(3).getLabels().has("env"));
    }

    /**
     * Test case 10: Preserve other labels when modifying one tag.
     */
    public void testPreserveOtherLabels() {
        TagSubStage stage = new TagSubStage("env", "prod", "production");

        List<Sample> samples = List.of(new FloatSample(1000L, 10.0));
        ByteLabels labels = ByteLabels.fromStrings("env", "prod", "service", "api", "region", "us-east", "version", "v1");
        TimeSeries timeSeries = new TimeSeries(samples, labels, 1000L, 1000L, 1000L, null);

        List<TimeSeries> result = stage.process(List.of(timeSeries));

        assertEquals(1, result.size());
        assertEquals("production", result.get(0).getLabels().get("env"));
        assertEquals("api", result.get(0).getLabels().get("service"));
        assertEquals("us-east", result.get(0).getLabels().get("region"));
        assertEquals("v1", result.get(0).getLabels().get("version"));
    }

    /**
     * Test case 11: Preserve alias when modifying labels.
     */
    public void testPreserveAlias() {
        TagSubStage stage = new TagSubStage("env", "prod", "production");

        List<Sample> samples = List.of(new FloatSample(1000L, 10.0));
        ByteLabels labels = ByteLabels.fromStrings("env", "prod");
        TimeSeries timeSeries = new TimeSeries(samples, labels, 1000L, 2000L, 1000L, "my-alias");

        List<TimeSeries> result = stage.process(List.of(timeSeries));

        assertEquals(1, result.size());
        assertEquals("production", result.get(0).getLabels().get("env"));
        assertEquals("my-alias", result.get(0).getAlias());
    }

    /**
     * Test case 12: Empty input list returns empty output.
     */
    public void testWithEmptyInput() {
        TagSubStage stage = new TagSubStage("env", "prod", "production");
        List<TimeSeries> result = stage.process(List.of());

        assertEquals(0, result.size());
    }

    /**
     * Test case 13: Null input should throw NullPointerException.
     */
    public void testWithNullInput() {
        TagSubStage stage = new TagSubStage("env", "prod", "production");
        assertNullInputThrowsException(stage, "tag_sub");
    }

    /**
     * Test getName returns correct stage name.
     */
    public void testGetName() {
        TagSubStage stage = new TagSubStage("env", "pattern", "replacement");
        assertEquals("tag_sub", stage.getName());
    }

    /**
     * Test equals and hashCode.
     */
    public void testEqualsAndHashCode() {
        TagSubStage stage1 = new TagSubStage("env", "prod", "production");
        TagSubStage stage2 = new TagSubStage("env", "prod", "production");
        TagSubStage stage3 = new TagSubStage("region", "prod", "production");
        TagSubStage stage4 = new TagSubStage("env", "staging", "production");
        TagSubStage stage5 = new TagSubStage("env", "prod", "staging");

        assertEquals(stage1, stage2);
        assertEquals(stage1.hashCode(), stage2.hashCode());

        assertNotEquals(stage1, stage3);
        assertNotEquals(stage1, stage4);
        assertNotEquals(stage1, stage5);
    }

    /**
     * Test fromArgs factory method with valid arguments.
     */
    public void testFromArgsWithValidArguments() {
        Map<String, Object> args = Map.of("tag_name", "env", "search_pattern", "prod", "replacement", "production");

        TagSubStage stage = TagSubStage.fromArgs(args);

        assertEquals("tag_sub", stage.getName());
    }

    /**
     * Test fromArgs with null args throws exception.
     */
    public void testFromArgsWithNullArgs() {
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> TagSubStage.fromArgs(null));
        assertEquals("Args cannot be null", exception.getMessage());
    }

    /**
     * Test fromArgs with missing tag_name throws exception.
     */
    public void testFromArgsWithMissingTagName() {
        Map<String, Object> args = Map.of("search_pattern", "prod", "replacement", "production");

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> TagSubStage.fromArgs(args));
        assertEquals("TagSub stage requires 'tag_name' argument", exception.getMessage());
    }

    /**
     * Test fromArgs with null tag_name throws exception.
     */
    public void testFromArgsWithNullTagName() {
        Map<String, Object> args = new java.util.HashMap<>();
        args.put("tag_name", null);
        args.put("search_pattern", "prod");
        args.put("replacement", "production");

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> TagSubStage.fromArgs(args));
        assertEquals("Tag name cannot be null or empty", exception.getMessage());
    }

    /**
     * Test fromArgs with empty tag_name throws exception.
     */
    public void testFromArgsWithEmptyTagName() {
        Map<String, Object> args = Map.of("tag_name", "", "search_pattern", "prod", "replacement", "production");

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> TagSubStage.fromArgs(args));
        assertEquals("Tag name cannot be null or empty", exception.getMessage());
    }

    /**
     * Test fromArgs with missing search_pattern throws exception.
     */
    public void testFromArgsWithMissingSearchPattern() {
        Map<String, Object> args = Map.of("tag_name", "env", "replacement", "production");

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> TagSubStage.fromArgs(args));
        assertEquals("TagSub stage requires 'search_pattern' argument", exception.getMessage());
    }

    /**
     * Test fromArgs with null search_pattern throws exception.
     */
    public void testFromArgsWithNullSearchPattern() {
        Map<String, Object> args = new java.util.HashMap<>();
        args.put("tag_name", "env");
        args.put("search_pattern", null);
        args.put("replacement", "production");

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> TagSubStage.fromArgs(args));
        assertEquals("Search pattern cannot be null or empty", exception.getMessage());
    }

    /**
     * Test fromArgs with empty search_pattern throws exception.
     */
    public void testFromArgsWithEmptySearchPattern() {
        Map<String, Object> args = Map.of("tag_name", "env", "search_pattern", "", "replacement", "production");

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> TagSubStage.fromArgs(args));
        assertEquals("Search pattern cannot be null or empty", exception.getMessage());
    }

    /**
     * Test fromArgs with missing replacement throws exception.
     */
    public void testFromArgsWithMissingReplacement() {
        Map<String, Object> args = Map.of("tag_name", "env", "search_pattern", "prod");

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> TagSubStage.fromArgs(args));
        assertEquals("TagSub stage requires 'replacement' argument", exception.getMessage());
    }

    /**
     * Test fromArgs with null replacement throws exception.
     */
    public void testFromArgsWithNullReplacement() {
        Map<String, Object> args = new java.util.HashMap<>();
        args.put("tag_name", "env");
        args.put("search_pattern", "prod");
        args.put("replacement", null);

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> TagSubStage.fromArgs(args));
        assertEquals("Replacement cannot be null", exception.getMessage());
    }

    /**
     * Test fromArgs with empty replacement (empty string is valid).
     */
    public void testFromArgsWithEmptyReplacement() {
        Map<String, Object> args = Map.of("tag_name", "env", "search_pattern", "prod", "replacement", "");

        TagSubStage stage = TagSubStage.fromArgs(args);
        assertNotNull(stage);
    }

    /**
     * Test serialization/deserialization with custom name.
     */
    public void testSerializationCustom() throws IOException {
        TagSubStage original = new TagSubStage("env", "^prod-(.*)$", "production-$1");

        BytesStreamOutput output = new BytesStreamOutput();
        original.writeTo(output);

        StreamInput input = output.bytes().streamInput();
        TagSubStage deserialized = TagSubStage.readFrom(input);

        assertEquals(original, deserialized);
    }

    /**
     * Test toXContent.
     */
    public void testToXContent() throws IOException {
        TagSubStage stage = new TagSubStage("env", "^prod-(.*)$", "production-$1");

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        stage.toXContent(builder, EMPTY_PARAMS);
        builder.endObject();

        String json = builder.toString();
        assertTrue(json.contains("\"tag_name\":\"env\""));
        assertTrue(json.contains("\"search_pattern\":\"^prod-(.*)$\""));
        assertTrue(json.contains("\"replacement\":\"production-$1\""));
    }

    /**
     * Test readFrom through PipelineStageFactory.
     */
    public void testReadFromThroughFactory() throws IOException {
        TagSubStage original = new TagSubStage("env", "prod", "production");

        BytesStreamOutput output = new BytesStreamOutput();
        output.writeString(original.getName());
        original.writeTo(output);

        StreamInput input = output.bytes().streamInput();
        PipelineStage deserialized = PipelineStageFactory.readFrom(input);

        assertTrue(deserialized instanceof TagSubStage);
        assertEquals(original, deserialized);
    }

    /**
     * Test case 14: Back reference using \1 syntax (new feature).
     * Replace "prod-east" with "production-east" using pattern "^prod-(.*)$" and replacement "production-\1".
     */
    public void testBackreferenceWithBackslashSyntax() {
        TagSubStage stage = new TagSubStage("env", "^prod-(.*)$", "production-\\1");

        List<Sample> samples = List.of(new FloatSample(1000L, 10.0));
        ByteLabels labels = ByteLabels.fromStrings("env", "prod-east", "service", "api");
        TimeSeries timeSeries = new TimeSeries(samples, labels, 1000L, 1000L, 1000L, null);

        List<TimeSeries> result = stage.process(List.of(timeSeries));

        assertEquals(1, result.size());
        assertEquals("production-east", result.get(0).getLabels().get("env"));
    }

    /**
     * Test case 15: Multiple back references using \1, \2, \3 syntax.
     * Replace "prod-us-east" with "production_us_east" using pattern "^(\\w+)-(\\w+)-(\\w+)$".
     */
    public void testMultipleBackreferencesWithBackslashSyntax() {
        TagSubStage stage = new TagSubStage("region", "^(\\w+)-(\\w+)-(\\w+)$", "\\1_\\2_\\3");

        List<Sample> samples = List.of(new FloatSample(1000L, 10.0));
        ByteLabels labels = ByteLabels.fromStrings("region", "prod-us-east");
        TimeSeries timeSeries = new TimeSeries(samples, labels, 1000L, 1000L, 1000L, null);

        List<TimeSeries> result = stage.process(List.of(timeSeries));

        assertEquals(1, result.size());
        assertEquals("prod_us_east", result.get(0).getLabels().get("region"));
    }

    /**
     * Test case 16: Back reference \0 for full match.
     * Replace entire match with prefix and suffix.
     */
    public void testBackreferenceZeroForFullMatch() {
        TagSubStage stage = new TagSubStage("env", "prod", "prefix-\\0-suffix");

        List<Sample> samples = List.of(new FloatSample(1000L, 10.0));
        ByteLabels labels = ByteLabels.fromStrings("env", "prod");
        TimeSeries timeSeries = new TimeSeries(samples, labels, 1000L, 1000L, 1000L, null);

        List<TimeSeries> result = stage.process(List.of(timeSeries));

        assertEquals(1, result.size());
        assertEquals("prefix-prod-suffix", result.get(0).getLabels().get("env"));
    }

    /**
     * Test case 17: Invalid back reference group number should throw exception.
     * Using \9 when only 2 groups exist should fail.
     */
    public void testInvalidBackreferenceGroupNumber() {
        TagSubStage stage = new TagSubStage("env", "^(\\w+)-(\\w+)$", "\\9");

        List<Sample> samples = List.of(new FloatSample(1000L, 10.0));
        ByteLabels labels = ByteLabels.fromStrings("env", "prod-east");
        TimeSeries timeSeries = new TimeSeries(samples, labels, 1000L, 1000L, 1000L, null);

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> stage.process(List.of(timeSeries)));
        assertTrue(exception.getMessage().contains("Invalid group reference"));
        assertTrue(exception.getMessage().contains("\\9"));
    }

    /**
     * Test case 18: Back reference to empty captured group.
     * Optional group that doesn't match should be replaced with empty string.
     */
    public void testBackreferenceToEmptyGroup() {
        TagSubStage stage = new TagSubStage("env", "^prod(-(.*))?$", "production\\2");

        List<Sample> samples = List.of(new FloatSample(1000L, 10.0));
        ByteLabels labels = ByteLabels.fromStrings("env", "prod");
        TimeSeries timeSeries = new TimeSeries(samples, labels, 1000L, 1000L, 1000L, null);

        List<TimeSeries> result = stage.process(List.of(timeSeries));

        assertEquals(1, result.size());
        assertEquals("production", result.get(0).getLabels().get("env"));
    }

    /**
     * Test case 19: Mixed $1 and \1 syntax in replacement.
     * Both should work correctly.
     */
    public void testMixedBackreferenceSyntax() {
        TagSubStage stage = new TagSubStage("env", "^(\\w+)-(\\w+)-(\\w+)$", "$1-\\2-$3");

        List<Sample> samples = List.of(new FloatSample(1000L, 10.0));
        ByteLabels labels = ByteLabels.fromStrings("env", "prod-us-east");
        TimeSeries timeSeries = new TimeSeries(samples, labels, 1000L, 1000L, 1000L, null);

        List<TimeSeries> result = stage.process(List.of(timeSeries));

        assertEquals(1, result.size());
        assertEquals("prod-us-east", result.get(0).getLabels().get("env"));
    }

    /**
     * Test case 20: Backreference with reordering of captured groups.
     * Swap positions using \2 and \1.
     */
    public void testBackreferenceReordering() {
        TagSubStage stage = new TagSubStage("region", "^(\\w+)-(\\w+)$", "\\2-\\1");

        List<Sample> samples = List.of(new FloatSample(1000L, 10.0));
        ByteLabels labels = ByteLabels.fromStrings("region", "us-east");
        TimeSeries timeSeries = new TimeSeries(samples, labels, 1000L, 1000L, 1000L, null);

        List<TimeSeries> result = stage.process(List.of(timeSeries));

        assertEquals(1, result.size());
        assertEquals("east-us", result.get(0).getLabels().get("region"));
    }

    /**
     * Test case 21: Duplicate back references.
     * Using the same group reference multiple times.
     */
    public void testDuplicateBackreferences() {
        TagSubStage stage = new TagSubStage("env", "^(\\w+)$", "\\1-\\1-\\1");

        List<Sample> samples = List.of(new FloatSample(1000L, 10.0));
        ByteLabels labels = ByteLabels.fromStrings("env", "prod");
        TimeSeries timeSeries = new TimeSeries(samples, labels, 1000L, 1000L, 1000L, null);

        List<TimeSeries> result = stage.process(List.of(timeSeries));

        assertEquals(1, result.size());
        assertEquals("prod-prod-prod", result.get(0).getLabels().get("env"));
    }

    /**
     * Test case 22: Back reference with complex pattern.
     * Extract and rearrange components from version string.
     */
    public void testBackreferenceComplexPattern() {
        TagSubStage stage = new TagSubStage("version", "^v([0-9]+)\\.([0-9]+)\\.([0-9]+)$", "version-\\1-\\2-\\3");

        List<Sample> samples = List.of(new FloatSample(1000L, 10.0));
        ByteLabels labels = ByteLabels.fromStrings("version", "v1.2.3");
        TimeSeries timeSeries = new TimeSeries(samples, labels, 1000L, 1000L, 1000L, null);

        List<TimeSeries> result = stage.process(List.of(timeSeries));

        assertEquals(1, result.size());
        assertEquals("version-1-2-3", result.get(0).getLabels().get("version"));
    }

    /**
     * Test case 23: Back reference with special characters in replacement.
     * Ensure special regex characters in replacement are handled properly.
     */
    public void testBackreferenceWithSpecialCharsInReplacement() {
        TagSubStage stage = new TagSubStage("env", "^(\\w+)$", "[\\1]");

        List<Sample> samples = List.of(new FloatSample(1000L, 10.0));
        ByteLabels labels = ByteLabels.fromStrings("env", "prod");
        TimeSeries timeSeries = new TimeSeries(samples, labels, 1000L, 1000L, 1000L, null);

        List<TimeSeries> result = stage.process(List.of(timeSeries));

        assertEquals(1, result.size());
        assertEquals("[prod]", result.get(0).getLabels().get("env"));
    }

    /**
     * Test case 24: No match scenario with back references.
     * When pattern doesn't match, original value should be preserved.
     */
    public void testNoMatchWithBackreferences() {
        TagSubStage stage = new TagSubStage("env", "^staging-(.*)$", "test-\\1");

        List<Sample> samples = List.of(new FloatSample(1000L, 10.0));
        ByteLabels labels = ByteLabels.fromStrings("env", "prod-east");
        TimeSeries timeSeries = new TimeSeries(samples, labels, 1000L, 1000L, 1000L, null);

        List<TimeSeries> result = stage.process(List.of(timeSeries));

        assertEquals(1, result.size());
        assertEquals("prod-east", result.get(0).getLabels().get("env"));
    }

    @Override
    protected TagSubStage createTestInstance() {
        return new TagSubStage(randomAlphaOfLength(5), randomAlphaOfLength(10), randomAlphaOfLength(10));
    }

    @Override
    protected Writeable.Reader<TagSubStage> instanceReader() {
        return TagSubStage::readFrom;
    }
}
