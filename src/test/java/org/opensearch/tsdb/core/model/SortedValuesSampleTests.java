/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.model;

import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SortedValuesSampleTests extends AbstractWireSerializingTestCase<SortedValuesSample> {

    @Override
    protected Writeable.Reader<SortedValuesSample> instanceReader() {
        // We need a custom reader since SortedValuesSample.readFrom expects the timestamp already read
        return in -> {
            long timestamp = in.readLong();
            SampleType type = SampleType.readFrom(in);
            if (type != SampleType.SORTED_VALUES_SAMPLE) {
                throw new IOException("Expected SORTED_VALUES_SAMPLE but got " + type);
            }
            return SortedValuesSample.readFrom(in, timestamp);
        };
    }

    @Override
    protected SortedValuesSample createTestInstance() {
        long timestamp = randomLong();

        // Randomly create either a single-value or multi-value sample
        if (randomBoolean()) {
            // Single value
            return new SortedValuesSample(timestamp, randomDouble());
        } else {
            // Multiple values (already sorted)
            int count = randomIntBetween(2, 10);
            List<Double> values = new ArrayList<>();
            for (int i = 0; i < count; i++) {
                values.add(randomDouble());
            }
            // Sort the values since SortedValuesSample expects sorted input
            values.sort(Double::compareTo);
            return new SortedValuesSample(timestamp, values);
        }
    }

    public void testSingleValue() {
        SortedValuesSample sample = new SortedValuesSample(1000L, 42.0);

        assertEquals(1000L, sample.getTimestamp());
        assertEquals(42.0, sample.getValue(), 0.0001);
        assertEquals(ValueType.FLOAT64, sample.valueType());
        assertEquals(SampleType.SORTED_VALUES_SAMPLE, sample.getSampleType());
        assertEquals(1, sample.getSortedValueList().size());
        assertEquals(42.0, sample.getSortedValueList().get(0), 0.0001);
    }

    public void testMultipleValues() {
        List<Double> values = List.of(10.0, 20.0, 30.0);
        SortedValuesSample sample = new SortedValuesSample(2000L, values);

        assertEquals(2000L, sample.getTimestamp());
        // getValue() should return 50th percentile (median) = 20.0
        assertEquals(20.0, sample.getValue(), 0.0001);
        assertEquals(3, sample.getSortedValueList().size());
        assertEquals(values, sample.getSortedValueList());
    }

    public void testGetValueMedianOddCount() {
        // Odd count: [10, 20, 30, 40, 50]
        // 50th percentile: fractionalRank=0.5*5=2.5, ceil=3, index=2 -> 30
        List<Double> values = List.of(10.0, 20.0, 30.0, 40.0, 50.0);
        SortedValuesSample sample = new SortedValuesSample(1000L, values);
        assertEquals(30.0, sample.getValue(), 0.0001);
    }

    public void testGetValueMedianEvenCount() {
        // Even count: [10, 20, 30, 40]
        // 50th percentile: fractionalRank=0.5*4=2.0, ceil=2, index=1 -> 20
        List<Double> values = List.of(10.0, 20.0, 30.0, 40.0);
        SortedValuesSample sample = new SortedValuesSample(1000L, values);
        assertEquals(20.0, sample.getValue(), 0.0001);
    }

    public void testMergeSamples() {
        SortedValuesSample sample1 = new SortedValuesSample(1000L, List.of(10.0, 30.0, 50.0));
        SortedValuesSample sample2 = new SortedValuesSample(1000L, List.of(20.0, 40.0));

        SortedValuesSample merged = sample1.merge(sample2);

        assertEquals(1000L, merged.getTimestamp());
        assertEquals(List.of(10.0, 20.0, 30.0, 40.0, 50.0), merged.getSortedValueList());
    }

    public void testMergeSamplesDisjoint() {
        SortedValuesSample sample1 = new SortedValuesSample(1000L, List.of(10.0, 20.0));
        SortedValuesSample sample2 = new SortedValuesSample(1000L, List.of(30.0, 40.0));

        SortedValuesSample merged = sample1.merge(sample2);

        assertEquals(List.of(10.0, 20.0, 30.0, 40.0), merged.getSortedValueList());
    }

    public void testMergeSamplesOverlapping() {
        SortedValuesSample sample1 = new SortedValuesSample(1000L, List.of(10.0, 30.0));
        SortedValuesSample sample2 = new SortedValuesSample(1000L, List.of(10.0, 20.0, 30.0));

        SortedValuesSample merged = sample1.merge(sample2);

        // Should include duplicates from both lists
        assertEquals(List.of(10.0, 10.0, 20.0, 30.0, 30.0), merged.getSortedValueList());
    }

    public void testMergeWithInterfaceMethod() {
        SortedValuesSample sample1 = new SortedValuesSample(1000L, List.of(10.0, 30.0));
        Sample sample2 = new SortedValuesSample(1000L, List.of(20.0, 40.0));

        Sample merged = sample1.merge(sample2);

        assertTrue(merged instanceof SortedValuesSample);
        assertEquals(List.of(10.0, 20.0, 30.0, 40.0), ((SortedValuesSample) merged).getSortedValueList());
    }

    public void testMergeWithWrongType() {
        SortedValuesSample sample1 = new SortedValuesSample(1000L, 10.0);
        FloatSample sample2 = new FloatSample(1000L, 20.0);

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> sample1.merge(sample2));
        assertTrue(e.getMessage().contains("Cannot merge SortedValuesSample with FloatSample"));
    }

    public void testEquals() {
        SortedValuesSample sample1 = new SortedValuesSample(1000L, List.of(10.0, 20.0, 30.0));
        SortedValuesSample sample2 = new SortedValuesSample(1000L, List.of(10.0, 20.0, 30.0));
        SortedValuesSample sample3 = new SortedValuesSample(1000L, List.of(10.0, 20.0));
        SortedValuesSample sample4 = new SortedValuesSample(2000L, List.of(10.0, 20.0, 30.0));

        assertEquals(sample1, sample2);
        assertNotEquals(sample1, sample3); // Different values
        assertNotEquals(sample1, sample4); // Different timestamp
        assertNotEquals(sample1, null);
        assertNotEquals(sample1, new FloatSample(1000L, 10.0));
    }

    public void testHashCode() {
        SortedValuesSample sample1 = new SortedValuesSample(1000L, List.of(10.0, 20.0, 30.0));
        SortedValuesSample sample2 = new SortedValuesSample(1000L, List.of(10.0, 20.0, 30.0));

        assertEquals(sample1.hashCode(), sample2.hashCode());
    }
}
