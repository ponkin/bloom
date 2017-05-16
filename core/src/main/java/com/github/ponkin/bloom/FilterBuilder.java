package com.github.ponkin.bloom;

import java.io.File;
import java.io.IOException;

/**
 * Common builder interface
 * for all filters
 *
 * @author Alexey Ponkin
 */
public interface FilterBuilder<T extends Filter> {

    FilterBuilder useOffHeapMemory(boolean useOffHeapMemory);

    FilterBuilder withFalsePositiveRate(double fprate);

    FilterBuilder withExpectedNumberOfItems(long expected);

    FilterBuilder withFileMapped(File file);

    FilterBuilder withHasher(HashFunction hasher);

    T build() throws IOException;
}
