package com.github.ponkin.bloom;

import java.util.logging.Logger;
import java.util.logging.Level;
import java.util.Deque;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.io.IOException;

/**
 * Scalable bloom filter implementation
 * http://gsd.di.uminho.pt/members/cbm/ps/dbloom.pdf
 *
 * Scalable Bloom Filters are useful for cases where the size of the data set
 * isn't known a priori and memory constraints aren't of particular concern.
 * For situations where memory is bounded, consider using Inverse or Stable
 * Bloom Filters
 *
 * @author Alexey Ponkin
 */
public class ScalableBloomFilter implements Filter {

  private static final Logger log = Logger.getLogger(ScalableBloomFilter.class.getName());

  /**
   * tightening ratio
   */
  private final double ratio;

  /**
   * target false-positive rate
   */
  private final double fpp;

  /**
   * partition fill ratio
   */
  private final double pratio;

  /**
   * filter size hint
   */
  private final long hint;

  private final boolean useOffHeapMemory;

  private final Deque<PartitionedBloomFilter> filters;

  ScalableBloomFilter(double ratio, double fpp, double pratio, long hint, boolean useOffHeapMemory) throws IOException {
    this.ratio = ratio;
    this.fpp = fpp;
    this.pratio = pratio;
    this.hint = hint;
    this.useOffHeapMemory = useOffHeapMemory;
    this.filters = new ConcurrentLinkedDeque<>(); // must be concurrent to safe publishing inside synchronized
    this.filters.addFirst(newFilter());
  }
  
  @Override
  public boolean remove(byte[] bytes) {
    throw new UnsupportedOperationException("remove() method is not supported in ScalableBloomFilter");
  }

  @Override
  public boolean mightContain(byte[] bytes) {
    // check all availaible filters
    boolean mightContain = false;
    for(Filter filter: filters) {
      if(filter.mightContain(bytes)) {
        mightContain = true;
        break;
      }
    }
    return mightContain;
  }

  @Override
  public boolean put(byte[] bytes) {
    // If the active filter has reached its fill ratio, add a new one.
    if(filters.peekFirst().estimatedFillRatio() >= pratio) {
      synchronized(this) {
        if(filters.peekFirst().estimatedFillRatio() >= pratio) {
          try {
            filters.addFirst(newFilter());
          } catch (IOException err) {
            log.log(Level.SEVERE, "Can not enlarge ScalableBloomFilter", err);
            return false;
          }
        }
      }
    }
    return filters.peekFirst().put(bytes);
  }

  /**
   * Create new partitioned bloom filter.
   * New Filter will have smaller fpp(than previously created)
   * according to <code>pratio</pratio>, to keep
   * overall fpp close to target one.
   */
  private final PartitionedBloomFilter newFilter() throws IOException {
    double newFpp = fpp * Math.pow(pratio, (double) filters.size());// calculate new fpp
    return PartitionedBloomFilter.builder()
            .withExpectedNumberOfItems(hint)
            .withFalsePositiveRate(newFpp)
            .useOffHeapMemory(useOffHeapMemory)
            .build();
  }

  @Override
  public double expectedFpp() {
    // according to the paper http://gsd.di.uminho.pt/members/cbm/ps/dbloom.pdf
    double compoundFpp = filters.stream()
                                .mapToDouble((f) -> 1D - f.expectedFpp())
                                .reduce( (p1, p2) -> p1 * p2)
                                .orElse(2D);// if something goes wrong, imethod must return -1
    return 1D - compoundFpp;
  }

  @Override
  public synchronized void clear() {
    while(filters.size() > 1) {
      filters.removeFirst().close();// we need to free memory
    }
    filters.peekFirst().clear();
  }

  @Override
  public Filter mergeInPlace(Filter other) throws Exception {
    throw new UnsupportedOperationException("mergeInPlace method is not supported in ScalableBloomFilter");
  }

  @Override
  public synchronized void close() {
    do {
      filters.removeFirst().close();
    } while(!filters.isEmpty());
  }

  public static Builder builder() {
    return new Builder();
  }

  /**
   * Builder for ScalablePartiotionedBloomFilter
   */
  static class Builder {
  }
}

