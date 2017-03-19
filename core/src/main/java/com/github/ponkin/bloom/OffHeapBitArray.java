package com.github.ponkin.bloom;

import sun.nio.ch.FileChannelImpl;

import java.util.logging.Logger;
import java.util.logging.Level;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

class OffHeapBitArray implements BitSet {

  private static final Logger log = Logger.getLogger(OffHeapBitArray.class.getName());

  enum State {
    CLOSED,
    MALLOC,
    MMAP
  }

  private static Method map0 =
    Platform.getMethod(FileChannelImpl.class, "map0", int.class, long.class, long.class);
  private static Method unmap0 =
    Platform.getMethod(FileChannelImpl.class, "unmap0", long.class, long.class);

  private final RandomAccessFile file;
  private final long addr;
  private final long numBits;
  private long bitCount = 0L;
  private State state;

  // word is 8 bits
  static long numWords(long numBits) {
    if (numBits <= 0) {
      throw new IllegalArgumentException("numBits must be positive, but got " + numBits);
    }
    // need to align with long
    long numWords = ((long) Math.ceil(numBits / 64.0) << 3);
    return numWords;
  }  

  /**
   * Create bit array in off heap memory
   *
   * @param numBits - number of bits
   */
  OffHeapBitArray(long numBits) {
    log.log(Level.INFO, String.format("Allocating off-heap memory for %1$d bits", numBits));
    this.file = null;
    this.addr = Platform.allocateRaw(numWords(numBits));
    this.numBits = numBits;
    this.state = State.MALLOC;
  }

  /**
   * Create bit array in off heap memory
   * with file mapped
   *
   * @param file - file to map bit array
   * @param numBits - number of bits to allocate
   */
  OffHeapBitArray(File file, long numBits) throws IOException {
    log.log(Level.INFO, String.format("Mapping off heap memory to '%1$s' for %2$d bits", file, numBits));
    this.file = new RandomAccessFile(file, "rw");
    this.numBits = numBits;
    long size = numWords(numBits);
    try {
      this.file.setLength(size);
      this.addr = map(this.file, 1, 0L, size);
      this.state = State.MMAP;
    } catch (IOException e) {
      log.log(Level.SEVERE, "Error while creating Offheap bitarray", e);
      try {
        this.file.close();
      } catch (IOException err) {
        log.log(Level.SEVERE, "Error while unmapping memory block", err);
      }
      throw e;
    }
  }

  /**
   * Return  size of bit array in number of bits
   *
   * @return number of bits in array
   */
  @Override
  public long bitSize() {
    return numBits;
  }

  /**
   * Return number of 1`s in underlying bit array
   *
   * @return number of set bits
   */
  @Override
  public long cardinality() {
    return bitCount;
  }

  /**
   * Set all bits in 0 in underlying bit array
   */
  @Override
  public void clear() {
    Platform.clear(addr, numWords(numBits));
  }

  @Override
  public boolean get(long index) {
    long pos = (index >>> 6) << 3;
    long chunk = 1L << index;
    long bit = Platform.getLong(addr+pos);
    return (bit & chunk) != 0L;
  }

  @Override
  public boolean set(long index) {
    long pos = (index >>> 6) << 3;
    long bit = 1L << index;
    long chunk = Platform.getLong(addr+pos);
    if( (bit & chunk) == 0L) {
      Platform.putLong(addr+pos, chunk | bit);
      bitCount++;
      return true;      
    }
    return false;      
  }

  @Override
  public boolean unset(long index) {
    long pos = (index >>> 6) << 3;
    long bit = 1L << index;
    long chunk = Platform.getLong(addr+pos);
    if( (bit & chunk) != 0L) {
      Platform.putLong(addr+pos, chunk & ~bit);
      bitCount--;
      return true;      
    }
    return false;      
  }


  @Override
  public void putAll(BitSet array) throws Exception {
    if (array == null || !(array instanceof OffHeapBitArray))  {
      throw new IncompatibleMergeException("Can't merge different bitsets");
    }
    OffHeapBitArray other = (OffHeapBitArray) array;
    if (this.numBits != other.numBits) {
      throw new IncompatibleMergeException("Can`t merge bitsets with different size");
    }
    long bitCount = 0;
    long size = numWords(numBits);
    for(long offset = 0; offset < size; offset+=8) {
      long thisByte = Platform.getLong(this.addr+offset);
      long otherByte = Platform.getLong(other.addr+offset);
      long newChunk = thisByte | otherByte;
      Platform.putLong(this.addr+offset, newChunk);
      bitCount += Long.bitCount(newChunk);
    }
    this.bitCount = bitCount;    
  }

  @Override
  public void close() {
    switch(state) {
      case MALLOC :
        Platform.freeMemory(addr);
        break;
      case MMAP:
        unmap(addr, numWords(numBits));
        try {
          file.close();
        } catch (IOException err) {
          log.log(Level.SEVERE, "Error while unmapping memory block", err);
        }
        break;
    }
    state = State.CLOSED;
  }

  private static long map(RandomAccessFile f, int mode, long start, long size) throws IOException {
    try {
      return (Long) map0.invoke(f.getChannel(), mode, start, size);
    } catch (IllegalAccessException e) {
      throw new IllegalStateException(e);
    } catch (InvocationTargetException e) {
      Throwable target = e.getTargetException();
      throw (target instanceof IOException) ? (IOException) target : new IOException(target);
    }
  }

  private static void unmap(long start, long size) {
    try {
      unmap0.invoke(null, start, size);
    } catch (IllegalAccessException e) {
      throw new IllegalStateException(e);
    } catch (InvocationTargetException err) {
      // Should not happen
      log.log(Level.SEVERE, "Error while invoking private sun API", err);
    }
  }  
}

