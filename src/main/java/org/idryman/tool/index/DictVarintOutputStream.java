package org.idryman.tool.index;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;

import com.google.common.annotations.VisibleForTesting;

public class DictVarintOutputStream extends FilterOutputStream implements LongOutputStream{
  private final static int [] mask_shift_64 = {55,48,41,34,27,20,13,6};
  private byte varint_buf_64[] = new byte[10];
  private UnaryCodeOutputStream unaryOutput;
  private long buffer[];
  private int count;

  public DictVarintOutputStream(OutputStream out) {
    this(out, 1024);
  }
  
  public DictVarintOutputStream(OutputStream out, int length) {
    super(out);
    this.count = 0;
    this.buffer = new long[length];
    this.unaryOutput = new UnaryCodeOutputStream(out);
  }
  
  @Override
  public synchronized void writeLong(long number) throws IOException {
    if (count >= buffer.length) {
      long buf_tmp [] = new long[buffer.length * 2];
      for (int i=0; i<buffer.length; i++)
        buf_tmp[i] = buffer[i];
      this.buffer = buf_tmp;
    }
    this.buffer[count++] = number;
  }
  
  @Override
  public synchronized void close() throws IOException {
    long [] dict = createDict(Arrays.copyOf(buffer, count));
    int [] indexes = new int [count];

    for(int i=0; i<count; i++) {
      indexes[i] = Arrays.binarySearch(dict, buffer[i]);
    }
    
    long prev = dict[0];
    for(int i=1; i<dict.length; i++) {
      writeVarint(dict[i-1], false);
      long tmp = dict[i];
      dict[i] -= prev;
      prev = tmp;
    }
    
    writeVarint(dict[dict.length-1], true);
    /*
     * It might be better if we use base2/base4 golomb-rice coding here
     * The penalty on long sequence of integers is quite high when we use plain unary coding.
     */
    for (int idx : indexes) {
      unaryOutput.writeInt(idx);
    }
    unaryOutput.close();
    super.flush();
    buffer = null;
  }
  

  @Override
  public int estimateBytes(long[] numbers) {
    long [] dict = createDict(Arrays.copyOf(numbers, numbers.length));
    int [] indexes = new int [numbers.length];
    
    for(int i=0; i<numbers.length; i++) {
      indexes[i] = Arrays.binarySearch(dict, numbers[i]);
    }
    
    long prev = dict[0];
    for(int i=1; i<dict.length; i++) {
      long tmp = dict[i];
      dict[i] -= prev;
      prev = tmp;
    }
    int dict_size = estimateVarintBytes(dict);
    int idx_size  = unaryOutput.estimateBytes(indexes);
    return dict_size + idx_size;
  }
  
  @VisibleForTesting
  long [] createDict(long[] numbers) {
    long [] buf = Arrays.copyOf(numbers, numbers.length);
    Arrays.sort(buf);
    // Get distinct elements only
    int i, j;
    for(i=0,j=1; j<buf.length; j++) {
      if (buf[i] < buf[j]) {
        buf[++i] = buf[j];
      }
    }
    buf = Arrays.copyOf(buf, i+1);
    return buf;
  }
  
  private void writeVarint(long number, boolean is_last) throws IOException {
    long masked;
    int pos=0;
    boolean seen = false;
    masked = number & 0xC0_00_00_00_00_00_00_00L;
    if (masked!=0) {
      varint_buf_64[pos++] = (byte) (masked >>>= 62);
      seen = true;
    }
    for (int shift : mask_shift_64) {
      final long mask = 0x7FL << shift;
      masked = number & mask;
      if (seen || masked != 0) {
        varint_buf_64[pos++] = (byte) (masked >>>= shift);
        seen = true;
      }
    }
    byte val = (byte) ((number & 0x3F) | 0x80);
    if (is_last) {
      val |= (byte)0x40;
    }
    varint_buf_64[pos++] = val;
    super.write(varint_buf_64, 0, pos);
  }
  
  @VisibleForTesting
  private int estimateVarintBytes(long [] numbers) {
    int sum = 0;
    for(long n : numbers) {
      int s = 64 - Long.numberOfLeadingZeros(n);
      for (int i=0; i<10; i++) {
        if (s <= (6+i*7)) {
          sum+=(1+i);
          break;
        }
      }
    }
    return sum;
  }
  
  @Override
  public synchronized void flush() throws IOException {
    this.close();
  }
}
