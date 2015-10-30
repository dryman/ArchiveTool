package org.idryman.tool.index;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;

import com.google.common.annotations.VisibleForTesting;

public class DictVarintOutputStream extends FilterOutputStream implements LongOutputStream{
  private final static int [] mask_shift_64 = {69,62,56,48,41,34,27,20,13,6};
  private byte varint_buf_64[] = new byte[11];
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
    
    for (int idx : indexes) {
      unaryOutput.writeInt(idx);
    }
    unaryOutput.close();
    buffer = null;
    unaryOutput = null;
  }
  

  @Override
  public int estimateBytes(long[] numbers) {
    long [] dict = createDict(Arrays.copyOf(numbers, numbers.length));
    int [] indexes = new int [numbers.length];
    long prev = dict[0];
    for(int i=1; i<dict.length; i++) {
      long tmp = dict[i];
      dict[i] -= prev;
      prev = tmp;
    }
    return 0;
  }
  
  private long [] createDict(long[] numbers) {
    long [] buf = numbers;
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
  
  private long [] generateDict(long [] numbers) {
    long [] numberBuf = Arrays.copyOf(numbers, numbers.length);
    Arrays.sort(numberBuf);
    int i, j;
    for(i=0,j=1; j<numberBuf.length; j++) {
      if (numberBuf[i] < numberBuf[j]) {
        numberBuf[++i] = numberBuf[j];
      }
    }
    return Arrays.copyOf(numberBuf, i+1);
  }
  
  private void writeVarint(long number, boolean is_last) throws IOException {
    long masked;
    int pos=0;
    boolean seen = false;
    for (int shift : mask_shift_64) {
      masked = number & (0x7FL << shift);
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
  
  private int estimateVarintBytes(long [] numbers) {
    int acc = 0;
    for(long n : numbers) {
      //if ()
      // TODO calculate the leading zeros => which scheme
      int s = 64 - Long.numberOfLeadingZeros(n);
      for(int i=0; i<mask_shift_64.length-1; i++) {
        if (s <= mask_shift_64[i] && s > mask_shift_64[i+1]) {
          acc += 10-i;
        }
      }
      
    }
    return acc;
  }
  
  @Override
  public synchronized void flush() throws IOException {
    this.close();
  }
}
