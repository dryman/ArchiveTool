package org.idryman.tool.index;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;

import org.apache.commons.codec.binary.Hex;

public class DictVarintOutputStream extends FilterOutputStream{
  private final static int [] mask_shift_64 = {69,62,56,48,41,34,27,20,13,6};
  private byte varint_buf_64[] = new byte[11];
  private UnaryCodeOutputStream unaryOutput;

  public DictVarintOutputStream(OutputStream out) {
    super(out);
    unaryOutput = new UnaryCodeOutputStream(out);
  }


  public synchronized void writeLongs(long [] numbers) throws IOException {
    long [] numberBuf = generateDict(numbers);
    int [] indexes = new int [numbers.length];
    System.out.println(Arrays.toString(numberBuf));

    for(int i=0; i<numbers.length; i++) {
      indexes[i] = Arrays.binarySearch(numberBuf, numbers[i]);
    }
    
    // Now we can write varint... but cannot use existing one :(
    // It's a special varint that has the last byte using only 6bits
    // When it is end of a int, it has MSB 00; when it is the end of whole varint
    // the MSB is 01
    writeVarint(numberBuf[0], false);
    for(int i=1; i<numberBuf.length-1; i++)
      writeVarint(numberBuf[i]-numberBuf[i-1], false);
    if (numberBuf.length>1)
      writeVarint(numberBuf[numberBuf.length-1]-numberBuf[numberBuf.length-2], true);
    
    unaryOutput.writeInts(indexes);

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

  public static void main(String[] args) throws IOException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(bos);
    DictVarintOutputStream dvos = new DictVarintOutputStream(dos);
    long [] longs = new long[]{3,3,3,4,4,1,1,2,3,4,4,1,1,4,4};
    dvos.writeLongs(longs);
    dvos.close();
    System.out.println(Hex.encodeHexString(bos.toByteArray()));
    System.out.println(dos.size());
    
    ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
    DictVarintInputStream dvis = new DictVarintInputStream(bis);
    
    
    for (int i=0; i<longs.length; i++) {
      System.out.println(dvis.readLong());
    }
    dvis.close();
  }
}
