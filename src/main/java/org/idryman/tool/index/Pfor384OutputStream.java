package org.idryman.tool.index;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;

import org.apache.log4j.Logger;

public class Pfor384OutputStream extends FilterOutputStream{
  private final static Logger LOG = Logger.getLogger(Pfor384OutputStream.class);
  private final static int [] schemes = {4,6,8,12,16,24,32,48,64};
  private final static int [] mask_shift_64 = {70,63,56,49,42,35,28,21,14,7};
  private byte varint_buf_64[] = new byte[11];
  private final long [] buffer = new long[8192];
  private int posIn = 0, posOut=0;
  
  /*
   * I may want to add some other scheme, like 14, 18, and 20
   * These are the most common seen number ranges
   */
  
  public Pfor384OutputStream(OutputStream out) {
    super(out);
  }
  
  public synchronized void writeLong(long in) throws IOException {
    if (posIn >= buffer.length) {
      adjustBuffer();
    }
    buffer[posIn++] = in;
    if (posIn-posOut >= 96) {
      flushGroupInts();
    }
  }
  
  private void adjustBuffer() {
    LOG.info("adjust buffer");
    for(int i=posOut, j=0; i<posIn; i++, j++) {
      buffer[j] = buffer[i];
    }
    posIn = posIn-posOut;
    posOut=0;
  }
  
  private void flushGroupInts() throws IOException {
    final int [] paramRef = new int [3]; // penalty, patch_offset, varints 
    
    final int decision = makeDecision(paramRef);
    
    writeWithScheme(buffer, posOut, decision, paramRef[0], paramRef[1], paramRef[2]);
    LOG.assertLog(decision != -1, "decision should not be -1");
    posOut += 384/schemes[decision];
  }
  
  @Override
  public void close() throws IOException {
    flushRemaining();
    super.close();
  }
  
  private void flushRemaining() throws IOException {
    final int [] paramRef = new int [3]; // penalty, patch_offset, varints 
    
    while(true) {
      final int decision = makeDecision(paramRef);
      if (decision != -1) {
        writeWithScheme(buffer, posOut, decision, paramRef[0], paramRef[1], paramRef[2]);
        posOut += 384/schemes[decision];
      } else {
        
        break;
      }
    }
  }
  
  /**
   * Make decision of choosing which scheme as the compression scheme.
   * decision 0-8 corresponding to 9 schemes. -1 is varint.
   * Other return value are set in paramRef.
   * @param input input array
   * @param pos current position in the array
   * @param paramRef tuple of {penalty, patch_offset, varints}
   * @return decision
   */
  private int makeDecision(int paramRef []) {
    final int pos = posOut;
    final int limit = posIn;
    final int [] penalties = new int [9];
    final int [] varints = new int[9];
    final int [] patch_offset = new int [9];
    
    // penalty, patch_offset, number of varint
    assert(paramRef.length==3);
    
    boolean considerVarInt = false;
    int decision=0;
    
    // Scheme4
    {
      final int scheme4 = schemes[0];
      final int end4 = 384/scheme4+pos;
      final long scheme4_limit = (1L<<scheme4)-1;
      penalties[0] = 0;
      varints[0] = 0;
      
      if (end4 > limit) {
        penalties[0] = Integer.MAX_VALUE;
        considerVarInt = true;
      } else {  
        // scheme 4 is special case.
        int diff_pos=-1;
        for (int j=pos; j<end4; j++) {
          if (buffer[j] > scheme4_limit) {
            
            // Case 1, first seen outlier. 
            // The maximum allowed offset is 62.
            if (diff_pos==-1) {
              if (j-pos > 62) {
                penalties[0] = Integer.MAX_VALUE;
                break;
              }
              patch_offset[0] = j-diff_pos;
              diff_pos = j;
              penalties[0] += varIntPenalty(buffer[j]);
              varints[0]++;
              continue;
            }
            
            // Case 2, not first seen. 
            // Maximum difference is 15
            if (j-diff_pos > scheme4_limit) {
              penalties[0] = Integer.MAX_VALUE;
              break;
            }
            
            // Otherwise calculate the penalty and proceed
            diff_pos = j;
            penalties[0] += varIntPenalty(buffer[j]);
            varints[0]++;
          }
        }
      }
    } // end scheme4
    
    // Rest of the schemes
    for (int i = 1; i<9; i++) {
      final int scheme = schemes[i];
      final int end = 384/scheme+pos;
      // FIXME the limit of scheme64 should use unsigned
      final long scheme_limit = scheme==64? Long.MAX_VALUE : (1L<<scheme) -1;
      if (end > limit) {
        penalties[i] = Integer.MAX_VALUE;
        considerVarInt = true;
        continue;
      }
      penalties[i] = 0;
      varints[i] = 0;
      boolean first_patch=true;
      for (int j=pos; j<end; j++) {
        if (buffer[j] > scheme_limit) {
          penalties[i] += varIntPenalty(buffer[j]);
          varints[i]++;
          if (first_patch) {
            patch_offset[i] = j-pos;
            first_patch = false;
          }
        }
      }
    }
    
    // A corner case in scheme 6
    // offset max is 62+1, but available length is 64 (one value is used for "no patch")
    if (patch_offset[1] > 62) {
      penalties[1] = Integer.MAX_VALUE;
    }
    
    double lowest_rate = Double.MAX_VALUE;
    for (int i=0; i<9; i++) {
      
      int tail_byte=0;
      // Add tailing byte if varints not aligned on 16bit
      if (penalties[i]!=Integer.MAX_VALUE) {
        if (penalties[i]-((penalties[i] >>> 4)<<4)!=0) {
          tail_byte=8;
        }
      }
      final double ints = 384/schemes[i];
      final double rate = (16.0+384.0+penalties[i]+tail_byte)/ints;
      if (rate < lowest_rate) {
        lowest_rate = rate;
        decision = i;
      }
    }
    
    if (considerVarInt) {
      int varint_penalty = 0;
      for (int j=pos; j<limit; j++) {
        varint_penalty += varIntPenalty(buffer[j]);
      }
      double rate = (8.0+varint_penalty)/(limit-pos);
      
      if (rate < lowest_rate) {
        LOG.debug("using varint");
        paramRef[0] = varint_penalty;
        paramRef[1] = 0; // no patch_offset
        paramRef[2] = limit-pos;
        return -1;
      }
    }
    
    LOG.info("using scheme: "+schemes[decision] + " with rate: " + (int)lowest_rate + " penalty "+ penalties[decision] + 
        " patch_offset " + patch_offset[decision] + " and varints " + varints[decision]);
    paramRef[0] = penalties[decision];
    paramRef[1] = patch_offset[decision];
    paramRef[2] = varints[decision];
    return decision;
  }
  
  private void writeWithScheme(long input[], int pos, int decision, int penalty, int patch_offset, int varints) throws IOException {
    byte [] header = new byte[2];
    
    header[0] = (byte) decision;
    
    int skip = penalty >>> 4;
    boolean has_tail=false;
      
    if (penalty-(skip<<4)!=0) {
      skip++;
      has_tail=true;
    }
    assert (skip < (1<<6)-1);
    header[1] = (byte) skip;
    
    byte patch_l = (byte) (patch_offset & 0x0F);
    byte patch_h = (byte) (patch_offset & 0x30);
    header[0] += patch_l<<4;
    header[1] += patch_h<<2;
    
    super.write(header);
    
    switch(decision) {
    case 0:
      writeScheme4( Arrays.copyOfRange(input, pos, pos+96), patch_offset, varints); break;
    case 1:
      writeScheme6( Arrays.copyOfRange(input, pos, pos+64), patch_offset, varints); break;
    case 2:
      writeScheme8( Arrays.copyOfRange(input, pos, pos+48), patch_offset, varints); break;
    case 3:
      writeScheme12(Arrays.copyOfRange(input, pos, pos+32), patch_offset, varints); break;
    case 4:
      writeScheme16(Arrays.copyOfRange(input, pos, pos+24), patch_offset, varints); break;
    case 5:
      writeScheme24(Arrays.copyOfRange(input, pos, pos+16), patch_offset, varints); break;
    case 6:
      writeScheme32(Arrays.copyOfRange(input, pos, pos+12), patch_offset, varints); break;
    case 7:
      writeScheme48(Arrays.copyOfRange(input, pos, pos+ 8), patch_offset, varints); break;
    default:
      writeScheme64(Arrays.copyOfRange(input, pos, pos+ 6), patch_offset, varints);
    }
    
    // Make it 16bit aligned
    if (has_tail) {
      super.write(0);
    }
  }
  
  private void writeScheme4(long input[], int patch_offset, int varints) throws IOException {
    long [] varint_buf = new long [varints];
    byte [] buf = new byte [48];
    assert(input.length==96);
    
    patchInput(input, patch_offset, varint_buf, (1L<<4) -1);
    
    // Now all elements in input is less than 15
    for (int i=0, j=0; i<input.length; i+=32, j+=16) {
      for(int k=0; k<16; k++) {
        buf[j+k] = (byte)(input[i+k] | (input[i+k+16] << 4));
      }
    }
    super.write(buf);
    for (long l : varint_buf) {
      writeVarInt64(l);
    }
  }
  
  private void writeScheme6(long input[], int patch_offset, int varints) throws IOException {
    long [] varint_buf = new long [varints];
    byte [] buf = new byte [48];
    patchInput(input, patch_offset, varint_buf, (1L<<6) -1);
    
    assert(input.length==64);
    for(int i=0; i<input.length-16; i++) {
      buf[i] = (byte)input[i];
    }
    for(int i=input.length-16; i<input.length; i++) {
      byte b = (byte) input[i];
      buf[i-48] |= (b & 0x03) << 6;
      buf[i-32] |= (b & 0x0C) << 4;
      buf[i-16] |= (b & 0x30) << 2;
    }
    super.write(buf);
    for (long l : varint_buf) {
      writeVarInt64(l);
    }
  }
  
  private void writeScheme8(long input[], int patch_offset, int varints) throws IOException {
    long [] varint_buf = new long [varints];
    byte [] buf = new byte [48];
    patchInput(input, patch_offset, varint_buf, (1L<<8) -1);
    assert(input.length==48);
    for(int i=0; i<input.length; i++) {
      buf[i] = (byte) input[i];
    }
    super.write(buf);
    for(long l: varint_buf)
      writeVarInt64(l);
  }
  
  private void writeScheme12(long input[], int patch_offset, int varints) throws IOException {
    long [] varint_buf = new long [varints];
    byte [] buf = new byte [48];
    patchInput(input, patch_offset, varint_buf, (1L<<12) -1);
    assert(input.length==32);
    for(int i=0,j=0; i<input.length-8; i++, j+=2) {
      buf[j]   = (byte)(input[i] & 0xFF);
      buf[j+1] = (byte)((input[i] & 0x0F00)>>>8);
    }
    for(int i=input.length-8, j=1; i<input.length; i++, j+=2) {
      long l = input[i];
      buf[j]    |= (byte) ((l & 0x0F) << 4); l>>=4;
      buf[j+8]  |= (byte) ((l & 0x0F) << 4); l>>=4;
      buf[j+16] |= (byte) ((l & 0x0F) << 4); 
    }
    super.write(buf);
    for(long l: varint_buf)
      writeVarInt64(l);
  }
  
  private void writeScheme16(long input[], int patch_offset, int varints) throws IOException {
    long [] varint_buf = new long [varints];
    byte [] buf = new byte [48];
    patchInput(input, patch_offset, varint_buf, (1L<<16) -1);
    assert(input.length==24);
    for(int i=0,j=0; i<input.length; i++,j+=2) {
      long l = input[i];
      buf[j]   = (byte) (l & 0xFF); l >>= 8;
      buf[j+1] = (byte) (l & 0xFF);
    }
    super.write(buf);
    for(long l: varint_buf)
      writeVarInt64(l);
  }
  
  private void writeScheme24(long input[], int patch_offset, int varints) throws IOException {
    long [] varint_buf = new long [varints];
    byte [] buf = new byte [48];
    patchInput(input, patch_offset, varint_buf, (1L<<24) -1);
    assert(input.length==16);
    
    for(int i=0,j=0; i<input.length-4; i++,j+=4) {
      long l = input[i];
      buf[j]   = (byte) (l & 0xFF); l >>= 8;
      buf[j+1] = (byte) (l & 0xFF); l >>= 8;
      buf[j+2] = (byte) (l & 0xFF);
    }
    for(int i=input.length-4, j=3; i<input.length; i++, j+=4) {
      long l = input[i];
      buf[j]    = (byte) (l & 0xFF); l>>=8;
    // TODO may be wrong
      buf[j+16] = (byte) (l & 0xFF); l>>=8;
      buf[j+32] = (byte) (l & 0xFF);; 
    }
    super.write(buf);
    for(long l: varint_buf)
      writeVarInt64(l);
  }
  
  private void writeScheme32(long input[], int patch_offset, int varints) throws IOException {
    long [] varint_buf = new long [varints];
    byte [] buf = new byte [48];
    patchInput(input, patch_offset, varint_buf, (1L<<32) -1);
    assert(input.length==12);
    for(int i=0,j=0; i<input.length-4; i++,j+=4) {
      long l = input[i];
      buf[j]   = (byte) (l & 0xFF); l >>= 8;
      buf[j+1] = (byte) (l & 0xFF); l >>= 8;
      buf[j+2] = (byte) (l & 0xFF); l >>= 8;
      buf[j+3] = (byte) (l & 0xFF);
    }
    super.write(buf);
    for(long l: varint_buf)
      writeVarInt64(l);
  }
  
  private void writeScheme48(long input[], int patch_offset, int varints) throws IOException {
    long [] varint_buf = new long [varints];
    byte [] buf = new byte [48];
    patchInput(input, patch_offset, varint_buf, (1L<<48) -1);
    assert(input.length==8);
    
    for(int i=0,j=0; i<input.length-2; i++,j+=8) {
      long l = input[i];
      buf[j]   = (byte) (l & 0xFF); l >>= 8;
      buf[j+1] = (byte) (l & 0xFF); l >>= 8;
      buf[j+2] = (byte) (l & 0xFF); l >>= 8;
      buf[j+3] = (byte) (l & 0xFF); l >>= 8;     
      buf[j+4] = (byte) (l & 0xFF); l >>= 8;
      buf[j+5] = (byte) (l & 0xFF);
    }
    for(int i=input.length-2, j=6; i<input.length; i++, j+=24) {
      long l = input[i]; 
      buf[j]    = (byte) (l & 0xFF); l >>= 8;
      buf[j+1]  = (byte) (l & 0xFF); l >>= 8;
      buf[j+8] =  (byte) (l & 0xFF); l >>= 8;
      buf[j+9] =  (byte) (l & 0xFF); l >>= 8;     
      buf[j+16] = (byte) (l & 0xFF); l >>= 8;
      buf[j+17] = (byte) (l & 0xFF);
    }
    super.write(buf);
    for(long l: varint_buf)
      writeVarInt64(l);
  }
  
  private void writeScheme64(long input[], int patch_offset, int varints) throws IOException {
    byte [] buf = new byte [48];
    assert(input.length==6);
    for(int i=0,j=0; i<input.length; i++,j+=8) {
      long l = input[i];
      buf[j]   = (byte) (l & 0xFF); l >>= 8;
      buf[j+1] = (byte) (l & 0xFF); l >>= 8;
      buf[j+2] = (byte) (l & 0xFF); l >>= 8;
      buf[j+3] = (byte) (l & 0xFF); l >>= 8;     
      buf[j+4] = (byte) (l & 0xFF); l >>= 8;
      buf[j+5] = (byte) (l & 0xFF); l >>= 8;
      buf[j+6] = (byte) (l & 0xFF); l >>= 8;
      buf[j+7] = (byte) (l & 0xFF);
    }
    super.write(buf);
  }
  
  private void patchInput(long input[], int patch_offset, long[] varint_buf, long ceilling) {
    int v=0;
    if (varint_buf.length==0) return;
    
    // first patch
    varint_buf[v++] = input[patch_offset];
    int j = patch_offset;
    
    for (int i=j+1; i<input.length; i++) {
      if (input[i] > ceilling) {
        LOG.debug("found " + input[i] + " > " + ceilling);
        varint_buf[v++] = input[i];
        input[j] = i-j;
        j=i;
      }
    }
    assert(v==varint_buf.length);
    input[j]=0;
  }
  
  
  private void writeVarInt64(long i) throws IOException {
    long masked;
    int pos=0;
    boolean seen = false;
    for (int shift : mask_shift_64) {
      masked = i & (0x7FL << shift);
      if (seen || masked != 0) {
        varint_buf_64[pos++] = (byte) ((masked >>>= shift) | 0x80);
        seen = true;
      }
    }
    long val = i & 0x7F;
    //printHex(val);
    varint_buf_64[pos++] = (byte) val;
    super.write(varint_buf_64, 0, pos);
    //System.out.println("------");
  }
  
  public static void calculateBits(long [] input) {
    int [] panelties = new int [9];
    int [] varints = new int[9];
    
    for (int i = 0; i<9; i++) {
      final int scheme = schemes[i];
      final int end = 384/scheme;
      final long scheme_limit = scheme==64? Long.MAX_VALUE : (1L<<scheme) -1;
      panelties[i] = 0;
      varints[i] = 0;
      for (int j=0; j<end; j++) {
        if (input[j] > scheme_limit) {
          System.out.println(input[j] + ">" + scheme_limit);
          panelties[i] += varIntPenalty(input[j]);
          varints[i]++;
        }
      }
    }
    for (int i=0; i<9; i++) {
      final double ints = 384/schemes[i];
      final double rate = (16.0+384.0+panelties[i])/ints;
      System.out.printf("scheme %02d: rate: %5.5f, varints: %03d\n", schemes[i], rate, varints[i]);
    }
    
  }
  
  public static int varIntPenalty(long input) {
    long masked;
    for (int i=0; i< 10; i++) {
      int shift = mask_shift_64[i];
      masked = input & (0x7f << shift);
      if (masked != 0) {
        return (10-i)<<3;
      }
    }
    return -1; // should never happen
  }
}
