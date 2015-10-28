package org.idryman.tool.index;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;

public class Pfor384OutputStream extends FilterOutputStream{
  private final static Logger LOG = Logger.getLogger(Pfor384OutputStream.class);
  private final static int [] schemes = {4,6,8,12,16,24,32,48,64};
  private final static int [] mask_shift_64 = {70,63,56,49,42,35,28,21,14,7};
  private byte varint_buf_64[] = new byte[11];
  private final long [] buffer = new long[8192];
  private final byte [] byteBuf = new byte[48];
  private int posIn = 0, posOut=0;
  private int penalty, patchOffset, varints;
  
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
    writeWithScheme(buffer, decision);
    LOG.assertLog(decision != -1, "decision should not be -1");
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
        writeWithScheme(buffer, decision);
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
    
    // Rest of the schemes except 64
    for (int i = 1; i<8; i++) {
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
        this.penalty = varint_penalty;
        this.varints = limit-pos;
        // No need to set offset. This is not group int.
        return -1;
      }
    }
    
    LOG.info("using scheme: "+schemes[decision] + " with rate: " + (int)lowest_rate + " penalty "+ penalties[decision] + 
        " patch_offset " + patch_offset[decision] + " and varints " + varints[decision]);
    this.penalty     = penalties[decision];
    this.patchOffset = patch_offset[decision];
    this.varints     = varints[decision];
    return decision;
  }
  
  private void writeWithScheme(long input[], int decision) throws IOException {
    final int penalty = this.penalty;
    final int patch_offset = this.patchOffset;
    
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
    
    // patch input
    final long [] varint_buf = new long [varints];
    if (varints>0) {
      final int scheme = schemes[decision];
      final long ceilling = (1L << scheme) -1;
      final int limit = posOut + (384/scheme);
      final int offset = posOut+patchOffset;
      int v=0;
      
      // first patch
      varint_buf[v++] = buffer[offset];
      LOG.debug("found " + buffer[offset] + " > " + ceilling);
      int j = offset;
      
      for (int i=j+1; i<limit; i++) {
        if (buffer[i] > ceilling) {
          LOG.debug("found " + buffer[i] + " > " + ceilling);
          varint_buf[v++] = buffer[i];
          buffer[j] = i-j;
          j=i;
        }
      }
      buffer[j]=0;
      Preconditions.checkState(v==varint_buf.length, "patch input not matching calculcated length");
    }
    
    switch(decision) {
    case 0:
      writeScheme4(); break;
    case 1:
      writeScheme6(); break;
    case 2:
      writeScheme8(); break;
    case 3:
      writeScheme12(); break;
    case 4:
      writeScheme16(); break;
    case 5:
      writeScheme24(); break;
    case 6:
      writeScheme32(); break;
    case 7:
      writeScheme48(); break;
    default:
      writeScheme64();
    }
    
    // TODO: varint seems expansive. May need to consider other options like simple8b
    for (long l: varint_buf)
      writeVarInt64(l);
       
    // Make it 16bit aligned
    if (has_tail) {
      super.write(0);
    }
  }
  
  private void writeScheme4() throws IOException {
    final int last_pos_out = this.posOut;
    
    // 3 iterations. Each takes 32 ints
    for (int i=0; i<48; i+=16) {
      // Inner loop take 16 * 2 ints
      for(int j=0; j<16; j++) {
        byteBuf[i+j] = (byte)(buffer[posOut+j] | (buffer[posOut+j+16] << 4));
      }
      posOut+=32;
    }
    Preconditions.checkState(posOut-last_pos_out==96, "scheme4 take 96 integers");
    super.write(byteBuf);
  }
  
  private void writeScheme6() throws IOException {
    final int last_pos_out = this.posOut;
    
    // 48 iterations
    for(int i=0; i<48; i++) {
      byteBuf[i] = (byte)buffer[posOut++];
    }
    // 16 left
    for(int i=0; i<16; i++) {
      byte b = (byte) buffer[posOut++];
      byteBuf[i]    |= (b & 0x03) << 6;
      byteBuf[i+16] |= (b & 0x0C) << 4;
      byteBuf[i+32] |= (b & 0x30) << 2;
    }
    Preconditions.checkState(posOut-last_pos_out==64, "scheme6 takes 64 integers");
    super.write(byteBuf);
  }
  
  private void writeScheme8() throws IOException {
    final int last_pos_out = this.posOut;

    // 48 iterations
    for(int i=0; i<48; i++) {
      byteBuf[i] = (byte) buffer[posOut++];
    }

    Preconditions.checkState(posOut-last_pos_out==48, "scheme8 takes 48 integers");
    super.write(byteBuf);
  }
  
  private void writeScheme12() throws IOException {
    final int last_pos_out = this.posOut;

    // 24 iterations
    for(int i=0; i<48; i+=2) {
      long l = buffer[posOut++];
      byteBuf[i]   = (byte)(l & 0xFF); l>>>=8;
      byteBuf[i+1] = (byte)(l & 0x0F);
    }
    // 8 left. Fill the high byte in the half word
    for(int i=1; i<16; i+=2) {
      long l = buffer[posOut++];
      byteBuf[i]    |= (byte) ((l & 0x0F) << 4); l>>>=4;
      byteBuf[i+16] |= (byte) ((l & 0x0F) << 4); l>>>=4;
      byteBuf[i+32] |= (byte) ((l & 0x0F) << 4);
    }
    Preconditions.checkState(posOut-last_pos_out==32, "scheme12 takes 32 integers");
    super.write(byteBuf);
  }
  
  private void writeScheme16() throws IOException {
    final int last_pos_out = this.posOut;

    // 24 iterations
    for(int i=0; i<48; i+=2) {
      long l = buffer[posOut++];
      byteBuf[i]   = (byte) (l & 0xFF); l>>>=8;
      byteBuf[i+1] = (byte) (l & 0xFF);
    }
    Preconditions.checkState(posOut-last_pos_out==24, "scheme16 takes 24 integers");
    super.write(byteBuf);
  }
  
  private void writeScheme24() throws IOException {
    final int last_pos_out = this.posOut;
    
    // 12 iterations
    for(int i=0; i<48; i+=4) {
      long l = buffer[posOut++];
      byteBuf[i]   = (byte) (l & 0xFF); l>>>=8;
      byteBuf[i+1] = (byte) (l & 0xFF); l>>>=8;
      byteBuf[i+2] = (byte) (l & 0xFF);
    }
    // 4 left. Fill the most significant byte in the word
    for(int i=3; i<16; i+=4) {
      long l = buffer[posOut++];
      byteBuf[i]    = (byte) (l & 0xFF); l>>>=8;
      byteBuf[i+16] = (byte) (l & 0xFF); l>>>=8;
      byteBuf[i+32] = (byte) (l & 0xFF);; 
    }
    Preconditions.checkState(posOut-last_pos_out==16, "scheme24 takes 16 integers");
    super.write(byteBuf);
  }
  
  private void writeScheme32() throws IOException {
    final int last_pos_out = this.posOut;

    // 12 iterations
    for(int i=0; i<48; i+=4) {
      long l = buffer[posOut++];
      byteBuf[i]   = (byte) (l & 0xFF); l>>>=8;
      byteBuf[i+1] = (byte) (l & 0xFF); l>>>=8;
      byteBuf[i+2] = (byte) (l & 0xFF); l>>>=8;
      byteBuf[i+3] = (byte) (l & 0xFF);
    }
    Preconditions.checkState(posOut-last_pos_out==12, "scheme32 takes 12 integers");
    super.write(byteBuf);
  }
  
  private void writeScheme48() throws IOException {
    final int last_pos_out = this.posOut;

    // 6 iterations
    for(int i=0; i<48; i+=8) {
      long l = buffer[posOut++];
      byteBuf[i]   = (byte) (l & 0xFF); l>>>=8;
      byteBuf[i+1] = (byte) (l & 0xFF); l>>>=8;
      byteBuf[i+2] = (byte) (l & 0xFF); l>>>=8;
      byteBuf[i+3] = (byte) (l & 0xFF); l>>>=8;     
      byteBuf[i+4] = (byte) (l & 0xFF); l>>>=8;
      byteBuf[i+5] = (byte) (l & 0xFF);
    }
    // 2 left. Fill the 2 most significant bytes in double word
    for(int i=6; i<48; i+=24) {
      long l = buffer[posOut++]; 
      byteBuf[i]    = (byte) (l & 0xFF); l >>= 8;
      byteBuf[i+1]  = (byte) (l & 0xFF); l >>= 8;
      byteBuf[i+8] =  (byte) (l & 0xFF); l >>= 8;
      byteBuf[i+9] =  (byte) (l & 0xFF); l >>= 8;     
      byteBuf[i+16] = (byte) (l & 0xFF); l >>= 8;
      byteBuf[i+17] = (byte) (l & 0xFF);
    }
    Preconditions.checkState(posOut-last_pos_out==8, "scheme48 takes 8 integers");
    super.write(byteBuf);
  }
  
  private void writeScheme64() throws IOException {
    final int last_pos_out = this.posOut;
    // 6 iterations
    for(int i=0; i<48; i+=8) {
      long l = buffer[posOut++];
      byteBuf[i]   = (byte) (l & 0xFF); l >>= 8;
      byteBuf[i+1] = (byte) (l & 0xFF); l >>= 8;
      byteBuf[i+2] = (byte) (l & 0xFF); l >>= 8;
      byteBuf[i+3] = (byte) (l & 0xFF); l >>= 8;     
      byteBuf[i+4] = (byte) (l & 0xFF); l >>= 8;
      byteBuf[i+5] = (byte) (l & 0xFF); l >>= 8;
      byteBuf[i+6] = (byte) (l & 0xFF); l >>= 8;
      byteBuf[i+7] = (byte) (l & 0xFF);
    }
    Preconditions.checkState(posOut-last_pos_out==6, "scheme64 takes 6 integers");
    super.write(byteBuf);
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
  
  
  private static int varIntPenalty(long input) {
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
