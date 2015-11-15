package org.idryman.tool.index;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;

import org.apache.log4j.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

public class Pfor384DictVarintOutputStream extends FilterOutputStream implements LongOutputStream{
  private final static Logger LOG = Logger.getLogger(Pfor384DictVarintOutputStream.class);
  private final static int [] schemes = {4,6,8,12,16,24,32,48,64};
  private final long [] buffer;
  private final byte [] byteBuf = new byte[48];
  private int posIn = 0, posOut=0;
  private int decision, penalty, patchOffset, exceptions;
  private long exception_buf[][] = new long[9][96];
  private int exception_len[] = new int[9];
  private DictVarintOutputStream dvos;
  
  public Pfor384DictVarintOutputStream(OutputStream out) {
    this(out, 8192);
  }
  
  public Pfor384DictVarintOutputStream(OutputStream out, int size) {
    super(out);
    buffer = new long[size];
    dvos = new DictVarintOutputStream(out);
  }

  @Override
  public synchronized void writeLong(long number) throws IOException {
    if (posIn >= buffer.length) {
      adjustBuffer();
    }
    buffer[posIn++] = number;
    if (posIn-posOut >= 96) {
      flushGroupInts();
    }
  }
  
  private void flushGroupInts() throws IOException {
    makeDecision();
    Preconditions.checkState(decision!=-1, "In flush group int it should not use DictVarInt");
    writeWithScheme();
  }
  
  private void flushRemaining() throws IOException {
    while(true) {
      makeDecision();
      if (decision != -1) {
        writeWithScheme();
      } else {
        // FIXME write dictvarint using different header
        break;
      }
    }
  }
  
  @Override
  public synchronized void close() throws IOException {
    flushRemaining();
    super.close();
  }
  
  private void adjustBuffer() {
    LOG.info("adjust buffer");
    for(int i=posOut, j=0; i<posIn; i++, j++) {
      buffer[j] = buffer[i];
    }
    posIn = posIn-posOut;
    posOut=0;
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
  @VisibleForTesting
  void makeDecision() {
    final int pos = posOut;
    final int limit = posIn;
    final int [] penalties = new int [9];
    final int [] varints = new int[9];
    final int [] patch_offset = new int [9];
    
    boolean considerDictVarInt = false;
    
    // Scheme4
    {
      final int scheme4 = schemes[0];
      final int end4 = 384/scheme4+pos;
      final long scheme4_limit = (1L<<scheme4)-1;
      boolean isInfinitePenalty = false;
      penalties[0] = 0;
      varints[0] = 0;
      exception_len[0] = 0;
      
      if (end4 > limit) {
        penalties[0] = Integer.MAX_VALUE;
        considerDictVarInt = true;
      } else {  
        // scheme 4 is special case.
        int diff_pos=-1;
        for (int j=pos; j<end4; j++) {
          if (buffer[j] > scheme4_limit) {
            
            // Case 1, first seen outlier. 
            // The maximum allowed offset is 62.
            if (diff_pos==-1) {
              if (j-pos > 62) {
                isInfinitePenalty = true;
                break;
              }
              patch_offset[0] = j-diff_pos;
              diff_pos = j;
              exception_buf[0][exception_len[0]++] = buffer[j];
              continue;
            }
            
            // Case 2, not first seen. 
            // Maximum difference is 15
            if (j-diff_pos > scheme4_limit) {
              isInfinitePenalty = true;
              break;
            }
            
            diff_pos = j;
            exception_buf[0][exception_len[0]++] = buffer[j];
          }
        }
        if (isInfinitePenalty || exception_len[0] > 32) {
          penalties[0] = Integer.MAX_VALUE;
        } else {
          penalties[0] = dvos.estimateBytes(Arrays.copyOf(exception_buf[0], exception_len[0]));
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
        considerDictVarInt = true;
        continue;
      }
      penalties[i] = 0;
      varints[i] = 0;
      exception_len[i] = 0;
      boolean first_patch=true;
      for (int j=pos; j<end; j++) {
        if (buffer[j] > scheme_limit) {
          exception_buf[i][exception_len[i]++] = buffer[j];
          varints[i]++;
          if (first_patch) {
            patch_offset[i] = j-pos;
            first_patch = false;
          }
        }
      }
      penalties[i] = exception_len[i] > 128/scheme ? 
          Integer.MAX_VALUE : 
            dvos.estimateBytes(Arrays.copyOf(exception_buf[i], exception_len[i]));
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
    
    if (considerDictVarInt) {
      int varint_penalty = dvos.estimateBytes(Arrays.copyOfRange(buffer, pos, limit));
      double rate = (8.0+varint_penalty)/(limit-pos);
      
      if (rate < lowest_rate) {
        LOG.debug("using varint");
        this.penalty = varint_penalty;
        this.exceptions = limit-pos;
        // No need to set offset. This is not group int.
        decision = -1;
        return;
      }
    }
    
    LOG.info("using scheme: "+schemes[decision] + " with rate: " + (int)lowest_rate + " extra bytes "+ penalties[decision] + 
        " patch_offset " + patch_offset[decision] + " and varints " + exception_len[decision]);
    this.penalty     = penalties[decision];
    this.patchOffset = patch_offset[decision];
    this.exceptions     = exception_len[decision];
  }
  
  private void writeWithScheme() throws IOException {
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
    // I think we don't need to calculate this again... 
    final long [] exception_buf = new long [exceptions];
    if (exceptions>0) {
      final int scheme = schemes[decision];
      final long ceilling = (1L << scheme) -1;
      final int limit = posOut + (384/scheme);
      final int offset = posOut+patchOffset;
      int v=0;
      
      // first patch
      exception_buf[v++] = buffer[offset];
      LOG.debug("found " + buffer[offset] + " > " + ceilling);
      int j = offset;
      
      for (int i=j+1; i<limit; i++) {
        if (buffer[i] > ceilling) {
          LOG.debug("found " + buffer[i] + " > " + ceilling);
          exception_buf[v++] = buffer[i];
          buffer[j] = i-j;
          j=i;
        }
      }
      buffer[j]=0;
      Preconditions.checkState(v==exception_buf.length, "patch input not matching calculcated length");
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
    for (long l: exception_buf) {
      dvos.writeLong(l);
    }
    dvos.flush();
       
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
  


  @Override
  public int estimateBytes(long[] numbers) {
 // TODO do this in the future. Maybe we can revisit when 
 //we move towards page-based buffer instead of block-based one. 
    throw new UnsupportedOperationException(); 
  }

}
