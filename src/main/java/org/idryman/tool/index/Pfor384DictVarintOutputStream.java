package org.idryman.tool.index;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;

import org.apache.log4j.Logger;

import com.google.common.annotations.VisibleForTesting;

public class Pfor384DictVarintOutputStream extends FilterOutputStream implements LongOutputStream{
  private final static Logger LOG = Logger.getLogger(Pfor384DictVarintOutputStream.class);
  private final static int [] schemes = {4,6,8,12,16,24,32,48,64};
  private final long [] buffer;
  private final byte [] byteBuf = new byte[48];
  private int posIn = 0, posOut=0;
  private int penalty, patchOffset, varints;
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
  public void writeLong(long number) throws IOException {
    if (posIn >= buffer.length) {
      adjustBuffer();
    }
    buffer[posIn++] = number;
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
  int makeDecision() {
    final int pos = posOut;
    final int limit = posIn;
    final int [] penalties = new int [9];
    final int [] varints = new int[9];
    final int [] patch_offset = new int [9];
    
    boolean considerDictVarInt = false;
    int decision=0;
    
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
            
            // Otherwise calculate the penalty and proceed
            diff_pos = j;
            exception_buf[0][exception_len[0]++] = buffer[j];
          }
        }
        if (isInfinitePenalty) {
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
      penalties[i] = dvos.estimateBytes(Arrays.copyOf(exception_buf[i], exception_len[i]));
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
        this.varints = limit-pos;
        // No need to set offset. This is not group int.
        return -1;
      }
    }
    
    LOG.info("using scheme: "+schemes[decision] + " with rate: " + (int)lowest_rate + " penalty "+ penalties[decision] + 
        " patch_offset " + patch_offset[decision] + " and varints " + exception_len[decision]);
    this.penalty     = penalties[decision];
    this.patchOffset = patch_offset[decision];
    this.varints     = exception_len[decision];
    return decision;
  }
  
  
  
  private void flushGroupInts() throws IOException {
    
    final int decision = 0;//makeDecision(paramRef);
    //writeWithScheme(buffer, decision);
    LOG.assertLog(decision != -1, "decision should not be -1");
  }

  @Override
  public int estimateBytes(long[] numbers) {
 // TODO do this in the future. Maybe we can revisit when 
 //we move towards page-based buffer instead of block-based one. 
    throw new UnsupportedOperationException(); 
  }

}
