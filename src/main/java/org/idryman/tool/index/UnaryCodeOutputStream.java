package org.idryman.tool.index;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Random;

import org.apache.commons.codec.binary.Hex;

import com.google.common.base.Preconditions;

public class UnaryCodeOutputStream extends FilterOutputStream{
  private static Random random = new Random(1);
  
  public UnaryCodeOutputStream(OutputStream out) {
    super(out);
  }
  
  public void writeInts(int [] numbers) throws IOException {
    int ps=0, idx=0;
    byte buf [] = new byte [getBytesForInts(numbers)];    
    for (int n: numbers) {
      n+=ps;
      ps=0;
      while(n >= 8) {
        idx++;
        n-=8;
      }
      //System.out.println(n);
      buf[idx] |= (byte)(1 << n);
      ps = n+1;
      if (ps==8){
        idx++;
        ps=0;
      }
    }
    super.write(buf);
  }
  public int getBytesForInts(int [] numbers) {
    int sum = 0, ret;
    for (int n : numbers) {
      sum += n+1;
    }
    ret = sum >>> 3;
    return sum - (ret << 3) == 0 ? ret : ret+1;
  }

  public static void main(String[] args) throws IOException {
    int [] input  = new int [10000];
    int [] output = new int [input.length];
    for (int i=0; i<input.length; i++) {      
      input[i] = random.nextInt(32) & Integer.MAX_VALUE;
      //System.out.println(input[i]);
    }
    
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(bos);
    UnaryCodeOutputStream uos = new UnaryCodeOutputStream(dos);
    uos.writeInts(input);
    uos.close();
    System.out.println("-----------");
    System.out.println(dos.size());
    System.out.println("-----------");
    
    ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
    UnaryCodeInputStream uis = new UnaryCodeInputStream(bis);
    uis.readInts(output, 0, output.length);
//    for(int i:output) {
//      System.out.println(i);
//    }
    uis.close();
    //System.out.println(Hex.encodeHexString(bos.toByteArray()));
    //System.out.println(bos.toByteArray().toString());
    for (int i=0; i<input.length; i++) {
      Preconditions.checkState(input[i]==output[i]);
    }
  }

}
