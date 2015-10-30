package org.idryman.tool.index;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Random;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.base.Preconditions;

public class UnaryCodeInputStreamTest {
  private static Random random = new Random(1);
  
  @Test
  public void test() throws IOException {
    int [] input  = new int [300];
    for (int i=0; i<input.length; i++) {      
      input[i] = random.nextInt(32) & Integer.MAX_VALUE;
      //System.out.println(input[i]);
    }
    
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(bos);
    UnaryCodeOutputStream uos = new UnaryCodeOutputStream(dos);
    //uos.writeInts(input);
    
    for (int i: input) {
      uos.writeInt(i);
    }
    
    uos.close();
    System.out.println("-----------");
    System.out.println(dos.size());
    System.out.println("-----------");
    Preconditions.checkState(uos.estimateBytes(input) == dos.size());
    
    ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
    UnaryCodeInputStream uis = new UnaryCodeInputStream(bis);

    for (int i=0; i<input.length; i++) {
      Assert.assertEquals(input[i], uis.readInt());
    }
    uis.close();
  }

}
