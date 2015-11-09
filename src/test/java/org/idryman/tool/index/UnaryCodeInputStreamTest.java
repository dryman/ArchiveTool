package org.idryman.tool.index;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class UnaryCodeInputStreamTest {
  private Random random;
  private int bound;
  private int length;
  
  @Parameters
  public static Collection<Object[]> testCases() {
    return Arrays.asList(new Object[][] {
        {1, 8, 500}, {2, 16, 300}, {3, 64, 50}, {4, 31, 400}, {5, 512, 300}
    });
  }
  
  public UnaryCodeInputStreamTest(int seed, int bound, int length) {
    this.random = new Random(seed);
    this.bound = bound;
    this.length = length;
  }
  
  @Test
  public void test() throws IOException {
    int [] input  = new int [length];
    for (int i=0; i<input.length; i++) {      
      input[i] = random.nextInt(bound);
    }
    
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(bos);
    UnaryCodeOutputStream uos = new UnaryCodeOutputStream(dos);
    
    for (int i: input) {
      uos.writeInt(i);
    }
    
    uos.close();
    dos.close();
    
    Assert.assertEquals(uos.estimateBytes(input), dos.size());
    ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
    UnaryCodeInputStream uis = new UnaryCodeInputStream(bis);

    for (int i=0; i<input.length; i++) {
      Assert.assertEquals(input[i], uis.readInt());
    }
    uis.close();
  }

}
