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
public class DictVarintInputStreamTest {
  private Random random;
  private int bound;
  private int length;
  
  @Parameters
  public static Collection<Object[]> testCases() {
    return Arrays.asList(new Object[][] {
        {1, 800, 500}, {2, 160, 300}, {3, 640000, 50}, {4, 3100000, 400}, {5, 512000000, 300}
    });
  }
  
  public DictVarintInputStreamTest(int seed, int bound, int length) {
    this.random = new Random(seed);
    this.bound = bound;
    this.length = length;
  }

  @Test
  public void test() throws IOException {
    long [] input  = new long [length];
    for (int i=0; i<input.length; i++) { 
      input[i] = random.nextInt(bound);
    }
    
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(bos);
    DictVarintOutputStream dvos = new DictVarintOutputStream(dos);
    for (long l : input) {
      dvos.writeLong(l);
    }
    dvos.close();
    dos.close();

    Assert.assertEquals(dvos.estimateBytes(input), dos.size());
    System.out.println(length*8.0/dos.size());
    
    ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
    DictVarintInputStream dvis = new DictVarintInputStream(bis);
    
    
    for (int i=0; i<input.length; i++) {
      Assert.assertEquals(input[i], dvis.readLong());
    }
    dvis.close();
  }

}
