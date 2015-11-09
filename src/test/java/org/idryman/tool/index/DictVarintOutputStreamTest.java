package org.idryman.tool.index;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class DictVarintOutputStreamTest {
  private long [] input;
  private int length;
  private long [] dict;
  private byte[] output;
  private ByteArrayOutputStream bos;
  private DataOutputStream dos;
  private DictVarintOutputStream dvos;
  
  @Parameters
  public static Collection<Object[]> testCases() {
    return Arrays.asList(new Object[][] {
        // 00001111 11100000
        {new long[]{32,32,32,32}, 2, new long[] {32}, new byte[]{(byte)0xE0, 0x0F}},
        // 00110110 11100000 10000001
        {new long[]{33,1,33,1}, 3, new long[] {1, 33}, new byte[]{(byte)0x81,(byte)0xE0, 0x36}},
        // 00000011 11000000 00000000x8 00000010
        {new long[]{0x8000_0000_0000_0000L, 0x8000_0000_0000_0000L}, 11, new long [] {0x8000_0000_0000_0000L} , new byte[]{0x02, 0,0,0,0,0,0,0,0, (byte)0xc0, 0x03}} 
    });
  }
  
  public DictVarintOutputStreamTest(long [] input, int length, long[] dict, byte[] output) {
    this.input = input;
    this.length = length;
    this.dict = dict;
    this.output = output;
  }
  
  @Before
  public void setUp() throws Exception {
    bos = new ByteArrayOutputStream();
    dos = new DataOutputStream(bos);
    dvos = new DictVarintOutputStream(dos);
  }

  @Test
  public void testEstimatedLength() {
    assertEquals(length, dvos.estimateBytes(input));
  }
  
  @Test
  public void testDict() {
    assertArrayEquals(dict, dvos.createDict(input));
  }
  
  @Test
  public void testExpectedLength() throws IOException {
    for(long i:input) dvos.writeLong(i);
    dvos.close();
    assertEquals(length, dos.size());
  }
  
  @Test
  public void testEncodedOutput() throws IOException {
    for(long i:input) dvos.writeLong(i);
    dvos.close();
    dos.close();
    assertArrayEquals(output, bos.toByteArray());
  }
}
