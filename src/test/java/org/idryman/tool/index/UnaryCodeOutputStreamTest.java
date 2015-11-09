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
public class UnaryCodeOutputStreamTest {
  private int [] input;
  private int length;
  private byte[] output;
  private ByteArrayOutputStream bos;
  private DataOutputStream dos;
  private UnaryCodeOutputStream uos;

  @Parameters
  public static Collection<Object[]> testCases() {
    return Arrays.asList(new Object[][] {
        // zero case    
        // 0000 0001
        {new int[]{0}, 1, new byte[]{0x1}},
        // normal input
        // 0000 0001 0001 0010
        {new int[]{1,2,3}, 2, new byte[]{0x12, 0x1}},
        // bit that matches the edge
        // 1000 0100 0000 0001 0000 0000
        {new int[]{8,9,4}, 3, new byte[]{0x0, 0x1, (byte) 0x84}},
        // bit exceeds the edge
        // 0000 0001 0000 0000
        {new int[]{8}, 2, new byte[]{0x0,0x1}},
        // 0011 0110
        {new int[]{1,0,1,0}, 1, new byte[]{ 0x36 }}
    });
  }
  
  public UnaryCodeOutputStreamTest(int [] input, int length, byte[] output) {
    this.input = input;
    this.length = length;
    this.output = output;
  }
  
  @Before
  public void setUp() throws Exception {
    bos = new ByteArrayOutputStream();
    dos = new DataOutputStream(bos);
    uos = new UnaryCodeOutputStream(dos);
  }

  @Test
  public void testEstimatedLength() {
    assertEquals(length, uos.estimateBytes(input));
  }
  
  @Test
  public void testExpectedLength() throws IOException {
    for(int i:input) uos.writeInt(i);
    uos.close();
    assertEquals(length, dos.size());
  }
  
  @Test
  public void testEncodedOutput() throws IOException {
    for(int i:input) uos.writeInt(i);
    uos.close();
    dos.close();
    assertArrayEquals(output, bos.toByteArray());
  }

}
