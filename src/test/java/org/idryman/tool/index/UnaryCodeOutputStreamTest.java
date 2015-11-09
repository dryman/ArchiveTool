package org.idryman.tool.index;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

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
    List<Object[]> asList = Arrays.asList(new Object[][] {
        // zero case
        {new int[]{0}, 1, new byte[]{0x1}},
        // normal input
        {new int[]{1,2,3}, 2, new byte[]{0x12, 0x1}},
        // byte that match the edge
        {new int[]{8,9,4}, 3, new byte[]{0x0, 0x1, (byte) 0x84}},
        // byte exceed the edge
        {new int[]{8}, 2, new byte[]{0x0,0x1}}
    });
    return asList;
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
