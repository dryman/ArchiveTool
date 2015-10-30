package org.idryman.tool.index;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.commons.codec.binary.Hex;
import org.junit.Test;

import static org.junit.Assert.*;

public class DictVarintInputStreamTest {

  @Test
  public void test() throws IOException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(bos);
    DictVarintOutputStream dvos = new DictVarintOutputStream(dos);
    long [] longs = new long[]{3,3,3,4,4,1,1,2,3,4,4,1,1,4,4};
    for (long l : longs)
      dvos.writeLong(l);
    dvos.close();
    System.out.println(Hex.encodeHexString(bos.toByteArray()));
    System.out.println(dos.size());
    
    ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
    DictVarintInputStream dvis = new DictVarintInputStream(bis);
    
    
    for (int i=0; i<longs.length; i++) {
      System.out.println(dvis.readLong());
    }
    dvis.close();
  }

}
