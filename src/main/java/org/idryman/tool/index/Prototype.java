package org.idryman.tool.index;

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Random;

import org.apache.commons.io.input.BoundedInputStream;
import org.idryman.compression.protocol.StreamProtos.IntegerStream;
import org.idryman.compression.protocol.StreamProtos.Stream;

public class Prototype {
  private static Random random = new Random(1);
  
  public static void varint() throws IOException {
    ByteArrayOutputStream bos1 = new ByteArrayOutputStream();
    ByteArrayOutputStream bos2 = new ByteArrayOutputStream();
    VarIntOutputStream     vios1 = new VarIntOutputStream(bos1);
    DiffVarIntOutputStream vios2 = new DiffVarIntOutputStream(bos2);
    
    int j = random.nextInt() & 0x3FFF;
    for (int i=0; i<30; i++) {
      j += random.nextInt() & 0x3FFF;
      System.out.println(j);
      //printHex(j);
      vios1.writeInt(j);
      vios2.writeInt(j);
    }
    vios1.close();
    vios2.close();
    
    System.out.println("===============");
    
    ByteArrayInputStream bis1 = new ByteArrayInputStream(bos1.toByteArray());
    ByteArrayInputStream bis2 = new ByteArrayInputStream(bos2.toByteArray());
    
    VarIntInpustStream    viis1 = new VarIntInpustStream(bis1);
    DiffVarIntInputStream viis2 = new DiffVarIntInputStream(bis2);
    
    int buf [] = new int[10];
    int len;
    while((len = viis1.readInts(buf, 0, 10))!=-1) {
      for (int i =0; i<len; i++) {
        System.out.println(buf[i]);
      }
    }
    viis1.close();
    System.out.println("===============");
    
    while((len = viis2.readInts(buf, 0, 10))!=-1) {
      for (int i =0; i<len; i++) {
        System.out.println(buf[i]);
      }
    }
    viis2.close();
    
    System.out.println("===============");
    
    System.out.println(bos1.size());
    System.out.println(bos2.size());
  }
  
  public static void main(String[] args) throws IOException {
    ByteArrayOutputStream bos1 = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(bos1);
    VarIntOutputStream     vios1 = new VarIntOutputStream(new BufferedOutputStream(dos));
    int j = random.nextInt() & 0x3FFF;
    for (int i=0; i<30; i++) {
      j += random.nextInt() & 0x3FFF;
      //System.out.println(j);
      vios1.writeInt(j);
    }
    vios1.flush();
    long offset = dos.size();
    System.out.println(offset);
    Stream stream = Stream.newBuilder()
        .setLength(dos.size())
        .setKind(Stream.Kind.INTEGER)
        .setIntegerStream(IntegerStream.newBuilder()
            .setKind(IntegerStream.Kind.INT_32)
            .setEncoding(IntegerStream.Encoding.VARINT_SORTED))
            .build();
    stream.writeTo(dos);
    System.out.println(dos.size());
    dos.writeLong(offset);
    System.out.println(dos.size());
    vios1.close();
    
    byte[] raw = bos1.toByteArray();
    ByteArrayInputStream bis1 = new ByteArrayInputStream(raw);
    bis1.skip(raw.length-8);
    long r_offset = new DataInputStream(bis1).readLong();
    System.out.println(r_offset);
    bis1.reset();
    bis1.skip(r_offset);
    
    Stream s = Stream.parseFrom(new BoundedInputStream(bis1, raw.length-8-r_offset));
    System.out.println(s);
  }

}
