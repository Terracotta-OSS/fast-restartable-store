/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */
package com.terracottatech.frs.io.nio;

import com.terracottatech.frs.io.Chunk;
import com.terracottatech.frs.io.HeapBufferSource;
import com.terracottatech.frs.io.WrappingChunk;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 * @author mscott
 */
public class NIOSpeedTest {
  
  NIOStreamImpl stream;
  long lsn = 0;
  static AtomicLong reads = new AtomicLong();
  
  public static void main(String[] args) {
    try {
      NIOSpeedTest test = new NIOSpeedTest(args[0]);
      long time = System.currentTimeMillis();
      long len = 0;
      
      for (int c=0;c<Integer.parseInt(args[1]);c++) {
        test.readThread();
      } 
      int total = 1024;
      if ( args.length > 2 ) {
        total *= Integer.parseInt(args[2]);
        System.out.println("total counts: " + total);
      }
      for (int x=0;x<total;x++) {
        len += test.writeAndSync(2,512,1024);
      }
      time = (System.currentTimeMillis() - time);
      System.out.println("total write: " + len);
      System.out.println("total read: " + reads.get());
      System.out.println("total time: " + time);
      System.out.println("write rate: " + ((len*1000)/(time*1024*1024)));
    } catch (IOException ioe) {
      ioe.printStackTrace();
    }

  }
  
  NIOSpeedTest(String path) throws IOException {
    File f = new File(path);
    stream = new NIOStreamImpl(f, NIOAccessMethod.getDefault(), 64 * 1024 * 1024, 64 * 1024 * 1024, new HeapBufferSource(512*1024*1024));
  }
  
  void readThread() {
    Thread t = new Thread() {
      public void run() {
        while ( true ) {
//          reads.addAndGet(stream.readRandom(1024).remaining());
        }
      }
    };
    t.setDaemon(true);
    t.start();
  }
  
  public int writeAndSync(int multi,int count,int size) throws IOException {
    int len = 0;
    for (int x=0;x<multi;x++) {
      len += stream.append(formChunk(count,size),lsn++);
    }
    stream.sync();
    return len;
  }
  
  public Chunk formChunk(int count, int size) {
    ByteBuffer buffer = ByteBuffer.allocate(size * count);
    for (int x=0;x<count * size;x++) {
      buffer.put((byte)(256 * Math.random()));
    }
    buffer.flip();
    return new WrappingChunk(buffer);
  }
}
