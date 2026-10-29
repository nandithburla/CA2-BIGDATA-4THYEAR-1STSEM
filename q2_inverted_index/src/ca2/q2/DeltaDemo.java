package ca2.q2;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Self-contained demonstration of the gap encoding (needs no cluster):   hadoop jar q2_inverted_index.jar ca2.q2.DeltaDemo
 * Shows [3, 10, 25] being stored as [3, 7, 15], a lossless round trip, and the serialized size in the three encodings.
 * The second block uses SYNTHETIC positions of a long document to show where gaps pay off (VInt needs 1 byte for 0..127,
 * 2 bytes for 128..255, 3 bytes up to 65535) - it is not part of the dataset.
 */
public final class DeltaDemo {
  private DeltaDemo() { }

  public static void main(String[] args) throws IOException {
    int[] example = {3, 10, 25};
    PostingWritable.setMode("delta");
    System.out.println("positions " + java.util.Arrays.toString(example) + " are stored as gaps " + java.util.Arrays.toString(gaps(example)));
    report("example [3,10,25]", example);
    int[] longDoc = new int[40];
    for (int i = 0; i < longDoc.length; i++) longDoc[i] = 1000 + i * 37;   // synthetic: 40 occurrences in a document of ~2500 tokens
    report("synthetic long document, 40 positions from 1000 step 37", longDoc);
    PostingWritable.setMode("delta");
  }

  private static int[] gaps(int[] p) {
    int[] g = new int[p.length];
    for (int i = 0; i < p.length; i++) g[i] = p[i] - (i == 0 ? 0 : p[i - 1]);
    return g;
  }

  private static void report(String title, int[] positions) throws IOException {
    System.out.println("-- " + title);
    for (String m : new String[] {"delta", "vint", "fixed"}) {
      PostingWritable.setMode(m);
      PostingWritable p = new PostingWritable();
      p.set("D000123", 3);
      for (int x : positions) p.addPosition(x);
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      p.write(new DataOutputStream(bos));
      PostingWritable q = new PostingWritable();
      q.readFields(new DataInputStream(new ByteArrayInputStream(bos.toByteArray())));
      boolean same = java.util.Arrays.equals(q.positionsCopy(), positions) && q.getDocId().equals("D000123") && q.getTf() == 3;
      System.out.printf("   encoding %-5s -> %3d bytes, round trip %s%n", m, bos.size(), same ? "OK" : "FAILED");
      if (!same) System.exit(1);
    }
  }
}
