package ca2.q2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

/**
 * One posting (doc_id, tf, positions) - the value that travels through the shuffle.
 * Serialised form: Text(doc_id), VInt(tf), VInt(number of positions), then the positions as VInts.
 * With delta (gap) encoding switched on, the positions are written as gaps:  [3, 10, 25] is stored as [3, 7, 15].
 * Encoding (q2.encoding, set by the driver from --delta on|off|fixed):  delta = gap VInts (default, the assignment's requirement),
 * vint = absolute VInts (experiment: "without delta encoding"),  fixed = absolute 4-byte ints (naive baseline).
 * The mode is a static because Writables are created by the framework without a Configuration; every Mapper/Reducer sets it in setup();
 * one job always uses one value.
 */
public class PostingWritable implements Writable {
  public static final int DELTA = 0, VINT = 1, FIXED = 2;
  public static int mode = DELTA;

  public static void setMode(String name) {
    mode = "vint".equals(name) ? VINT : "fixed".equals(name) ? FIXED : DELTA;
  }

  private String docId = "";
  private int tf;
  private int n;
  private int[] pos = new int[8];

  public PostingWritable() { }

  public void set(String docId, int tf) { this.docId = docId; this.tf = tf; this.n = 0; }
  public void addPosition(int p) {
    if (n == pos.length) pos = java.util.Arrays.copyOf(pos, n * 2);
    pos[n++] = p;
  }
  public String getDocId() { return docId; }
  public int getTf() { return tf; }
  public int positionCount() { return n; }
  public int position(int i) { return pos[i]; }
  public int[] positionsCopy() { return java.util.Arrays.copyOf(pos, n); }

  @Override
  public void write(DataOutput out) throws IOException {
    Text.writeString(out, docId);
    WritableUtils.writeVInt(out, tf);
    WritableUtils.writeVInt(out, n);
    int prev = 0;
    for (int i = 0; i < n; i++) {
      if (mode == FIXED) out.writeInt(pos[i]);
      else WritableUtils.writeVInt(out, mode == DELTA ? pos[i] - prev : pos[i]);
      prev = pos[i];
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    docId = Text.readString(in);
    tf = WritableUtils.readVInt(in);
    n = WritableUtils.readVInt(in);
    if (pos.length < n) pos = new int[Math.max(n, pos.length * 2)];
    int prev = 0;
    for (int i = 0; i < n; i++) {
      int v = mode == FIXED ? in.readInt() : WritableUtils.readVInt(in);
      pos[i] = mode == DELTA ? prev + v : v;
      prev = pos[i];
    }
  }
}
