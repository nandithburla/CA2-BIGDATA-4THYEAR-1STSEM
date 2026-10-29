package ca2.q3;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Comparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

/**
 * One Top-N candidate (product, net units, net revenue in cents).
 * BEST_FIRST is a STRICT TOTAL ORDER: revenue desc, then units desc, then product_id asc. Because product_id is unique inside a
 * (category, month) group, compare() returns 0 only for the very same product - two different products with identical revenue and units
 * are therefore never merged by a TreeSet/TreeMap (the classic "TreeMap key collision" bug of keying by revenue only).
 */
public class Candidate implements Writable {
  public static final Comparator<Candidate> BEST_FIRST = (a, b) -> {
    if (a.cents != b.cents) return Long.compare(b.cents, a.cents);
    if (a.units != b.units) return Long.compare(b.units, a.units);
    return a.product.compareTo(b.product);
  };

  String product = "";
  long units, cents;

  public Candidate() { }
  public Candidate(String product, long units, long cents) { this.product = product; this.units = units; this.cents = cents; }
  public Candidate copy() { return new Candidate(product, units, cents); }
  public String getProduct() { return product; }
  public long getUnits() { return units; }
  public long getCents() { return cents; }

  @Override public void write(DataOutput out) throws IOException { Text.writeString(out, product); WritableUtils.writeVLong(out, units); WritableUtils.writeVLong(out, cents); }
  @Override public void readFields(DataInput in) throws IOException { product = Text.readString(in); units = WritableUtils.readVLong(in); cents = WritableUtils.readVLong(in); }
}
