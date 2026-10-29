package ca2.q3;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/** Job 2 key: (category, order month yyyy-MM, product_id). */
public class CatMonthProductKey implements WritableComparable<CatMonthProductKey> {
  private final Text category = new Text(), month = new Text(), product = new Text();

  public CatMonthProductKey() { }
  public void set(String c, String m, String p) { category.set(c); month.set(m); product.set(p); }
  public Text getCategory() { return category; }
  public Text getMonth() { return month; }
  public Text getProduct() { return product; }

  @Override public void write(DataOutput out) throws IOException { category.write(out); month.write(out); product.write(out); }
  @Override public void readFields(DataInput in) throws IOException { category.readFields(in); month.readFields(in); product.readFields(in); }
  @Override
  public int compareTo(CatMonthProductKey o) {
    int c = category.compareTo(o.category);
    if (c != 0) return c;
    c = month.compareTo(o.month);
    return c != 0 ? c : product.compareTo(o.product);
  }
  @Override public boolean equals(Object o) { return o instanceof CatMonthProductKey && compareTo((CatMonthProductKey) o) == 0; }
  @Override public int hashCode() { return (category.hashCode() * 31 + month.hashCode()) * 31 + product.hashCode(); }
  @Override public String toString() { return category + "," + month + "," + product; }
}
