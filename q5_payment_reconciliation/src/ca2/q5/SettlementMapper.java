package ca2.q5;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/** bank_settlements.csv: settlement_id,txn_id,settled_amount_inr,settlement_date -> key (txn_id, MANY), value "settlement_id,settled_amount_inr,settlement_date". */
public class SettlementMapper extends Mapper<Object, Text, TaggedKey, Text> {
  private final TaggedKey key = new TaggedKey();
  private final Text val = new Text();

  @Override
  protected void map(Object offset, Text value, Context ctx) throws IOException, InterruptedException {
    String line = value.toString().trim();
    if (line.isEmpty()) return;
    String[] f = line.split(",", -1);
    if (f[0].equals("settlement_id")) { ctx.getCounter(TxnJoinCounters.SETTLEMENT_HEADERS_SKIPPED).increment(1); return; }
    ctx.getCounter(TxnJoinCounters.SETTLEMENT_LINES_READ).increment(1);
    try {
      if (f.length != 4 || f[0].trim().isEmpty() || f[1].trim().isEmpty()) throw new IllegalArgumentException();
      Money.inrCentsToMicro(f[2]);                           // validates the number
      key.set(f[1].trim(), TaggedKey.MANY);
      val.set(f[0].trim() + "," + f[2].trim() + "," + f[3].trim());
      ctx.write(key, val);
    } catch (RuntimeException e) {
      ctx.getCounter(TxnJoinCounters.BAD_SETTLEMENT_LINES).increment(1);
    }
  }
}
