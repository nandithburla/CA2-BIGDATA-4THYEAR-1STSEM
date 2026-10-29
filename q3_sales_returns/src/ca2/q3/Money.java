package ca2.q3;

import java.math.BigDecimal;
import java.math.RoundingMode;

/** Exact money arithmetic: prices are parsed to integer cents, so sums do not depend on the order of addition. */
public final class Money {
  private Money() { }

  public static long parseCents(String s) {
    return new BigDecimal(s.trim()).setScale(2, RoundingMode.HALF_UP).movePointRight(2).longValueExact();
  }

  public static String format(long cents) {
    long a = Math.abs(cents);
    return (cents < 0 ? "-" : "") + (a / 100) + "." + (a % 100 < 10 ? "0" : "") + (a % 100);
  }

  /** RFC-4180 style escaping for the CSV outputs. */
  public static String csv(String s) {
    if (s.indexOf(',') < 0 && s.indexOf('"') < 0 && s.indexOf('\n') < 0) return s;
    return '"' + s.replace("\"", "\"\"") + '"';
  }
}
