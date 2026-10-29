package ca2.q5;

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * Exact arithmetic: amounts are integer cents, FX rates integer 1e-4 units, so  cents x rate  is an exact count of micro-INR (1e-6 INR).
 * Sums of micro-INR are therefore independent of the order in which records arrive.
 */
public final class Money {
  private Money() { }
  public static final long MICRO = 1_000_000L;

  public static long cents(String s) {
    long c = new BigDecimal(s.trim()).setScale(2, RoundingMode.HALF_UP).movePointRight(2).longValueExact();
    return c;
  }
  public static long rate1e4(String s) { return new BigDecimal(s.trim()).setScale(4, RoundingMode.HALF_UP).movePointRight(4).longValueExact(); }
  /** settled_amount_inr (INR, 2 decimals) -> micro-INR */
  public static long inrCentsToMicro(String s) { return cents(s) * 10_000L; }

  /** "123.456789" for 123_456_789 micro (sign handled). */
  public static String micro(long m) {
    long a = Math.abs(m);
    return (m < 0 ? "-" : "") + (a / MICRO) + "." + String.format("%06d", a % MICRO);
  }

  /** inverse of micro(): "123.456789" -> 123456789 */
  public static long parseMicro(String s) { return new BigDecimal(s.trim()).movePointRight(6).setScale(0, RoundingMode.HALF_UP).longValueExact(); }
}
