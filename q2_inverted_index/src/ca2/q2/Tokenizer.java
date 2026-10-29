package ca2.q2;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Tokenisation rules of the assignment: lower-case, split on every non-alphanumeric character, keep tokens of length >= 2, drop stop-words.
 * Position policy (q2.positions):
 *   kept (default) - positions are numbered over the token stream that is left AFTER filtering (title first, position 0)
 *   raw            - positions are numbered over all tokens before filtering (stop-words leave gaps)
 */
public final class Tokenizer {
  private Tokenizer() { }

  /** A token with its position. */
  public static final class Tok {
    public final String term; public final int pos; public final boolean title;
    Tok(String t, int p, boolean ti) { term = t; pos = p; title = ti; }
  }

  /** Split lower-cased text into alphanumeric runs. */
  public static List<String> split(String text) {
    List<String> out = new ArrayList<>();
    String s = text.toLowerCase(java.util.Locale.ROOT);
    int start = -1;
    for (int i = 0; i < s.length(); i++) {
      boolean alnum = Character.isLetterOrDigit(s.charAt(i));
      if (alnum && start < 0) start = i;
      if (!alnum && start >= 0) { out.add(s.substring(start, i)); start = -1; }
    }
    if (start >= 0) out.add(s.substring(start));
    return out;
  }

  /** Tokenise title then body as ONE stream. counts[0..4] += raw, short, stopword, kept, titleKept. */
  public static List<Tok> tokenize(String title, String body, Set<String> stop, boolean rawPositions, long[] counts) {
    List<Tok> out = new ArrayList<>();
    int pos = 0;
    for (int part = 0; part < 2; part++) {
      boolean isTitle = part == 0;
      for (String t : split(isTitle ? title : body)) {
        counts[0]++;
        boolean drop = false;
        if (t.length() < 2) { counts[1]++; drop = true; }
        else if (stop.contains(t)) { counts[2]++; drop = true; }
        if (drop) { if (rawPositions) pos++; continue; }
        counts[3]++; if (isTitle) counts[4]++;
        out.add(new Tok(t, pos++, isTitle));
      }
    }
    return out;
  }
}
