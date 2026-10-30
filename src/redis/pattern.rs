//! Redis glob-style pattern matching.
//!
//! `KEYS`, `SCAN`/`SSCAN`/`HSCAN ... MATCH`, `PSUBSCRIBE`, `PUBLISH` and
//! `PUBSUB CHANNELS` all share one informally specified glob dialect (see
//! <https://redis.io/docs/latest/commands/keys/>). This module is the single
//! home for it: callers depend on the [`PatternMatcher`] trait and build a
//! [`GlobPattern`] once per command rather than re-parsing per candidate.
//!
//! ## Dialect
//!
//! - `*`  — any sequence of bytes, including the empty one
//! - `?`  — exactly one byte
//! - `[abc]`, `[a-z]` — one byte from a set / range. Only `^` negates
//!   (`[^a-z]`); `!` is an ordinary member. A reversed range such as `[z-a]`
//!   is read as `[a-z]`
//! - `\x` — the literal byte `x` (inside or outside a class)
//!
//! Matching is byte-oriented, so binary names work and no UTF-8 is assumed.
//!
//! ## Deliberate divergence from C Redis
//!
//! Redis reads pattern and input as `char`, so its class *ranges* with bytes
//! `>= 0x80` as an endpoint (`[a-\xff]`) depend on whether the platform's
//! `char` is signed. Here bytes are always unsigned, so the result is the same
//! on every platform. Everything else, including high bytes as plain class
//! members or literals, agrees with Redis.
//!
//! ## Redis quirks that are deliberately preserved
//!
//! The matcher is a port of Redis's `stringmatchlen()` (`src/util.c`), so the
//! odd corners agree with a real server:
//!
//! - the empty string is matched only by the empty pattern; even `*` does not
//!   match it. Redis's callers special-case a lone `*`, which is what
//!   [`GlobPattern::keyspace`] does for the key-iterating commands
//! - an unterminated class (`[abc`) runs to the end of the pattern
//! - a trailing lone `\` matches a literal backslash
//! - backtracking is bounded (`skipLongerMatches`) so `*a*a*a*b` is not
//!   exponential, and more than [`MAX_STAR_NESTING`] nested `*`s never match

use bytes::Bytes;

/// Something that can decide whether a name (key, field, member, channel...)
/// is selected by a command's `MATCH`/pattern argument.
pub trait PatternMatcher {
    /// Whether `candidate` is selected by this pattern.
    fn is_match(&self, candidate: &[u8]) -> bool;
}

/// Longest chain of nested `*` expansions tried before giving up (as Redis).
pub const MAX_STAR_NESTING: usize = 1000;

/// A compiled Redis glob pattern.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GlobPattern {
    pattern: Bytes,
    nocase: bool,
    /// A lone `*` that selects *everything*, empty names included.
    select_all: bool,
}

impl GlobPattern {
    /// A pattern with exactly the semantics of Redis's `stringmatchlen()`.
    /// This is what the pub/sub commands use: a `*` subscriber is not
    /// delivered messages for the empty channel name.
    pub fn new(pattern: impl Into<Bytes>) -> Self {
        Self {
            pattern: pattern.into(),
            nocase: false,
            select_all: false,
        }
    }

    /// As [`GlobPattern::new`], except that a lone `*` selects every name,
    /// including the empty one — the way `KEYS` and the `*SCAN` commands
    /// treat it in Redis.
    pub fn keyspace(pattern: impl Into<Bytes>) -> Self {
        let pattern = pattern.into();
        let select_all = pattern.as_ref() == b"*";
        Self {
            pattern,
            nocase: false,
            select_all,
        }
    }

    /// Compare ASCII-case-insensitively. No Invar command needs this today
    /// (Redis matches key/channel names case-sensitively), but the underlying
    /// matcher supports it, so it is kept available and tested.
    pub fn with_nocase(mut self, nocase: bool) -> Self {
        self.nocase = nocase;
        self
    }

    /// The raw pattern bytes.
    pub fn as_bytes(&self) -> &[u8] {
        &self.pattern
    }
}

impl PatternMatcher for GlobPattern {
    fn is_match(&self, candidate: &[u8]) -> bool {
        self.select_all || glob_match(&self.pattern, candidate, self.nocase)
    }
}

/// Byte-level port of Redis's `stringmatchlen()`.
fn glob_match(pattern: &[u8], string: &[u8], nocase: bool) -> bool {
    let mut skip_longer_matches = false;
    match_at(pattern, string, nocase, &mut skip_longer_matches, 0)
}

fn match_at(
    mut pat: &[u8],
    mut s: &[u8],
    nocase: bool,
    skip_longer_matches: &mut bool,
    nesting: usize,
) -> bool {
    // Protection against abusive patterns.
    if nesting > MAX_STAR_NESTING {
        return false;
    }
    let fold = |b: u8| if nocase { b.to_ascii_lowercase() } else { b };

    while !pat.is_empty() && !s.is_empty() {
        match pat[0] {
            b'*' => {
                while pat.len() > 1 && pat[1] == b'*' {
                    pat = &pat[1..];
                }
                if pat.len() == 1 {
                    return true;
                }
                while !s.is_empty() {
                    if match_at(&pat[1..], s, nocase, skip_longer_matches, nesting + 1) {
                        return true;
                    }
                    if *skip_longer_matches {
                        return false;
                    }
                    s = &s[1..];
                }
                // The rest of the pattern matches nowhere in the rest of the
                // string, so no earlier `*` can help by consuming more.
                *skip_longer_matches = true;
                return false;
            }
            b'?' => {
                s = &s[1..];
                pat = &pat[1..];
            }
            b'[' => {
                pat = &pat[1..];
                let negate = pat.first() == Some(&b'^');
                if negate {
                    pat = &pat[1..];
                }
                let mut matched = false;
                loop {
                    match pat.first().copied() {
                        Some(b'\\') if pat.len() >= 2 => {
                            // An escaped member; Redis compares it case-sensitively.
                            pat = &pat[1..];
                            if pat[0] == s[0] {
                                matched = true;
                            }
                            pat = &pat[1..];
                        }
                        Some(b']') => {
                            pat = &pat[1..];
                            break;
                        }
                        // Unterminated class: stop at the end of the pattern.
                        None => break,
                        Some(first) => {
                            if pat.len() >= 3 && pat[1] == b'-' {
                                let (mut start, mut end) = (first, pat[2]);
                                if start > end {
                                    std::mem::swap(&mut start, &mut end);
                                }
                                let c = fold(s[0]);
                                if c >= fold(start) && c <= fold(end) {
                                    matched = true;
                                }
                                pat = &pat[3..];
                            } else {
                                if fold(first) == fold(s[0]) {
                                    matched = true;
                                }
                                pat = &pat[1..];
                            }
                        }
                    }
                }
                if matched == negate {
                    return false;
                }
                s = &s[1..];
            }
            c => {
                // `\x` matches `x`; a trailing lone `\` matches itself.
                if c == b'\\' && pat.len() >= 2 {
                    pat = &pat[1..];
                }
                if fold(pat[0]) != fold(s[0]) {
                    return false;
                }
                s = &s[1..];
                pat = &pat[1..];
            }
        }
        if s.is_empty() {
            while pat.first() == Some(&b'*') {
                pat = &pat[1..];
            }
            break;
        }
    }
    pat.is_empty() && s.is_empty()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn m(pat: &str, s: &str) -> bool {
        GlobPattern::new(Bytes::copy_from_slice(pat.as_bytes())).is_match(s.as_bytes())
    }

    fn mi(pat: &str, s: &str) -> bool {
        GlobPattern::new(Bytes::copy_from_slice(pat.as_bytes()))
            .with_nocase(true)
            .is_match(s.as_bytes())
    }

    #[test]
    fn star_matches_anything_but_the_empty_string() {
        // Redis's matcher never matches the empty string against `*`; KEYS and
        // SCAN special-case a lone `*` themselves, pub/sub does not (a `*`
        // pattern subscriber gets nothing published to the empty channel).
        assert!(!m("*", ""));
        assert!(m("*", "hello"));
        assert!(m("*", "hello world"));
        assert!(m("h*llo", "hllo"));
        assert!(m("h*llo", "heeeello"));
        assert!(!m("h*llo", "world"));
    }

    #[test]
    fn keyspace_star_selects_the_empty_name_too() {
        let p = GlobPattern::keyspace("*");
        assert!(p.is_match(b""));
        assert!(p.is_match(b"anything"));
        // Only a *lone* `*` is special; the rest is stringmatchlen() as-is.
        assert!(!GlobPattern::keyspace("**").is_match(b""));
        assert!(GlobPattern::keyspace("a*").is_match(b"abc"));
        assert!(!GlobPattern::keyspace("").is_match(b"a"));
        assert!(GlobPattern::keyspace("").is_match(b""));
    }

    #[test]
    fn question_mark_matches_single_byte() {
        assert!(m("h?llo", "hello"));
        assert!(!m("h?llo", "hllo"));
        assert!(!m("h?llo", "heello"));
        assert!(m("?", "a"));
        assert!(!m("?", ""));
        assert!(!m("?", "ab"));
    }

    #[test]
    fn character_class_basic() {
        assert!(m("[abc]", "a"));
        assert!(m("[abc]", "b"));
        assert!(m("[abc]", "c"));
        assert!(!m("[abc]", "d"));
        assert!(!m("[abc]", ""));
        assert!(!m("[abc]", "ab")); // class matches exactly one char
    }

    #[test]
    fn character_class_range() {
        assert!(m("[a-z]", "a"));
        assert!(m("[a-z]", "z"));
        assert!(m("[a-z]", "m"));
        assert!(!m("[a-z]", "A"));
        assert!(!m("[a-z]", "0"));
        assert!(m("[0-9]", "5"));
        assert!(!m("[0-9]", "a"));
    }

    #[test]
    fn character_class_negation_caret() {
        assert!(!m("[^abc]", "a"));
        assert!(!m("[^abc]", "b"));
        assert!(m("[^abc]", "d"));
        assert!(m("[^a-z]", "A"));
        assert!(!m("[^a-z]", "m"));
    }

    #[test]
    fn bang_is_an_ordinary_class_member_not_a_negation() {
        // Only `^` negates in Redis: `[!a]` is the set {'!', 'a'}.
        assert!(m("[!a]", "!"));
        assert!(m("[!a]", "a"));
        assert!(!m("[!a]", "b"));
    }

    #[test]
    fn escapes_inside_classes() {
        assert!(m("[\\]]", "]"));
        assert!(m("[\\^a]", "^"));
        assert!(m("[a\\-z]", "-"));
        assert!(!m("[a\\-z]", "m"));
        assert!(m("[\\\\]", "\\"));
    }

    #[test]
    fn reversed_ranges_are_swapped_and_malformed_ranges_follow_redis() {
        assert!(m("[z-a]", "m"));
        assert!(!m("[z-a]", "0"));
        // `[a-]` is read as the range 'a'..']' (swapped to ']'..'a'), so it
        // matches 'a' and the bytes between ']' and 'a'.
        assert!(m("[a-]", "a"));
        assert!(m("[a-]", "_"));
        assert!(!m("[a-]", "b"));
        // An unterminated class takes what it has and then the pattern ends
        // (checked against redis-server 8.6.2).
        assert!(m("a[b", "ab"));
        assert!(m("[a", "a"));
        assert!(!m("[", "a"));
        assert!(!m("a[", "ab"));
    }

    #[test]
    fn trailing_stars_match_once_the_string_is_used_up() {
        assert!(m("ab**", "ab"));
        assert!(m("a*b*", "ab"));
        assert!(!m("**", ""));
    }

    /// Without Redis's `skipLongerMatches` bound this takes exponential time.
    #[test]
    fn pathological_patterns_are_matched_in_bounded_time() {
        let start = std::time::Instant::now();
        let long = "a".repeat(64);
        assert!(!m("a*a*a*a*a*a*a*a*a*a*a*a*b", &long));
        assert!(
            start.elapsed() < std::time::Duration::from_secs(1),
            "took {:?}",
            start.elapsed()
        );
    }

    #[test]
    fn backslash_escaping() {
        assert!(m("h\\*llo", "h*llo"));
        assert!(!m("h\\*llo", "hello"));
        assert!(m("h\\?llo", "h?llo"));
        assert!(!m("h\\?llo", "hello"));
        assert!(m("\\[a\\]", "[a]"));
        // A trailing lone backslash is a literal backslash.
        assert!(m("a\\", "a\\"));
        assert!(!m("a\\", "a"));
    }

    #[test]
    fn multiple_stars_collapse() {
        assert!(m("a**b", "ab"));
        assert!(m("a**b", "aXb"));
        assert!(m("a***b", "aXXXb"));
        assert!(!m("a***b", "axyz"));
    }

    #[test]
    fn empty_pattern_matches_only_empty_string() {
        assert!(m("", ""));
        assert!(!m("", "a"));
    }

    #[test]
    fn exact_literal_match() {
        assert!(m("hello", "hello"));
        assert!(!m("hello", "hell"));
        assert!(!m("hello", "helloo"));
    }

    #[test]
    fn mixed_wildcards() {
        assert!(m("h?llo*world", "helloworld"));
        assert!(m("h?llo*world", "hello big world"));
        assert!(!m("h?llo*world", "hello"));
    }

    #[test]
    fn matching_is_case_sensitive_by_default() {
        // Redis matches key and channel names case-sensitively (issue #118).
        assert!(!m("News.*", "news.sport"));
        assert!(!m("[A-Z]", "a"));
        assert!(m("News.*", "News.sport"));
    }

    #[test]
    fn nocase_flag() {
        assert!(mi("hello", "HELLO"));
        assert!(mi("H?LLO", "hello"));
        assert!(mi("[A-Z]", "a"));
        assert!(!mi("[A-Z]", "0"));
        // Escaped class members stay case-sensitive, as in Redis.
        assert!(!mi("[\\a]", "A"));
    }

    #[test]
    fn binary_names_are_matched_bytewise() {
        let p = GlobPattern::new(&b"\xff?\x00*"[..]);
        assert!(p.is_match(b"\xff\x80\x00tail"));
        assert!(!p.is_match(b"\xfe\x80\x00tail"));
        assert!(GlobPattern::new(&b"[\x80-\xff]"[..]).is_match(b"\xc3"));
    }

    #[test]
    fn redis_real_world_patterns() {
        // From Redis's own test suite.
        assert!(m("foo*", "foobar"));
        assert!(m("foo*", "foo"));
        assert!(!m("foo*", "foa"));
        assert!(m("*bar*", "foobar"));
        assert!(m("*bar*", "bar"));
        assert!(m("*bar*", "barfoo"));
        assert!(!m("*bar*", "foo"));
        assert!(m("foo*bar", "fooXbar"));
        assert!(m("foo*bar", "foobar"));
        assert!(!m("foo*bar", "foobaz"));
        assert!(m("*x*", "axb"));
        assert!(!m("*x*", "abc"));
    }

    #[test]
    fn excessive_star_nesting_never_matches_and_does_not_blow_the_stack() {
        // Redis gives up (no match) past 1000 nested `*` expansions.
        let pattern = "*a".repeat(MAX_STAR_NESTING + 10);
        let input = "a".repeat(MAX_STAR_NESTING + 10);
        assert!(!m(&pattern, &input));
        // Comfortably under the limit still works.
        assert!(m(&"*a".repeat(100), &"a".repeat(100)));
    }

    // -----------------------------------------------------------------------
    // Fuzz / property tests
    //
    // Deterministic (seeded) so failures reproduce; each failure message
    // carries the pattern and input. `redis_differential` additionally checks
    // against a real `redis-server` when one is available.
    // -----------------------------------------------------------------------

    /// xorshift64*: no external RNG dependency and stable across versions.
    struct Rng(u64);

    impl Rng {
        fn new(seed: u64) -> Self {
            Self(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1)
        }

        fn next(&mut self) -> u64 {
            self.0 ^= self.0 >> 12;
            self.0 ^= self.0 << 25;
            self.0 ^= self.0 >> 27;
            self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
        }

        fn below(&mut self, n: usize) -> usize {
            (self.next() % n as u64) as usize
        }

        fn pick<T: Copy>(&mut self, items: &[T]) -> T {
            items[self.below(items.len())]
        }
    }

    /// Small alphabet heavy on metacharacters so interesting interactions
    /// (escapes inside classes, `-` ranges, `^`, `]` ...) are frequent.
    const PATTERN_ALPHABET: &[u8] = b"abcAB-^!]\\[*?*[a-c-\x00\xff";
    const INPUT_ALPHABET: &[u8] = b"abcABC-^!][\\*?\x00\xff";

    fn random_bytes(rng: &mut Rng, alphabet: &[u8], max_len: usize) -> Vec<u8> {
        let len = rng.below(max_len + 1);
        (0..len).map(|_| rng.pick(alphabet)).collect()
    }

    // ---- Independent reference implementation ----------------------------
    //
    // A different algorithm from the production matcher: tokenise the pattern
    // the way Redis reads it, then run a plain dynamic-programming match. No
    // recursion, no `skipLongerMatches`, so agreement is meaningful.

    #[derive(Debug)]
    enum Token {
        Star,
        Any,
        Byte(u8),
        Class { negate: bool, members: Vec<Member> },
    }

    #[derive(Debug)]
    enum Member {
        /// Compared case-insensitively under `nocase`.
        Byte(u8),
        /// An escaped member: always compared exactly.
        Exact(u8),
        Range(u8, u8),
    }

    fn tokenize(pat: &[u8]) -> Vec<Token> {
        let mut tokens = Vec::new();
        let mut i = 0;
        while i < pat.len() {
            match pat[i] {
                b'*' => {
                    if !matches!(tokens.last(), Some(Token::Star)) {
                        tokens.push(Token::Star);
                    }
                    i += 1;
                }
                b'?' => {
                    tokens.push(Token::Any);
                    i += 1;
                }
                b'[' => {
                    i += 1;
                    let negate = pat.get(i) == Some(&b'^');
                    if negate {
                        i += 1;
                    }
                    let mut members = Vec::new();
                    while i < pat.len() {
                        if pat[i] == b'\\' && i + 1 < pat.len() {
                            members.push(Member::Exact(pat[i + 1]));
                            i += 2;
                        } else if pat[i] == b']' {
                            i += 1;
                            break;
                        } else if i + 2 < pat.len() && pat[i + 1] == b'-' {
                            members.push(Member::Range(pat[i], pat[i + 2]));
                            i += 3;
                        } else {
                            members.push(Member::Byte(pat[i]));
                            i += 1;
                        }
                    }
                    tokens.push(Token::Class { negate, members });
                }
                b'\\' if i + 1 < pat.len() => {
                    tokens.push(Token::Byte(pat[i + 1]));
                    i += 2;
                }
                c => {
                    tokens.push(Token::Byte(c));
                    i += 1;
                }
            }
        }
        tokens
    }

    fn token_matches(token: &Token, c: u8, nocase: bool) -> bool {
        let fold = |b: u8| if nocase { b.to_ascii_lowercase() } else { b };
        match token {
            Token::Star => unreachable!("handled by the DP"),
            Token::Any => true,
            Token::Byte(b) => fold(*b) == fold(c),
            Token::Class { negate, members } => {
                let hit = members.iter().any(|m| match *m {
                    Member::Byte(b) => fold(b) == fold(c),
                    Member::Exact(b) => b == c,
                    Member::Range(a, b) => {
                        let (lo, hi) = if a > b { (b, a) } else { (a, b) };
                        fold(c) >= fold(lo) && fold(c) <= fold(hi)
                    }
                });
                hit != *negate
            }
        }
    }

    fn oracle(pat: &[u8], s: &[u8], nocase: bool) -> bool {
        // Redis quirk: the empty string only ever matches the empty pattern.
        if s.is_empty() {
            return pat.is_empty();
        }
        let tokens = tokenize(pat);
        let n = s.len();
        // dp[j]: tokens[..i] matches s[..j]
        let mut dp = vec![false; n + 1];
        dp[0] = true;
        for token in &tokens {
            let mut next = vec![false; n + 1];
            if matches!(token, Token::Star) {
                let mut seen = false;
                for j in 0..=n {
                    seen |= dp[j];
                    next[j] = seen;
                }
            } else {
                for j in 1..=n {
                    next[j] = dp[j - 1] && token_matches(token, s[j - 1], nocase);
                }
            }
            dp = next;
        }
        dp[n]
    }

    fn production(pat: &[u8], s: &[u8], nocase: bool) -> bool {
        GlobPattern::new(Bytes::copy_from_slice(pat))
            .with_nocase(nocase)
            .is_match(s)
    }

    #[test]
    fn fuzz_agrees_with_reference_implementation() {
        let mut rng = Rng::new(0xC0FFEE);
        for _ in 0..200_000 {
            let pat = random_bytes(&mut rng, PATTERN_ALPHABET, 10);
            let input = random_bytes(&mut rng, INPUT_ALPHABET, 8);
            let nocase = rng.below(4) == 0;
            assert_eq!(
                production(&pat, &input, nocase),
                oracle(&pat, &input, nocase),
                "pattern={pat:?} input={input:?} nocase={nocase}"
            );
        }
    }

    /// Patterns derived from the input are far likelier to match than
    /// independent random ones, so this exercises the "true" side.
    #[test]
    fn fuzz_agrees_with_reference_on_patterns_derived_from_the_input() {
        let mut rng = Rng::new(0xBADCAFE);
        let mut matches = 0;
        for _ in 0..100_000 {
            let input = random_bytes(&mut rng, INPUT_ALPHABET, 10);
            let mut pat = Vec::new();
            for &b in &input {
                match rng.below(6) {
                    0 => pat.push(b'?'),
                    1 => pat.push(b'*'),
                    2 => pat.extend_from_slice(&[b'[', b, b']']),
                    3 => pat.extend_from_slice(&[b'\\', b]),
                    4 => pat.extend_from_slice(&[b'[', b'^', b, b']']),
                    _ => pat.push(b),
                }
                if rng.below(8) == 0 {
                    pat.push(b'*');
                }
            }
            let got = production(&pat, &input, false);
            assert_eq!(
                got,
                oracle(&pat, &input, false),
                "pattern={pat:?} input={input:?}"
            );
            matches += got as usize;
        }
        assert!(
            matches > 1_000,
            "generator produced too few matches: {matches}"
        );
    }

    fn escape(s: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        for &b in s {
            if matches!(b, b'*' | b'?' | b'[' | b']' | b'\\' | b'^' | b'-') {
                out.push(b'\\');
            }
            out.push(b);
        }
        out
    }

    #[test]
    fn fuzz_an_escaped_string_matches_exactly_itself() {
        let mut rng = Rng::new(7);
        for _ in 0..50_000 {
            let s = random_bytes(&mut rng, INPUT_ALPHABET, 12);
            let other = random_bytes(&mut rng, INPUT_ALPHABET, 12);
            let pat = escape(&s);
            assert!(production(&pat, &s, false), "pat={pat:?} s={s:?}");
            assert_eq!(
                production(&pat, &other, false),
                s == other,
                "pat={pat:?} other={other:?}"
            );
        }
    }

    #[test]
    fn fuzz_star_sandwich_matches_substrings() {
        let mut rng = Rng::new(11);
        for _ in 0..50_000 {
            let hay = random_bytes(&mut rng, INPUT_ALPHABET, 14);
            if hay.is_empty() {
                continue;
            }
            let start = rng.below(hay.len());
            let end = start + rng.below(hay.len() - start + 1);
            let needle = &hay[start..end];

            let mut pat = vec![b'*'];
            pat.extend(escape(needle));
            pat.push(b'*');
            assert!(production(&pat, &hay, false), "pat={pat:?} hay={hay:?}");

            // Prefix / suffix forms.
            let mut prefix = escape(&hay[..end]);
            prefix.push(b'*');
            assert!(
                production(&prefix, &hay, false),
                "pat={prefix:?} hay={hay:?}"
            );
            let mut suffix = vec![b'*'];
            suffix.extend(escape(&hay[start..]));
            assert!(
                production(&suffix, &hay, false),
                "pat={suffix:?} hay={hay:?}"
            );
        }
    }

    #[test]
    fn fuzz_runs_of_stars_are_equivalent_to_one() {
        let mut rng = Rng::new(13);
        for _ in 0..50_000 {
            let input = random_bytes(&mut rng, INPUT_ALPHABET, 8);
            let head = escape(&random_bytes(&mut rng, INPUT_ALPHABET, 4));
            let tail = escape(&random_bytes(&mut rng, INPUT_ALPHABET, 3));
            let stars = b"*".repeat(2 + rng.below(4));
            let one = [head.as_slice(), b"*", tail.as_slice()].concat();
            let many = [head.as_slice(), stars.as_slice(), tail.as_slice()].concat();
            assert_eq!(
                production(&one, &input, false),
                production(&many, &input, false),
                "one={one:?} many={many:?} input={input:?}"
            );
        }
    }

    #[test]
    fn fuzz_class_and_negated_class_partition_single_bytes() {
        let mut rng = Rng::new(17);
        for _ in 0..20_000 {
            // A well-formed class body: no `]`, `\`, or leading `^`.
            let body_alphabet = b"abcxyzABC019!-_";
            let mut body = random_bytes(&mut rng, body_alphabet, 6);
            // A trailing `-` would swallow the closing `]` as a range end.
            while body.last() == Some(&b'-') {
                body.pop();
            }
            if body.is_empty() {
                continue;
            }
            let positive = [b"[".as_slice(), &body, b"]"].concat();
            let negative = [b"[^".as_slice(), &body, b"]"].concat();
            for c in 0u8..=255 {
                assert_ne!(
                    production(&positive, &[c], false),
                    production(&negative, &[c], false),
                    "class={positive:?} byte={c}"
                );
            }
        }
    }

    #[test]
    fn fuzz_never_panics_on_arbitrary_bytes() {
        let mut rng = Rng::new(19);
        for _ in 0..100_000 {
            let pat: Vec<u8> = (0..rng.below(12)).map(|_| rng.next() as u8).collect();
            let input: Vec<u8> = (0..rng.below(12)).map(|_| rng.next() as u8).collect();
            let nocase = rng.below(2) == 0;
            assert_eq!(
                production(&pat, &input, nocase),
                oracle(&pat, &input, nocase),
                "pattern={pat:?} input={input:?} nocase={nocase}"
            );
        }
    }

    #[test]
    fn fuzz_star_heavy_patterns_finish_quickly() {
        // The shapes that make a naive backtracking matcher exponential.
        let mut rng = Rng::new(23);
        let start = std::time::Instant::now();
        for _ in 0..500 {
            let segments = 4 + rng.below(20);
            let mut pat = Vec::new();
            for _ in 0..segments {
                pat.push(b'*');
                pat.push(rng.pick(b"ab"));
            }
            pat.push(b'c');
            let input = vec![b'a'; 20 + rng.below(200)];
            assert!(!production(&pat, &input, false), "pat={pat:?}");
        }
        assert!(
            start.elapsed() < std::time::Duration::from_secs(5),
            "took {:?}",
            start.elapsed()
        );
    }

    #[test]
    fn fuzz_keyspace_differs_from_new_only_for_a_lone_star() {
        let mut rng = Rng::new(29);
        for _ in 0..50_000 {
            let pat = random_bytes(&mut rng, PATTERN_ALPHABET, 6);
            let input = random_bytes(&mut rng, INPUT_ALPHABET, 6);
            let plain = GlobPattern::new(pat.clone()).is_match(&input);
            let keyspace = GlobPattern::keyspace(pat.clone()).is_match(&input);
            if pat == b"*" {
                assert!(keyspace);
            } else {
                assert_eq!(plain, keyspace, "pattern={pat:?} input={input:?}");
            }
        }
    }

    // ---- Differential test against a real redis-server -------------------

    mod redis_differential {
        use super::*;
        use std::io::{BufRead, BufReader, Read, Write};
        use std::net::{TcpListener, TcpStream};
        use std::process::{Child, Command, Stdio};
        use std::time::{Duration, Instant};

        struct Server {
            child: Child,
            conn: BufReader<TcpStream>,
        }

        impl Drop for Server {
            fn drop(&mut self) {
                let _ = self.child.kill();
                let _ = self.child.wait();
            }
        }

        enum Reply {
            /// Status or integer replies, whose values the test never reads.
            Other,
            Bulk(Vec<u8>),
            Array(Vec<Reply>),
        }

        impl Server {
            fn start() -> Option<Server> {
                let port = TcpListener::bind("127.0.0.1:0")
                    .ok()?
                    .local_addr()
                    .ok()?
                    .port();
                let child = Command::new("redis-server")
                    .args([
                        "--port",
                        &port.to_string(),
                        "--save",
                        "",
                        "--appendonly",
                        "no",
                    ])
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .spawn()
                    .ok()?;
                let deadline = Instant::now() + Duration::from_secs(5);
                let stream = loop {
                    match TcpStream::connect(("127.0.0.1", port)) {
                        Ok(s) => break s,
                        Err(_) if Instant::now() < deadline => {
                            std::thread::sleep(Duration::from_millis(25))
                        }
                        Err(_) => return None,
                    }
                };
                Some(Server {
                    child,
                    conn: BufReader::new(stream),
                })
            }

            fn cmd(&mut self, args: &[&[u8]]) -> Reply {
                let mut buf = format!("*{}\r\n", args.len()).into_bytes();
                for a in args {
                    buf.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
                    buf.extend_from_slice(a);
                    buf.extend_from_slice(b"\r\n");
                }
                self.conn.get_mut().write_all(&buf).unwrap();
                self.read_reply()
            }

            fn read_line(&mut self) -> Vec<u8> {
                let mut line = Vec::new();
                self.conn.read_until(b'\n', &mut line).unwrap();
                line.truncate(line.len() - 2);
                line
            }

            fn read_reply(&mut self) -> Reply {
                let line = self.read_line();
                let rest = String::from_utf8_lossy(&line[1..]).into_owned();
                match line[0] {
                    b'+' | b':' => Reply::Other,
                    b'$' => {
                        let n: i64 = rest.parse().unwrap();
                        if n < 0 {
                            return Reply::Array(vec![]);
                        }
                        let mut data = vec![0; n as usize + 2];
                        self.conn.read_exact(&mut data).unwrap();
                        data.truncate(n as usize);
                        Reply::Bulk(data)
                    }
                    b'*' => {
                        let n: usize = rest.parse().unwrap();
                        Reply::Array((0..n).map(|_| self.read_reply()).collect())
                    }
                    b'-' => panic!("redis error: {rest}"),
                    other => panic!("unexpected reply type {}", other as char),
                }
            }

            fn keys(&mut self, pattern: &[u8]) -> std::collections::BTreeSet<Vec<u8>> {
                match self.cmd(&[b"KEYS", pattern]) {
                    Reply::Array(items) => items
                        .into_iter()
                        .map(|r| match r {
                            Reply::Bulk(b) => b,
                            _ => panic!("non-bulk key"),
                        })
                        .collect(),
                    _ => panic!("KEYS did not return an array"),
                }
            }
        }

        /// Fuzzes `KEYS` on a real server against [`GlobPattern::keyspace`].
        ///
        /// Ignored by default because it needs `redis-server` on `PATH`:
        /// `cargo test -p redis pattern::tests::redis_differential -- --ignored`
        #[test]
        #[ignore = "requires redis-server on PATH"]
        fn keys_agrees_with_real_redis() {
            let mut server = Server::start().expect("could not start redis-server");
            let mut rng = Rng::new(0xDEC0DE);

            let mut candidates: std::collections::BTreeSet<Vec<u8>> = Default::default();
            candidates.insert(Vec::new());
            // High bytes in range endpoints are the one documented divergence
            // (signed `char` in C), so keep them out of the *patterns*; they
            // still appear in the candidate names.
            let alphabet: Vec<u8> = PATTERN_ALPHABET
                .iter()
                .copied()
                .filter(|&b| b < 0x80)
                .collect();
            while candidates.len() < 600 {
                candidates.insert(random_bytes(&mut rng, INPUT_ALPHABET, 7));
            }
            for key in &candidates {
                match server.cmd(&[b"SET", key, b"v"]) {
                    Reply::Other => {}
                    _ => panic!("SET failed"),
                }
            }

            let mut checked = 0;
            for _ in 0..3_000 {
                let pat = random_bytes(&mut rng, &alphabet, 9);
                let want = server.keys(&pat);
                let matcher = GlobPattern::keyspace(pat.clone());
                let got: std::collections::BTreeSet<Vec<u8>> = candidates
                    .iter()
                    .filter(|k| matcher.is_match(k))
                    .cloned()
                    .collect();
                assert_eq!(got, want, "pattern={pat:?}");
                checked += want.len();
            }
            assert!(
                checked > 1_000,
                "patterns selected suspiciously little: {checked}"
            );

            // Abusive nesting: Redis gives up, so must we.
            let pat = "*a".repeat(MAX_STAR_NESTING + 5).into_bytes();
            let key = "a".repeat(MAX_STAR_NESTING + 5).into_bytes();
            server.cmd(&[b"SET", &key, b"v"]);
            assert_eq!(
                server.keys(&pat).contains(&key),
                GlobPattern::keyspace(pat).is_match(&key)
            );
        }
    }
}
