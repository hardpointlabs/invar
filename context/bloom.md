how does redis split large bloom filters? I understand it doesn't place any limit on bloom filter size

Redis Bloom filters in the [RedisBloom module](https://redis.io/docs/latest/develop/data-types/probabilistic/bloom-filter/?utm_source=chatgpt.com) are implemented as a **scalable Bloom filter**, not as one giant infinitely growing bit array.

So when a filter becomes “too large” or reaches its configured capacity, Redis does **not** resize the existing bitset in place. Instead, it appends a **new sub-filter** internally.

Conceptually, the structure looks like:

```text
Bloom Filter Key
├── Sub-filter 0 (initial filter)
├── Sub-filter 1
├── Sub-filter 2
└── ...
```

Each sub-filter is an independent Bloom filter with:

* its own bit array
* its own hash seeds
* its own capacity
* its own false positive rate target

## What triggers a split?

When you create a filter:

```redis
BF.RESERVE myfilter 0.01 1000000
```

you specify:

* target false positive rate
* initial capacity

Once the first sub-filter becomes saturated (approaches the designed insertion count), Redis creates another one.

New inserts go into the newest sub-filter.

Lookups check all sub-filters until one says “possibly exists”.

---

## Why Redis does this instead of resizing

Classic Bloom filters cannot safely resize because:

* the hash modulo changes
* existing bits become invalid
* rehashing all entries would require storing originals (which Bloom filters do not)

So scalable Bloom filters avoid resizing entirely.

This comes from the paper:

* Scalable Bloom Filters

The idea:

* allocate new filters progressively
* tighten error rates per layer
* keep overall false positive probability bounded

---

## How growth works internally

By default RedisBloom uses:

* an **expansion factor** of 2

So capacities grow roughly like:

| Sub-filter | Capacity |
| ---------- | -------- |
| 0          | 1M       |
| 1          | 2M       |
| 2          | 4M       |
| 3          | 8M       |

You can control this:

```redis
BF.RESERVE myfilter 0.01 1000000 EXPANSION 4
```

Then capacities become:

* 1M
* 4M
* 16M
* etc.

Larger expansion:

* fewer sub-filters
* larger memory jumps
* potentially faster lookups

Smaller expansion:

* smoother memory growth
* more sub-filters
* slower lookups eventually

---

## Important nuance: “No size limit” is practical, not literal

RedisBloom has no *logical* maximum cardinality for the Bloom filter key because it chains sub-filters.

But each sub-filter is still bounded by:

* Redis allocator limits
* available RAM
* maximum allocation sizes
* Redis key overhead

So “unbounded” really means:

> “automatically scalable by adding internal filters”

—not:

> “one giant endlessly resizing bitmap”

---

## Time complexity implications

As the filter scales:

### Insert

Usually:

```text
O(1)
```

because inserts only touch the newest sub-filter.

### Lookup

Becomes:

```text
O(number_of_subfilters)
```

because Redis checks each layer.

In practice this is usually small because exponential growth keeps the number of layers low.

Example:

* 1B entries with expansion=2
* starting at 1M
* only about 10 sub-filters

---

## Memory behavior

Bloom filter memory is determined approximately by:

m=-\frac{n\ln p}{(\ln 2)^2}

Where:

* (m) = bits required
* (n) = expected insertions
* (p) = false positive probability

Redis computes this per sub-filter.

The number of hash functions is:

k=\frac{m}{n}\ln 2

---

## Operational caveat

A scalable Bloom filter never shrinks.

If:

* you massively overgrow it
* then delete most data logically

…the memory stays allocated until the key is deleted/rebuilt.

That surprises people coming from dynamically resizable structures like hash tables.

so if I were to reimplement Redis bloom filters based on persistent LSM trees (BadgerDB), it's just a case of handling the splits, tracking insertion count and pointers to the various sub-filters?

Yes — at a high level, that’s essentially the architecture.

A scalable Bloom filter on top of an LSM KV store like [BadgerDB](https://github.com/dgraph-io/badger?utm_source=chatgpt.com) is mostly:

1. a sequence of immutable-ish Bloom filter segments
2. metadata tracking
3. growth/split policy
4. lookup orchestration across segments

The core idea is much simpler than people initially expect.

---

# Conceptual model

Instead of:

```text id="8x3v3g"
One giant resizeable bloom filter
```

you build:

```text id="f9yjlwm"
BloomFilter
├── Filter #0
├── Filter #1
├── Filter #2
└── ...
```

Each filter has:

* bitset
* hash seed(s)
* capacity
* insertion count
* false positive target
* storage location

---

# The metadata you need

You typically need one small metadata object:

```go id="ayvnhj"
type FilterMeta struct {
    Filters []SubFilterMeta
}

type SubFilterMeta struct {
    ID              uint64
    Capacity        uint64
    Inserted        uint64
    ErrorRate       float64
    NumHashes       uint32
    BitArraySize    uint64
    Seed            uint64
    Location        string
}
```

This is basically the entire scalable-BF state machine.

---

# Insert flow

Insertion logic is:

```text id="rwj0iy"
latest := filters[last]

if latest.Inserted >= latest.Capacity:
    latest = createNewSubFilter()

insertInto(latest)
latest.Inserted++
```

That’s it structurally.

---

# Lookup flow

Lookups check newest → oldest:

```text id="gmqqak"
for i := len(filters)-1; i >= 0; i-- {
    if filters[i].MayContain(key) {
        return true
    }
}

return false
```

Newest-first matters because:

* recent inserts likely live there
* improves cache locality
* early-exit probability is better

---

# Important implementation details

The hard parts are not the splits themselves.

The real engineering complexity is:

## 1. Atomicity

You must atomically:

* set bits
* increment insertion count
* possibly create new filter metadata

Otherwise crashes can corrupt logical state.

With Badger transactions this is manageable.

---

## 2. Bitset layout

Efficient persistent storage matters more than the scalable logic.

Naive implementation:

```text id="ef7kjp"
key: bf:1:bit:1234567 -> 1
```

…would be catastrophically slow.

Instead you want:

* chunked bitmaps
* memory-mapped blocks
* append-friendly storage
* compressed pages
* large aligned writes

Usually:

```text id="kzaxjz"
bf:{filterID}:{pageID} -> []byte
```

where each page might hold:

* 4KB
* 16KB
* 64KB

of bits.

---

## 3. Write amplification

LSM trees already rewrite data repeatedly.

Bloom filters are random-write-heavy.

That combination can become nasty:

* memtable churn
* compaction amplification
* SSD wear
* cache misses

Redis avoids this because it’s RAM-first.

On Badger, batching writes becomes extremely important.

---

# Key insight: insertion count is approximate anyway

A Bloom filter doesn’t know unique cardinality.

Redis simply tracks:

* attempted insertions

not:

* distinct elements

So:

```text id="lg67tt"
ADD foo
ADD foo
ADD foo
```

increments count 3 times.

This is intentional because:

* dedup would require another structure
* that defeats the point

Capacity means:

> expected insertion count before FP degradation

—not unique members.

---

# One subtle thing RedisBloom does

RedisBloom progressively tightens FP rate on newer subfilters.

Why?

Because total false positive probability across filters is additive-ish.

If each filter had:

```text id="1adbhp"
1%
```

then 20 filters becomes terrible.

So scalable Bloom filters typically use:

```text id="9bryoz"
p_i = p_0 * r^i
```

where:

* (r < 1)

This keeps global FP bounded.

This part is commonly missed in reimplementations.

---

# LSM-specific optimization opportunity

Since you’re already on an LSM:

You can actually make the Bloom filters partially immutable.

A very good design is:

## Active mutable filter

Stored in memory + WAL.

## Frozen filters

Stored compactly and never modified again.

This aligns beautifully with LSM philosophy:

* immutable SSTables
* append-only growth
* sequential IO

You effectively get:

* fewer random writes
* better compression
* simpler crash recovery

This is probably better than literally copying RedisBloom’s RAM-oriented design.

---

# Another important optimization

You probably want:

* mmap’d filters
  or
* block cache

Because Bloom lookups are random-access bit probes.

Without caching:

* lookup latency explodes on disk.

---

# Complexity summary

The scalable logic itself is maybe:

```text id="ckud6m"
~500 LOC
```

The difficult parts are:

* efficient persistent bit storage
* batching
* compaction interaction
* crash consistency
* cache behavior

—not the filter splitting policy itself.

## Scalable bloom filter operating principles

Here is a summary of the paper "Scalable Bloom Filters" in markdown format:

**Introduction**
Bloom Filters provide space-efficient storage of sets at the cost of a probability of false positives. However, the size of the filter must be defined a priori, which can lead to over-dimensioning and wasted space.

**Problem Statement**
The problem is that it is not always possible to know in advance how many elements will need to be stored, which leads to over-dimensioning or relinquishing the maximum error probability.

**Solution: Scalable Bloom Filters (SBF)**
SBF is a variant of Bloom Filters that can adapt dynamically to the number of elements stored while ensuring a maximum false positive probability. The key ideas are:

* SBF is made up of a series of one or more (plain) Bloom Filters. When filters get full, a new one is added.
* Each successive Bloom Filter is created with a tighter maximum error probability on a geometric progression, so that the compounded probability over the whole series converges to a desired value.

**Properties of SBF**

* The number of slices for each filter is determined by the initial size and error probability.
* The error probability for the SBF is bounded by: P ≤ 2P0^(1/k0)
* The space usage for the SBF is determined by the geometric progression of the filter sizes.

**Choosing Parameters for SBF**

* The growth rate of the filter size (s) can be chosen to balance space usage and number of stages.
* The error probability tightening rate (r) can be chosen to balance space usage and error probability.

**Conclusion**
SBF provides a mechanism for representing sets without having to know a priori the maximum set size, while being able to choose from the start the maximum false positive probability. The mechanism adapts to set growth by using a series of classic Bloom Filters of increasing sizes and tighter error probabilities, added as needed.

The redis filters base their plitting strategy on a 2007 paper called [Scalable Bloom Filters](https://gsd.di.uminho.pt/members/cbm/ps/dbloom.pdf).