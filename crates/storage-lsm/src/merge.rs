use crate::sstable::EntryValue;
use std::cmp::Ordering;
use std::collections::BinaryHeap;

struct MergeCursor {
    key: Vec<u8>,
    val: EntryValue,
    /// Higher sequence means newer source (e.g. MemTable > L0 newest > L0 oldest > L1 > L2)
    seq: usize,
    source_idx: usize,
}

impl PartialEq for MergeCursor {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.seq == other.seq
    }
}

impl Eq for MergeCursor {}

impl PartialOrd for MergeCursor {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for MergeCursor {
    fn cmp(&self, other: &Self) -> Ordering {
        // Min-heap by key ascending, then max-heap by seq descending (newest first)
        other
            .key
            .cmp(&self.key)
            .then_with(|| self.seq.cmp(&other.seq))
    }
}

/// Streaming K-way merge iterator that deduplicates older versions of identical keys.
pub struct KWayMergeIterator {
    sources: Vec<std::vec::IntoIter<(Vec<u8>, EntryValue)>>,
    heap: BinaryHeap<MergeCursor>,
    last_emitted_key: Option<Vec<u8>>,
}

impl KWayMergeIterator {
    /// `ranked_sources` must be ordered from oldest (`seq = 0`) to newest (`seq = N - 1`).
    pub fn new(ranked_sources: Vec<Vec<(Vec<u8>, EntryValue)>>) -> Self {
        let mut sources = Vec::with_capacity(ranked_sources.len());
        let mut heap = BinaryHeap::with_capacity(ranked_sources.len());

        for (seq, entries) in ranked_sources.into_iter().enumerate() {
            let mut iter = entries.into_iter();
            if let Some((key, val)) = iter.next() {
                heap.push(MergeCursor {
                    key,
                    val,
                    seq,
                    source_idx: sources.len(),
                });
            }
            sources.push(iter);
        }

        Self {
            sources,
            heap,
            last_emitted_key: None,
        }
    }
}

impl Iterator for KWayMergeIterator {
    type Item = (Vec<u8>, EntryValue);

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(cursor) = self.heap.pop() {
            if let Some((next_k, next_v)) = self.sources[cursor.source_idx].next() {
                self.heap.push(MergeCursor {
                    key: next_k,
                    val: next_v,
                    seq: cursor.seq,
                    source_idx: cursor.source_idx,
                });
            }

            if self
                .last_emitted_key
                .as_ref()
                .is_some_and(|prev| prev == &cursor.key)
            {
                // Skip superseded older version of the same key
                continue;
            }

            self.last_emitted_key = Some(cursor.key.clone());
            return Some((cursor.key, cursor.val));
        }
        None
    }
}
