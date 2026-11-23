/// Compact, zero-dependency Bloom Filter using double hashing (FNV-1a variants).
#[derive(Debug, Clone)]
pub struct BloomFilter {
    bits: Vec<u8>,
    num_hashes: u8,
}

impl BloomFilter {
    /// Creates a Bloom filter sized for `expected_items` with `bits_per_key` (e.g. 10 bits/key -> ~1% FP rate).
    pub fn new(expected_items: usize, bits_per_key: usize) -> Self {
        let total_bits = (expected_items.max(1) * bits_per_key.max(4)).next_multiple_of(8);
        let num_hashes = (((bits_per_key as f64) * 0.69).round() as u8).clamp(1, 16);
        Self {
            bits: vec![0u8; total_bits / 8],
            num_hashes,
        }
    }

    pub fn insert(&mut self, key: &[u8]) {
        let m = (self.bits.len() * 8) as u64;
        let (h1, h2) = hash_pair(key);
        for i in 0..self.num_hashes as u64 {
            let idx = (h1.wrapping_add(i.wrapping_mul(h2)) % m) as usize;
            self.bits[idx / 8] |= 1 << (idx % 8);
        }
    }

    pub fn may_contain(&self, key: &[u8]) -> bool {
        if self.bits.is_empty() {
            return true;
        }
        let m = (self.bits.len() * 8) as u64;
        let (h1, h2) = hash_pair(key);
        for i in 0..self.num_hashes as u64 {
            let idx = (h1.wrapping_add(i.wrapping_mul(h2)) % m) as usize;
            if (self.bits[idx / 8] & (1 << (idx % 8))) == 0 {
                return false;
            }
        }
        true
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(5 + self.bits.len());
        out.push(self.num_hashes);
        out.extend_from_slice(&(self.bits.len() as u32).to_le_bytes());
        out.extend_from_slice(&self.bits);
        out
    }

    pub fn decode(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < 5 {
            return None;
        }
        let num_hashes = bytes[0];
        let len = u32::from_le_bytes(bytes[1..5].try_into().ok()?) as usize;
        if bytes.len() < 5 + len {
            return None;
        }
        Some(Self {
            bits: bytes[5..5 + len].to_vec(),
            num_hashes,
        })
    }
}

fn hash_pair(data: &[u8]) -> (u64, u64) {
    let mut h1: u64 = 0xcbf2_9ce4_8422_2325;
    let mut h2: u64 = 0x8422_2325_cbf2_9ce4;
    for &b in data {
        h1 ^= b as u64;
        h1 = h1.wrapping_mul(0x0000_0100_0000_01b3);
        h2 = h2.wrapping_mul(0x0000_0100_0000_01b3);
        h2 ^= b as u64;
    }
    (h1, h2 | 1)
}
