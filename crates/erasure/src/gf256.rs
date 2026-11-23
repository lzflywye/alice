/// Galois Field GF(2^8) arithmetic using primitive polynomial x^8 + x^4 + x^3 + x^2 + 1 (0x11D).
const PRIMITIVE_POLY: u16 = 0x11D;

pub struct Gf256Tables {
    pub exp: [u8; 512],
    pub log: [u8; 256],
}

pub const GF: Gf256Tables = build_tables();

const fn build_tables() -> Gf256Tables {
    let mut exp = [0u8; 512];
    let mut log = [0u8; 256];
    let mut x: u16 = 1;
    let mut i = 0;
    while i < 255 {
        exp[i] = x as u8;
        exp[i + 255] = x as u8;
        log[x as usize] = i as u8;
        x <<= 1;
        if x & 0x100 != 0 {
            x ^= PRIMITIVE_POLY;
        }
        i += 1;
    }
    Gf256Tables { exp, log }
}

#[inline]
pub fn add(a: u8, b: u8) -> u8 {
    a ^ b
}

#[inline]
pub fn mul(a: u8, b: u8) -> u8 {
    if a == 0 || b == 0 {
        return 0;
    }
    let idx = (GF.log[a as usize] as usize) + (GF.log[b as usize] as usize);
    GF.exp[idx]
}

#[inline]
pub fn div(a: u8, b: u8) -> u8 {
    assert!(b != 0, "GF(2^8) division by zero");
    if a == 0 {
        return 0;
    }
    let idx = (GF.log[a as usize] as usize) + 255 - (GF.log[b as usize] as usize);
    GF.exp[idx]
}

#[inline]
pub fn inv(a: u8) -> u8 {
    assert!(a != 0, "GF(2^8) inverse of zero");
    GF.exp[255 - (GF.log[a as usize] as usize)]
}

#[inline]
pub fn pow(a: u8, power: usize) -> u8 {
    if power == 0 {
        return 1;
    }
    if a == 0 {
        return 0;
    }
    let idx = ((GF.log[a as usize] as usize) * power) % 255;
    GF.exp[idx]
}
