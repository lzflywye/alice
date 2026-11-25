use common::DbError;
use std::cmp::Ordering;
use std::fmt;

pub const MAX_NUMERIC_PRECISION: u8 = 38;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RoundingMode {
    /// Round to nearest; ties go to even digit (banker's rounding).
    HalfEven,
    /// Round to nearest; ties round away from zero (standard 4/5 rounding).
    HalfUp,
    /// Truncate fractional digits toward zero.
    Down,
    /// Round away from zero if any non-zero discarded digits exist.
    Up,
}

/// 128-bit fixed-point decimal supporting up to 38 digits of precision (`NUMERIC(p, s)`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Numeric {
    pub unscaled: i128,
    pub scale: u8,
}

impl Numeric {
    pub fn new(unscaled: i128, scale: u8) -> Result<Self, DbError> {
        if scale > MAX_NUMERIC_PRECISION {
            return Err(DbError::CorruptedData("Numeric scale exceeds 38"));
        }
        Ok(Self { unscaled, scale })
    }

    /// Parses decimal literals such as `"-123.450"` or `"42"`.
    pub fn parse(s: &str) -> Result<Self, DbError> {
        let s = s.trim();
        if s.is_empty() {
            return Err(DbError::CorruptedData("Empty numeric string"));
        }
        let (neg, rest) = if let Some(r) = s.strip_prefix('-') {
            (true, r)
        } else if let Some(r) = s.strip_prefix('+') {
            (false, r)
        } else {
            (false, s)
        };

        let mut parts = rest.split('.');
        let int_part = parts.next().unwrap_or("");
        let frac_part = parts.next();
        if parts.next().is_some() || (int_part.is_empty() && frac_part.unwrap_or("").is_empty()) {
            return Err(DbError::CorruptedData("Invalid numeric format"));
        }

        let frac = frac_part.unwrap_or("");
        let scale = u8::try_from(frac.len())
            .map_err(|_| DbError::CorruptedData("Numeric scale overflow"))?;
        if scale > MAX_NUMERIC_PRECISION {
            return Err(DbError::CorruptedData("Numeric scale exceeds 38"));
        }

        let mut digits = String::with_capacity(int_part.len() + frac.len());
        digits.push_str(int_part);
        digits.push_str(frac);
        if !digits.bytes().all(|b| b.is_ascii_digit()) {
            return Err(DbError::CorruptedData("Non-digit character in numeric"));
        }

        let mag: i128 = digits
            .parse()
            .map_err(|_| DbError::CorruptedData("Numeric overflows 128-bit integer"))?;
        let unscaled = if neg { -mag } else { mag };
        Ok(Self { unscaled, scale })
    }

    /// Rescales to `target_scale` using `mode`, and validates that total digits fit within `precision`.
    pub fn fit_to_schema(
        self,
        precision: u8,
        target_scale: u8,
        mode: RoundingMode,
    ) -> Result<Self, DbError> {
        if precision == 0 || precision > MAX_NUMERIC_PRECISION || target_scale > precision {
            return Err(DbError::CorruptedData("Invalid NUMERIC(precision, scale)"));
        }
        let rescaled = self.rescale(target_scale, mode)?;
        let max_abs = pow10_i128(precision)? - 1;
        if rescaled.unscaled.saturating_abs() > max_abs {
            return Err(DbError::CorruptedData(
                "Numeric value exceeds column precision",
            ));
        }
        Ok(rescaled)
    }

    /// Converts this `Numeric` to `target_scale` using the specified `RoundingMode`.
    pub fn rescale(self, target_scale: u8, mode: RoundingMode) -> Result<Self, DbError> {
        if target_scale > MAX_NUMERIC_PRECISION {
            return Err(DbError::CorruptedData("Numeric scale exceeds 38"));
        }
        if self.scale == target_scale {
            return Ok(self);
        }
        if target_scale > self.scale {
            let factor = pow10_i128(target_scale - self.scale)?;
            let unscaled = self
                .unscaled
                .checked_mul(factor)
                .ok_or(DbError::CorruptedData("Numeric rescale overflow"))?;
            return Ok(Self {
                unscaled,
                scale: target_scale,
            });
        }

        // Downscaling: divide by 10^(self.scale - target_scale) with rounding
        let diff = self.scale - target_scale;
        let divisor = pow10_i128(diff)?;
        let quot = self.unscaled / divisor;
        let rem = self.unscaled % divisor;
        if rem == 0 {
            return Ok(Self {
                unscaled: quot,
                scale: target_scale,
            });
        }

        let sign: i128 = if self.unscaled < 0 { -1 } else { 1 };
        let abs_rem = rem.abs();
        let half = divisor / 2;

        let increment = match mode {
            RoundingMode::Down => false,
            RoundingMode::Up => true,
            RoundingMode::HalfUp => abs_rem >= half,
            RoundingMode::HalfEven => match abs_rem.cmp(&half) {
                Ordering::Greater => true,
                Ordering::Less => false,
                Ordering::Equal => (quot.abs() % 2) == 1,
            },
        };

        let adjusted = if increment { quot + sign } else { quot };
        Ok(Self {
            unscaled: adjusted,
            scale: target_scale,
        })
    }

    pub fn checked_add(self, other: Self) -> Result<Self, DbError> {
        let target_scale = self.scale.max(other.scale);
        let a = self.rescale(target_scale, RoundingMode::Down)?;
        let b = other.rescale(target_scale, RoundingMode::Down)?;
        let sum = a
            .unscaled
            .checked_add(b.unscaled)
            .ok_or(DbError::CorruptedData("Numeric addition overflow"))?;
        Ok(Self {
            unscaled: sum,
            scale: target_scale,
        })
    }

    pub fn checked_sub(self, other: Self) -> Result<Self, DbError> {
        let target_scale = self.scale.max(other.scale);
        let a = self.rescale(target_scale, RoundingMode::Down)?;
        let b = other.rescale(target_scale, RoundingMode::Down)?;
        let diff = a
            .unscaled
            .checked_sub(b.unscaled)
            .ok_or(DbError::CorruptedData("Numeric subtraction overflow"))?;
        Ok(Self {
            unscaled: diff,
            scale: target_scale,
        })
    }

    pub fn checked_mul(self, other: Self) -> Result<Self, DbError> {
        let prod = self
            .unscaled
            .checked_mul(other.unscaled)
            .ok_or(DbError::CorruptedData("Numeric multiplication overflow"))?;
        let raw_scale = self.scale + other.scale;
        let raw = Self {
            unscaled: prod,
            scale: raw_scale,
        };
        if raw_scale > MAX_NUMERIC_PRECISION {
            raw.rescale(MAX_NUMERIC_PRECISION, RoundingMode::HalfEven)
        } else {
            Ok(raw)
        }
    }
}

impl PartialOrd for Numeric {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Numeric {
    fn cmp(&self, other: &Self) -> Ordering {
        let s = self.scale.max(other.scale);
        let a = self.rescale(s, RoundingMode::Down).unwrap();
        let b = other.rescale(s, RoundingMode::Down).unwrap();
        a.unscaled.cmp(&b.unscaled)
    }
}

impl fmt::Display for Numeric {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.scale == 0 {
            return write!(f, "{}", self.unscaled);
        }
        let sign = if self.unscaled < 0 { "-" } else { "" };
        let abs_str = self.unscaled.unsigned_abs().to_string();
        let scale = self.scale as usize;
        if abs_str.len() <= scale {
            let zeros = "0".repeat(scale - abs_str.len());
            write!(f, "{sign}0.{zeros}{abs_str}")
        } else {
            let split = abs_str.len() - scale;
            write!(f, "{sign}{}.{}", &abs_str[..split], &abs_str[split..])
        }
    }
}

fn pow10_i128(exp: u8) -> Result<i128, DbError> {
    10i128
        .checked_pow(exp as u32)
        .ok_or(DbError::CorruptedData("Power of 10 overflow"))
}
