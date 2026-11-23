use common::{DbError, crc32};
use std::io::{Read, Write};

pub fn write_frame<W: Write>(w: &mut W, tag: u8, body: &[u8]) -> Result<(), DbError> {
    let total_len = (1 + body.len() + 4) as u32;
    let mut frame = Vec::with_capacity(4 + total_len as usize);
    frame.extend_from_slice(&total_len.to_le_bytes());
    frame.push(tag);
    frame.extend_from_slice(body);
    let checksum = crc32(&frame[4..]);
    frame.extend_from_slice(&checksum.to_le_bytes());
    w.write_all(&frame)?;
    w.flush()?;
    Ok(())
}

pub fn read_frame<R: Read>(r: &mut R) -> Result<(u8, Vec<u8>), DbError> {
    let mut len_buf = [0u8; 4];
    r.read_exact(&mut len_buf)?;
    let total_len = u32::from_le_bytes(len_buf) as usize;
    if total_len < 5 {
        return Err(DbError::CorruptedData("Frame too short"));
    }
    let mut payload = vec![0u8; total_len];
    r.read_exact(&mut payload)?;
    let crc_offset = total_len - 4;
    let expected_crc = u32::from_le_bytes(payload[crc_offset..].try_into().unwrap());
    let actual_crc = crc32(&payload[..crc_offset]);
    if expected_crc != actual_crc {
        return Err(DbError::ChecksumMismatch {
            expected: expected_crc,
            actual: actual_crc,
        });
    }
    Ok((payload[0], payload[1..crc_offset].to_vec()))
}
