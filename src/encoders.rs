//! # Column Encoders and Compression
//!
//! This module provides column-specific encoding and general-purpose compression
//! for the ledger's columnar storage format. It follows patterns from ClickHouse
//! and Parquet for optimal compression of financial data.
//!
//! ## Encoding Layers
//!
//! 1. **Column-specific encodings** (applied first):
//!    - Delta encoding: for monotonically increasing sequences (id, timestamp, journal_entry_id)
//!    - RLE: for low-cardinality data (account_id, transaction_type)
//!    - Dictionary: for string data (description)
//!
//! 2. **Block compression** (applied to encoded data):
//!    - ZSTD: default, balanced compression ratio
//!    - LZ4: faster alternative for hot data

use std::collections::HashMap;

use crate::error::{LedgerError, Result};

pub const DEFAULT_COMPRESSION_BLOCK_SIZE: usize = 16384;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressionCodec {
    None,
    Zstd,
    Lz4,
}

impl CompressionCodec {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::None),
            1 => Some(Self::Zstd),
            2 => Some(Self::Lz4),
            _ => None,
        }
    }

    pub fn to_u8(self) -> u8 {
        match self {
            Self::None => 0,
            Self::Zstd => 1,
            Self::Lz4 => 2,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnEncoding {
    None,
    Dictionary,
    Delta,
    Rle,
    DeltaDictionary,
}

impl ColumnEncoding {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::None),
            1 => Some(Self::Dictionary),
            2 => Some(Self::Delta),
            3 => Some(Self::Rle),
            4 => Some(Self::DeltaDictionary),
            _ => None,
        }
    }

    pub fn to_u8(self) -> u8 {
        match self {
            Self::None => 0,
            Self::Dictionary => 1,
            Self::Delta => 2,
            Self::Rle => 3,
            Self::DeltaDictionary => 4,
        }
    }
}

pub struct DeltaEncoder {
    first_value: i64,
    deltas: Vec<i64>,
}

impl DeltaEncoder {
    pub fn encode(values: &[i64]) -> Self {
        if values.is_empty() {
            return Self {
                first_value: 0,
                deltas: Vec::new(),
            };
        }
        let first_value = values[0];
        let mut deltas = Vec::with_capacity(values.len() - 1);
        for i in 1..values.len() {
            deltas.push(values[i] - values[i - 1]);
        }
        Self {
            first_value,
            deltas,
        }
    }

    pub fn decode(&self) -> Vec<i64> {
        if self.deltas.is_empty() {
            return vec![self.first_value];
        }
        let mut values = Vec::with_capacity(self.deltas.len() + 1);
        values.push(self.first_value);
        let mut current = self.first_value;
        for delta in &self.deltas {
            current += delta;
            values.push(current);
        }
        values
    }

    pub fn encoded_size(&self) -> usize {
        8 + (self.deltas.len() * 8)
    }

    pub fn encode_to_bytes(&self, dst: &mut Vec<u8>) {
        dst.extend_from_slice(&self.first_value.to_le_bytes());
        for delta in &self.deltas {
            dst.extend_from_slice(&delta.to_le_bytes());
        }
    }

    pub fn decode_from_bytes(data: &[u8]) -> Option<Self> {
        if data.len() < 8 {
            return None;
        }
        let first_value = i64::from_le_bytes(data[0..8].try_into().ok()?);
        let mut deltas = Vec::with_capacity((data.len() - 8) / 8);
        for chunk in data[8..].chunks_exact(8) {
            deltas.push(i64::from_le_bytes(chunk.try_into().ok()?));
        }
        Some(Self {
            first_value,
            deltas,
        })
    }
}

pub struct DeltaEncoderU64 {
    first_value: u64,
    deltas: Vec<i64>,
}

impl DeltaEncoderU64 {
    pub fn encode(values: &[u64]) -> Self {
        if values.is_empty() {
            return Self {
                first_value: 0,
                deltas: Vec::new(),
            };
        }
        let first_value = values[0];
        let mut deltas = Vec::with_capacity(values.len() - 1);
        for i in 1..values.len() {
            let diff = values[i] as i64 - values[i - 1] as i64;
            deltas.push(diff);
        }
        Self {
            first_value,
            deltas,
        }
    }

    pub fn decode(&self) -> Vec<u64> {
        if self.deltas.is_empty() {
            return vec![self.first_value];
        }
        let mut values = Vec::with_capacity(self.deltas.len() + 1);
        values.push(self.first_value);
        let mut current = self.first_value as i64;
        for delta in &self.deltas {
            current += delta;
            values.push(current as u64);
        }
        values
    }

    pub fn encode_to_bytes(&self, dst: &mut Vec<u8>) {
        dst.extend_from_slice(&self.first_value.to_le_bytes());
        for delta in &self.deltas {
            dst.extend_from_slice(&delta.to_le_bytes());
        }
    }

    pub fn decode_from_bytes(data: &[u8]) -> Option<Self> {
        if data.len() < 8 {
            return None;
        }
        let first_value = u64::from_le_bytes(data[0..8].try_into().ok()?);
        let mut deltas = Vec::with_capacity((data.len() - 8) / 8);
        for chunk in data[8..].chunks_exact(8) {
            deltas.push(i64::from_le_bytes(chunk.try_into().ok()?));
        }
        Some(Self {
            first_value,
            deltas,
        })
    }
}

pub struct RleEncoder {
    runs: Vec<(u64, u32)>,
}

impl RleEncoder {
    pub fn encode(values: &[u64]) -> Self {
        if values.is_empty() {
            return Self { runs: Vec::new() };
        }
        let mut runs = Vec::new();
        let mut current_value = values[0];
        let mut current_count = 1u32;

        for &v in &values[1..] {
            if v == current_value && current_count < u32::MAX {
                current_count += 1;
            } else {
                runs.push((current_value, current_count));
                current_value = v;
                current_count = 1;
            }
        }
        runs.push((current_value, current_count));
        Self { runs }
    }

    pub fn decode(&self) -> Vec<u64> {
        let total_len: usize = self.runs.iter().map(|(_, c)| *c as usize).sum();
        let mut values = Vec::with_capacity(total_len);
        for &(val, count) in &self.runs {
            for _ in 0..count {
                values.push(val);
            }
        }
        values
    }

    pub fn encode_to_bytes(&self, dst: &mut Vec<u8>) {
        for (value, count) in &self.runs {
            dst.extend_from_slice(&value.to_le_bytes());
            dst.extend_from_slice(&count.to_le_bytes());
        }
    }

    pub fn decode_from_bytes(data: &[u8]) -> Option<Self> {
        if data.len() % 12 != 0 {
            return None;
        }
        let mut runs = Vec::with_capacity(data.len() / 12);
        for chunk in data.chunks_exact(12) {
            let value = u64::from_le_bytes(chunk[0..8].try_into().ok()?);
            let count = u32::from_le_bytes(chunk[8..12].try_into().ok()?);
            runs.push((value, count));
        }
        Some(Self { runs })
    }
}

pub struct DictionaryEncoder {
    dictionary: HashMap<Vec<u8>, u32>,
    dictionary_values: Vec<Vec<u8>>,
    indices: Vec<u32>,
}

impl DictionaryEncoder {
    pub fn encode(values: &[Vec<u8>]) -> Self {
        let mut dictionary = HashMap::new();
        let mut dictionary_values = Vec::new();

        for value in values {
            if let Some(&idx) = dictionary.get(value) {
                continue;
            }
            let idx = dictionary_values.len() as u32;
            dictionary.insert(value.clone(), idx);
            dictionary_values.push(value.clone());
        }

        let indices: Vec<u32> = values.iter().map(|v| *dictionary.get(v).unwrap()).collect();

        Self {
            dictionary,
            dictionary_values,
            indices,
        }
    }

    pub fn encode_to_bytes(&self, dst: &mut Vec<u8>) {
        dst.extend_from_slice(&(self.dictionary_values.len() as u32).to_le_bytes());
        for value in &self.dictionary_values {
            dst.extend_from_slice(&(value.len() as u32).to_le_bytes());
            dst.extend_from_slice(value);
        }
        for &idx in &self.indices {
            dst.extend_from_slice(&idx.to_le_bytes());
        }
    }

    pub fn decode_from_bytes(data: &[u8]) -> Option<Self> {
        let mut offset = 0;
        if data.len() < 4 {
            return None;
        }
        let dict_size = u32::from_le_bytes(data[0..4].try_into().ok()?) as usize;
        offset += 4;

        let mut dictionary_values = Vec::with_capacity(dict_size);
        for _ in 0..dict_size {
            if offset + 4 > data.len() {
                return None;
            }
            let len = u32::from_le_bytes(data[offset..offset + 4].try_into().ok()?) as usize;
            offset += 4;
            if offset + len > data.len() {
                return None;
            }
            dictionary_values.push(data[offset..offset + len].to_vec());
            offset += len;
        }

        let indices: Vec<u32> = data[offset..]
            .chunks_exact(4)
            .map(|c| u32::from_le_bytes(c.try_into().ok().unwrap()))
            .collect();

        Some(Self {
            dictionary: dictionary_values
                .iter()
                .enumerate()
                .map(|(i, v)| (v.clone(), i as u32))
                .collect(),
            dictionary_values,
            indices,
        })
    }

    pub fn decode(&self) -> Vec<Vec<u8>> {
        self.indices
            .iter()
            .map(|&idx| self.dictionary_values[idx as usize].clone())
            .collect()
    }
}

pub struct BitPackedEncoder {
    bit_width: u8,
    values: Vec<u64>,
}

impl BitPackedEncoder {
    pub fn encode(values: &[u64]) -> Self {
        if values.is_empty() {
            return Self {
                bit_width: 0,
                values: Vec::new(),
            };
        }
        let max_val = values.iter().max().copied().unwrap_or(0);
        let bit_width = if max_val == 0 {
            1
        } else {
            (64 - max_val.leading_zeros()) as u8
        };
        Self {
            bit_width,
            values: values.to_vec(),
        }
    }

    pub fn decode(&self) -> Vec<u64> {
        self.values.clone()
    }

    pub fn encode_to_bytes(&self, dst: &mut Vec<u8>) {
        dst.push(self.bit_width);
        if self.bit_width == 0 {
            return;
        }
        let bytes_per_value = ((self.bit_width as usize + 7) / 8).min(8);
        for &val in &self.values {
            let bytes = val.to_le_bytes();
            dst.extend_from_slice(&bytes[..bytes_per_value]);
        }
    }

    pub fn decode_from_bytes(data: &[u8]) -> Option<Self> {
        if data.is_empty() {
            return None;
        }
        let bit_width = data[0];
        if bit_width == 0 {
            return Some(Self {
                bit_width: 0,
                values: Vec::new(),
            });
        }
        let bytes_per_value = ((bit_width as usize + 7) / 8).min(8);
        let value_count = (data.len() - 1) / bytes_per_value;
        let mut values = Vec::with_capacity(value_count);
        for i in 0..value_count {
            let offset = 1 + i * bytes_per_value;
            let mut bytes = [0u8; 8];
            bytes[..bytes_per_value].copy_from_slice(&data[offset..offset + bytes_per_value]);
            values.push(u64::from_le_bytes(bytes));
        }
        Some(Self { bit_width, values })
    }
}

pub struct BlockCompressor {
    codec: CompressionCodec,
    level: i32,
}

impl BlockCompressor {
    pub fn new(codec: CompressionCodec) -> Self {
        Self { codec, level: 1 }
    }

    pub fn with_level(codec: CompressionCodec, level: i32) -> Self {
        Self { codec, level }
    }

    pub fn compress(&self, data: &[u8]) -> Result<Vec<u8>> {
        match self.codec {
            CompressionCodec::None => Ok(data.to_vec()),
            CompressionCodec::Zstd => {
                let compressed = zstd::encode_all(data, self.level)
                    .map_err(|e| LedgerError::Encoding(format!("zstd compress error: {}", e)))?;
                Ok(compressed)
            }
            CompressionCodec::Lz4 => {
                let compressed = lz4_flex::compress_prepend_size(data);
                Ok(compressed)
            }
        }
    }

    pub fn decompress(&self, data: &[u8], uncompressed_size: usize) -> Result<Vec<u8>> {
        match self.codec {
            CompressionCodec::None => Ok(data.to_vec()),
            CompressionCodec::Zstd => {
                let decompressed = zstd::decode_all(data)
                    .map_err(|e| LedgerError::Encoding(format!("zstd decompress error: {}", e)))?;
                Ok(decompressed)
            }
            CompressionCodec::Lz4 => {
                let decompressed = lz4_flex::decompress_size_prepended(data)
                    .map_err(|e| LedgerError::Encoding(format!("lz4 decompress error: {}", e)))?;
                Ok(decompressed)
            }
        }
    }
}

pub struct ColumnCodec {
    pub encoding: ColumnEncoding,
    pub compression: CompressionCodec,
}

impl ColumnCodec {
    pub fn for_column(col_idx: usize) -> Self {
        match col_idx {
            0 => Self {
                encoding: ColumnEncoding::Delta,
                compression: CompressionCodec::Zstd,
            },
            1 => Self {
                encoding: ColumnEncoding::Rle,
                compression: CompressionCodec::Zstd,
            },
            2 => Self {
                encoding: ColumnEncoding::Delta,
                compression: CompressionCodec::Zstd,
            },
            3 => Self {
                encoding: ColumnEncoding::Dictionary,
                compression: CompressionCodec::Zstd,
            },
            4 => Self {
                encoding: ColumnEncoding::Delta,
                compression: CompressionCodec::Zstd,
            },
            5 => Self {
                encoding: ColumnEncoding::Dictionary,
                compression: CompressionCodec::Zstd,
            },
            6 => Self {
                encoding: ColumnEncoding::None,
                compression: CompressionCodec::Lz4,
            },
            7 => Self {
                encoding: ColumnEncoding::Delta,
                compression: CompressionCodec::Zstd,
            },
            _ => Self {
                encoding: ColumnEncoding::None,
                compression: CompressionCodec::Zstd,
            },
        }
    }
}

pub fn encode_column(col_idx: usize, values: &[u8], codec: &ColumnCodec) -> Result<Vec<u8>> {
    let encoded = match (col_idx, codec.encoding) {
        (_, ColumnEncoding::None) => values.to_vec(),
        (3, ColumnEncoding::Dictionary) => {
            let string_values: Vec<Vec<u8>> = values.chunks(1).map(|c| c.to_vec()).collect();
            let enc = DictionaryEncoder::encode(&string_values);
            let mut bytes = Vec::new();
            enc.encode_to_bytes(&mut bytes);
            bytes
        }
        (0 | 4 | 7, ColumnEncoding::Delta) => {
            let u64_values: Vec<u64> = values
                .chunks_exact(8)
                .map(|c| u64::from_le_bytes(c.try_into().unwrap()))
                .collect();
            let enc = DeltaEncoderU64::encode(&u64_values);
            let mut bytes = Vec::new();
            enc.encode_to_bytes(&mut bytes);
            bytes
        }
        (1, ColumnEncoding::Rle) => {
            let u64_values: Vec<u64> = values
                .chunks_exact(8)
                .map(|c| u64::from_le_bytes(c.try_into().unwrap()))
                .collect();
            let enc = RleEncoder::encode(&u64_values);
            let mut bytes = Vec::new();
            enc.encode_to_bytes(&mut bytes);
            bytes
        }
        (2, ColumnEncoding::Delta) => {
            let i64_values: Vec<i64> = values
                .chunks_exact(8)
                .map(|c| i64::from_le_bytes(c.try_into().unwrap()))
                .collect();
            let enc = DeltaEncoder::encode(&i64_values);
            let mut bytes = Vec::new();
            enc.encode_to_bytes(&mut bytes);
            bytes
        }
        _ => values.to_vec(),
    };

    let compressor = BlockCompressor::new(codec.compression);
    compressor.compress(&encoded)
}

pub fn decode_column(
    col_idx: usize,
    data: &[u8],
    encoding: ColumnEncoding,
    compression: CompressionCodec,
    uncompressed_size: usize,
) -> Result<Vec<u8>> {
    let compressor = BlockCompressor::new(compression);
    let decompressed = compressor.decompress(data, uncompressed_size)?;

    let decoded = match (col_idx, encoding) {
        (_, ColumnEncoding::None) => decompressed,
        (3, ColumnEncoding::Dictionary) => {
            let enc = DictionaryEncoder::decode_from_bytes(&decompressed)
                .ok_or_else(|| LedgerError::Encoding("failed to decode dictionary".into()))?;
            let string_values = enc.decode();
            let mut result = Vec::with_capacity(string_values.len());
            for sv in string_values {
                result.extend_from_slice(&sv);
            }
            result
        }
        (0 | 4 | 7, ColumnEncoding::Delta) => {
            let enc = DeltaEncoderU64::decode_from_bytes(&decompressed)
                .ok_or_else(|| LedgerError::Encoding("failed to decode delta u64".into()))?;
            let u64_values = enc.decode();
            let mut result = Vec::with_capacity(u64_values.len() * 8);
            for v in u64_values {
                result.extend_from_slice(&v.to_le_bytes());
            }
            result
        }
        (1, ColumnEncoding::Rle) => {
            let enc = RleEncoder::decode_from_bytes(&decompressed)
                .ok_or_else(|| LedgerError::Encoding("failed to decode RLE".into()))?;
            let u64_values = enc.decode();
            let mut result = Vec::with_capacity(u64_values.len() * 8);
            for v in u64_values {
                result.extend_from_slice(&v.to_le_bytes());
            }
            result
        }
        (2, ColumnEncoding::Delta) => {
            let enc = DeltaEncoder::decode_from_bytes(&decompressed)
                .ok_or_else(|| LedgerError::Encoding("failed to decode delta i64".into()))?;
            let i64_values = enc.decode();
            let mut result = Vec::with_capacity(i64_values.len() * 8);
            for v in i64_values {
                result.extend_from_slice(&v.to_le_bytes());
            }
            result
        }
        _ => decompressed,
    };

    Ok(decoded)
}
