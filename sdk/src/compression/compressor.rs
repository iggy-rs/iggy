use crate::error::Error;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use lzzzz::lz4f;
use lzzzz::lz4f::Preferences;
use std::io::{Read, Write};

pub trait Compressor {
    fn compress<'a>(
        &self,
        data: &'a [u8],
        compression_buffer: &'a mut Vec<u8>,
    ) -> Result<(), Error>;
    fn decompress<'a>(
        &self,
        data: &'a [u8],
        decompression_buffer: &'a mut Vec<u8>,
    ) -> Result<(), Error>;
}
#[derive(Default)]
pub struct ZstdCompressor {}
// TODO(numinex) - explore using this https://docs.rs/zstd/0.13.0/zstd/dict/index.html
impl Compressor for ZstdCompressor {
    fn compress<'a>(
        &self,
        data: &'a [u8],
        compression_buffer: &'a mut Vec<u8>,
    ) -> Result<(), Error> {
        zstd::stream::copy_encode(data, compression_buffer, zstd::DEFAULT_COMPRESSION_LEVEL)?;
        Ok(())
    }

    fn decompress<'a>(
        &self,
        data: &'a [u8],
        decompression_buffer: &'a mut Vec<u8>,
    ) -> Result<(), Error> {
        zstd::stream::copy_decode(data, decompression_buffer)?;
        Ok(())
    }
}
#[derive(Default)]
pub struct Lz4Compressor {}
//TODO (numinex) - Explore using lz4-flex crate instead.
impl Compressor for Lz4Compressor {
    fn compress<'a>(
        &self,
        data: &'a [u8],
        compression_buffer: &'a mut Vec<u8>,
    ) -> Result<(), Error> {
        let pref = Preferences::default();
        lz4f::compress_to_vec(data, compression_buffer, &pref)?;
        Ok(())
    }

    fn decompress<'a>(
        &self,
        data: &'a [u8],
        decompression_buffer: &'a mut Vec<u8>,
    ) -> Result<(), Error> {
        lz4f::decompress_to_vec(data, decompression_buffer)?;
        Ok(())
    }
}

#[derive(Default)]
pub struct GzCompressor {}
impl Compressor for GzCompressor {
    fn compress<'a>(
        &self,
        data: &'a [u8],
        compression_buffer: &'a mut Vec<u8>,
    ) -> Result<(), Error> {
        let mut encoder = GzEncoder::new(compression_buffer, Compression::default());
        encoder.write_all(data)?;
        encoder.finish()?;
        Ok(())
    }
    fn decompress<'a>(
        &self,
        data: &'a [u8],
        decompression_buffer: &'a mut Vec<u8>,
    ) -> Result<(), Error> {
        let mut decoder = GzDecoder::new(data);
        decoder.read_to_end(decompression_buffer)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const DATA: &str = r#"
        (a stench is in 3 5)
         (a pit is nearby)
         (is the wumpus near)
         (Did you go to 3 8)
         (Yes -- Nothing is there)
        (Shoot -- Shoot left)
        (Kill the wumpus -- shoot up)))
        (defun ss (&optional (sentences *sentences*))
        "Run some test sentences, and count how many were¬
        not parsed."
        (count-if-not
        #'(lambda (s)
        (format t "~2&>>> ~(~{~a ~}~)~%"¬
        s)
        (write (second (parse s)) :pretty t))
        *sentences*))"#;

    #[test]
    fn test_gzip_compress() {
        let compressor = GzCompressor::default();
        let mut compression_buffer = Vec::new();
        let result = compressor.compress(DATA.as_bytes(), &mut compression_buffer);
        assert!(result.is_ok());
        assert_ne!(compression_buffer.len(), DATA.len());
    }

    #[test]
    fn test_gzip_decompress() {
        let compressor = GzCompressor::default();
        let mut compression_buffer = Vec::new();
        compressor
            .compress(DATA.as_bytes(), &mut compression_buffer)
            .unwrap();
        let mut decompression_buffer = Vec::new();
        let result = compressor.decompress(&compression_buffer, &mut decompression_buffer);

        assert!(result.is_ok());
        assert_eq!(decompression_buffer, DATA.as_bytes());
    }

    #[test]
    fn test_lz4_compress() {
        let compressor = Lz4Compressor::default();
        let mut compression_buffer = Vec::new();
        let result = compressor.compress(DATA.as_bytes(), &mut compression_buffer);
        assert!(result.is_ok());
        assert_ne!(compression_buffer.len(), DATA.len());
    }
    #[test]
    fn test_lz4_decompress() {
        let compressor = Lz4Compressor::default();
        let mut compression_buffer = Vec::new();
        compressor
            .compress(DATA.as_bytes(), &mut compression_buffer)
            .unwrap();
        let mut decompression_buffer = Vec::with_capacity(DATA.len());
        let result = compressor.decompress(&compression_buffer, &mut decompression_buffer);

        assert!(result.is_ok());
        assert_eq!(decompression_buffer, DATA.as_bytes());
    }
    #[test]
    fn test_zstd_compress() {
        let compressor = ZstdCompressor::default();
        let mut compression_buffer = Vec::new();
        let result = compressor.compress(DATA.as_bytes(), &mut compression_buffer);
        assert!(result.is_ok());
        assert_ne!(compression_buffer.len(), DATA.len());
    }
    #[test]
    fn test_zstd_decompress() {
        let compressor = ZstdCompressor::default();
        let mut compression_buffer = Vec::new();
        compressor
            .compress(DATA.as_bytes(), &mut compression_buffer)
            .unwrap();
        let mut decompression_buffer = Vec::with_capacity(DATA.len());
        let result = compressor.decompress(&compression_buffer, &mut decompression_buffer);

        assert!(result.is_ok());
        assert_eq!(decompression_buffer, DATA.as_bytes());
    }
}
