mod decoder;
mod encoding;
mod fetcher;
mod segment;

pub use decoder::{decode_inline_segment, decompress_segment_bytes};
pub use encoding::SpoolingEncoding;
pub use fetcher::SegmentFetcher;
pub use segment::Segment;
