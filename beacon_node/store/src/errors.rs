use crate::chunked_vector::ChunkError;
use crate::hot_cold_store::HotColdDBError;
use ssz::DecodeError;
use types::{BeaconStateError, Hash256};

#[derive(Debug)]
pub enum Error {
    SszDecodeError(DecodeError),
    VectorChunkError(ChunkError),
    BeaconStateError(BeaconStateError),
    PartialBeaconStateError,
    HotColdDBError(HotColdDBError),
    DBError { message: String },
    RlpError(String),
    BlockNotFound(Hash256),
    IOError(std::io::Error),
}

impl From<DecodeError> for Error {
    fn from(e: DecodeError) -> Error {
        Error::SszDecodeError(e)
    }
}

impl From<ChunkError> for Error {
    fn from(e: ChunkError) -> Error {
        Error::VectorChunkError(e)
    }
}

impl From<HotColdDBError> for Error {
    fn from(e: HotColdDBError) -> Error {
        Error::HotColdDBError(e)
    }
}

impl From<BeaconStateError> for Error {
    fn from(e: BeaconStateError) -> Error {
        Error::BeaconStateError(e)
    }
}

impl From<DBError> for Error {
    fn from(e: DBError) -> Error {
        Error::DBError { message: e.message }
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Error {
        Error::IOError(e)
    }
}

#[derive(Debug)]
pub struct DBError {
    pub message: String,
}

impl DBError {
    pub fn new(message: String) -> Self {
        Self { message }
    }
}
