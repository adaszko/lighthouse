use super::*;
use crate::forwards_iter::SimpleForwardsBlockRootsIterator;
use crate::impls::beacon_state::{get_full_state, store_full_state, StorageContainer};
use crate::metrics;
use db_key::Key;
use leveldb::database::batch::{Batch, Writebatch};
use leveldb::database::kv::KV;
use leveldb::database::Database;
use leveldb::error::Error as LevelDBError;
use leveldb::options::{Options, ReadOptions, WriteOptions};
use ssz::Encode;
use std::marker::PhantomData;
use std::path::Path;

/// A wrapped leveldb database.
pub struct LevelDB<E: EthSpec> {
    db: Database<BytesKey>,
    _phantom: PhantomData<E>,
}

impl<E: EthSpec> LevelDB<E> {
    /// Open a database at `path`, creating a new database if one does not already exist.
    pub fn open(path: &Path) -> Result<Self, Error> {
        let mut options = Options::new();

        options.create_if_missing = true;

        let db = Database::open(path, options)?;

        Ok(Self {
            db,
            _phantom: PhantomData,
        })
    }

    fn read_options(&self) -> ReadOptions<BytesKey> {
        ReadOptions::new()
    }

    fn write_options(&self) -> WriteOptions {
        WriteOptions::new()
    }

    fn get_key_for_col(col: &str, key: &[u8]) -> BytesKey {
        let mut col = col.as_bytes().to_vec();
        col.append(&mut key.to_vec());
        BytesKey { key: col }
    }
}

/// Used for keying leveldb.
pub struct BytesKey {
    key: Vec<u8>,
}

impl Key for BytesKey {
    fn from_u8(key: &[u8]) -> Self {
        Self { key: key.to_vec() }
    }

    fn as_slice<T, F: Fn(&[u8]) -> T>(&self, f: F) -> T {
        f(self.key.as_slice())
    }
}

impl<E: EthSpec> Store<E> for LevelDB<E> {
    type ForwardsBlockRootsIterator = SimpleForwardsBlockRootsIterator;

    /// Retrieve some bytes in `column` with `key`.
    fn get_bytes(&self, col: &str, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        let column_key = Self::get_key_for_col(col, key);

        metrics::inc_counter(&metrics::DISK_DB_READ_COUNT);
        let timer = metrics::start_timer(&metrics::DISK_DB_READ_TIMES);

        self.db
            .get(self.read_options(), column_key)
            .map_err(Into::into)
            .map(|opt| {
                opt.map(|bytes| {
                    metrics::inc_counter_by(&metrics::DISK_DB_READ_BYTES, bytes.len() as i64);
                    metrics::stop_timer(timer);
                    bytes
                })
            })
    }

    /// Store some `value` in `column`, indexed with `key`.
    fn put_bytes(&self, col: &str, key: &[u8], val: &[u8]) -> Result<(), Error> {
        let column_key = Self::get_key_for_col(col, key);

        metrics::inc_counter(&metrics::DISK_DB_WRITE_COUNT);
        metrics::inc_counter_by(&metrics::DISK_DB_WRITE_BYTES, val.len() as i64);
        let timer = metrics::start_timer(&metrics::DISK_DB_WRITE_TIMES);

        self.db
            .put(self.write_options(), column_key, val)
            .map_err(Into::into)
            .map(|()| {
                metrics::stop_timer(timer);
            })
    }

    /// Return `true` if `key` exists in `column`.
    fn key_exists(&self, col: &str, key: &[u8]) -> Result<bool, Error> {
        let column_key = Self::get_key_for_col(col, key);

        metrics::inc_counter(&metrics::DISK_DB_EXISTS_COUNT);

        self.db
            .get(self.read_options(), column_key)
            .map_err(Into::into)
            .and_then(|val| Ok(val.is_some()))
    }

    /// Removes `key` from `column`.
    fn key_delete(&self, col: &str, key: &[u8]) -> Result<(), Error> {
        let column_key = Self::get_key_for_col(col, key);

        metrics::inc_counter(&metrics::DISK_DB_DELETE_COUNT);

        self.db
            .delete(self.write_options(), column_key)
            .map_err(Into::into)
    }

    /// Store a state in the store.
    fn put_state(&self, state_root: &Hash256, state: &BeaconState<E>) -> Result<(), Error> {
        store_full_state(self, state_root, &state)
    }

    /// Fetch a state from the store.
    fn get_state(
        &self,
        state_root: &Hash256,
        _: Option<Slot>,
    ) -> Result<Option<BeaconState<E>>, Error> {
        get_full_state(self, state_root)
    }

    fn forwards_block_roots_iterator(
        store: Arc<Self>,
        start_slot: Slot,
        end_state: BeaconState<E>,
        end_block_root: Hash256,
        _: &ChainSpec,
    ) -> Self::ForwardsBlockRootsIterator {
        SimpleForwardsBlockRootsIterator::new(store, start_slot, end_state, end_block_root)
    }

    fn do_atomically(&self, ops_batch: &[StoreOp<E>]) -> Result<(), Error> {
        let mut leveldb_batch = Writebatch::new();
        for op in ops_batch {
            match op {
                StoreOp::PutBlock(block_hash, block) => {
                    let untyped_hash: Hash256 = (*block_hash).into();
                    let key = Self::get_key_for_col(
                        DBColumn::BeaconBlock.into(),
                        untyped_hash.as_bytes(),
                    );
                    let value = block.as_store_bytes();
                    leveldb_batch.put(key, &value);
                }

                StoreOp::PutState(state_hash, state) => {
                    let untyped_hash: Hash256 = (*state_hash).into();
                    let key = Self::get_key_for_col(
                        DBColumn::BeaconState.into(),
                        untyped_hash.as_bytes(),
                    );
                    let value = StorageContainer::new(state).as_ssz_bytes();
                    leveldb_batch.put(key, &value);
                }

                StoreOp::DeleteBlock(block_hash) => {
                    let untyped_hash: Hash256 = (*block_hash).into();
                    let key = Self::get_key_for_col(
                        DBColumn::BeaconBlock.into(),
                        untyped_hash.as_bytes(),
                    );
                    leveldb_batch.delete(key);
                }

                StoreOp::DeleteState(state_hash, slot) => {
                    let untyped_hash: Hash256 = (*state_hash).into();
                    let state_summary_key = Self::get_key_for_col(
                        DBColumn::BeaconStateSummary.into(),
                        untyped_hash.as_bytes(),
                    );
                    leveldb_batch.delete(state_summary_key);

                    if *slot % E::slots_per_epoch() == 0 {
                        let state_key = Self::get_key_for_col(
                            DBColumn::BeaconState.into(),
                            untyped_hash.as_bytes(),
                        );
                        leveldb_batch.delete(state_key);
                    }
                }
            }
        }
        self.db.write(self.write_options(), &leveldb_batch)?;
        Ok(())
    }
}

impl From<LevelDBError> for Error {
    fn from(e: LevelDBError) -> Error {
        Error::DBError {
            message: format!("{:?}", e),
        }
    }
}
