use crate::row_metadata::RowMetadata;
use crate::{
    bytes_ext::BytesExt,
    cursors::RawCursor,
    error::{Error, Result},
    response::Response,
    rowbinary, Row,
};
use clickhouse_types::error::TypesError;
use clickhouse_types::parse_rbwnat_columns_header;
use serde::Deserialize;
use std::marker::PhantomData;

enum ValidationMode {
    Enabled(Box<RowMetadata>),
    Disabled,
}

/// A cursor that emits rows deserialized as structures from RowBinary.
#[must_use]
pub struct RowCursor<T> {
    raw: RawCursor,
    bytes: BytesExt,
    validation: ValidationMode,
    _marker: PhantomData<T>,
}

impl<T: Row> RowCursor<T> {
    pub(crate) async fn new(response: Response, validation: bool) -> Result<Self> {
        let mut bytes = BytesExt::default();
        let mut raw = RawCursor::new(response);
        if validation {
            loop {
                match raw.next().await? {
                    Some(chunk) => bytes.extend(chunk),
                    None => {
                        return Err(Error::BadResponse(
                            "Could not read columns header".to_string(),
                        ));
                    }
                }
                let mut slice = bytes.slice();
                match parse_rbwnat_columns_header(&mut slice) {
                    Ok(columns) if !columns.is_empty() => {
                        bytes.set_remaining(slice.len());
                        let row_metadata = RowMetadata::new::<T>(columns);
                        return Ok(Self {
                            raw,
                            bytes,
                            validation: ValidationMode::Enabled(Box::new(row_metadata)),
                            _marker: PhantomData,
                        });
                    }
                    Ok(_) => {
                        // This does not panic, as it could be a network issue
                        // or a malformed response from the server or LB,
                        // and a simple retry might help in certain cases.
                        return Err(Error::BadResponse(
                            "Expected at least one column in the header".to_string(),
                        ));
                    }
                    Err(TypesError::NotEnoughData(_)) => {}
                    Err(err) => {
                        return Err(Error::InvalidColumnsHeader(err.into()));
                    }
                }
            }
        } else {
            Ok(Self {
                raw,
                bytes,
                validation: ValidationMode::Disabled,
                _marker: PhantomData,
            })
        }
    }

    /// Emits the next row.
    ///
    /// The result is unspecified if it's called after `Err` is returned.
    ///
    /// # Cancel safety
    ///
    /// This method is cancellation safe.
    pub async fn next<'cursor, 'data: 'cursor>(&'cursor mut self) -> Result<Option<T>>
    where
        T: Deserialize<'data> + Row,
    {
        loop {
            if self.bytes.remaining() > 0 {
                let mut slice: &[u8];
                let result = match &self.validation {
                    ValidationMode::Enabled(row_metadata) => {
                        slice = super::workaround_51132(self.bytes.slice());
                        rowbinary::deserialize_rbwnat::<T>(&mut slice, row_metadata)
                    }
                    ValidationMode::Disabled => {
                        slice = super::workaround_51132(self.bytes.slice());
                        rowbinary::deserialize_row_binary::<T>(&mut slice)
                    }
                };
                match result {
                    Ok(value) => {
                        self.bytes.set_remaining(slice.len());
                        return Ok(Some(value));
                    }
                    Err(Error::NotEnoughData) => {}
                    Err(err) => {
                        return Err(err);
                    }
                }
            }

            match self.raw.next().await? {
                Some(chunk) => self.bytes.extend(chunk),
                None if self.bytes.remaining() > 0 => {
                    // If some data is left, we have an incomplete row in the buffer.
                    // This is usually a schema mismatch on the client side.
                    return Err(Error::NotEnoughData);
                }
                None => return Ok(None),
            }
        }
    }

    /// Returns the total size in bytes received from the CH server since
    /// the cursor was created.
    ///
    /// This method counts only size without HTTP headers for now.
    /// It can be changed in the future without notice.
    #[inline]
    pub fn received_bytes(&self) -> u64 {
        self.raw.received_bytes()
    }

    /// Returns the total size in bytes decompressed since the cursor was created.
    #[inline]
    pub fn decoded_bytes(&self) -> u64 {
        self.raw.decoded_bytes()
    }
}
