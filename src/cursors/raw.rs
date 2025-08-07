use crate::bytes_ext::BytesExt;
use crate::error::Error;
use crate::row_metadata::RowMetadata;
use crate::{
    error::Result,
    response::{Chunks, Response, ResponseFuture},
    RowRead,
};
use bytes::Bytes;
use clickhouse_types::error::TypesError;
use clickhouse_types::parse_rbwnat_columns_header;
use futures::Stream;
use std::{
    pin::pin,
    task::{ready, Context, Poll},
};

/// A cursor over raw bytes of a query response.
/// All other cursors are built on top of this one.
pub(crate) struct RawCursor(RawCursorState, Option<RowMetadata>);

enum RawCursorState {
    Waiting(RawCursorWaiting),
    Loading(RawCursorLoading),
}

struct RawCursorLoading {
    chunks: Chunks,
    net_size: u64,
    data_size: u64,
}

struct RawCursorWaiting {
    future: ResponseFuture,
    validation: bool,
}

struct ParsedRowMetadata {
    row_metadata: RowMetadata,
    net_size: u64,
    data_size: u64,
    remaining_data: Option<Bytes>,
}

impl RawCursor {
    pub(crate) fn new(response: Response, validation: bool) -> Self {
        Self(
            RawCursorState::Waiting(RawCursorWaiting {
                future: response.into_future(),
                validation,
            }),
            None,
        )
    }

    pub(crate) async fn next<T: RowRead>(&mut self) -> Result<Option<Bytes>> {
        std::future::poll_fn(|cx| self.poll_next::<T>(cx)).await
    }

    pub(crate) fn poll_next<T: RowRead>(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<Bytes>>> {
        match &mut self.0 {
            RawCursorState::Waiting(_) => {
                let maybe_remaining = ready!(self.poll_resolve::<T>(cx)?);
                if let Some(remaining) = maybe_remaining {
                    // Maybe there is enough data for the first N rows after parsing the header
                    Poll::Ready(Ok(Some(remaining)))
                } else {
                    self.poll_next::<T>(cx)
                }
            }
            RawCursorState::Loading(state) => {
                let chunks = pin!(&mut state.chunks);
                Poll::Ready(match ready!(chunks.poll_next(cx)?) {
                    Some(chunk) => {
                        state.net_size += chunk.net_size as u64;
                        state.data_size += chunk.data.len() as u64;
                        Ok(Some(chunk.data))
                    }
                    None => Ok(None),
                })
            }
        }
    }

    #[cold]
    #[inline(never)]
    fn poll_resolve<T: RowRead>(&mut self, cx: &mut Context<'_>) -> Poll<Result<Option<Bytes>>> {
        let RawCursorState::Waiting(RawCursorWaiting { future, validation }) = &mut self.0 else {
            panic!("poll_resolve called in invalid state");
        };

        // Poll the future, but don't return the result yet.
        // In case of an error, we should replace the current state anyway
        // in order to provide proper fused behavior of the cursor.
        let ready_chunks = ready!(future.as_mut().poll(cx));
        Poll::Ready(match ready_chunks {
            Ok(mut chunks) if *validation => {
                let ParsedRowMetadata {
                    row_metadata,
                    net_size,
                    data_size,
                    remaining_data,
                } = ready!(self.parse_row_metadata::<T>(&mut chunks, cx)?);
                self.0 = RawCursorState::Loading(RawCursorLoading {
                    chunks,
                    net_size,
                    data_size,
                });
                self.1 = Some(row_metadata);
                Ok(remaining_data)
            }
            Ok(chunks) => {
                self.0 = RawCursorState::Loading(RawCursorLoading {
                    chunks,
                    net_size: 0,
                    data_size: 0,
                });
                Ok(None)
            }
            Err(err) => {
                self.0 = RawCursorState::Loading(RawCursorLoading {
                    chunks: Chunks::empty(),
                    net_size: 0,
                    data_size: 0,
                });
                Err(err)
            }
        })
    }

    fn parse_row_metadata<T: RowRead>(
        &mut self,
        chunks: &mut Chunks,
        cx: &mut Context<'_>,
    ) -> Poll<Result<ParsedRowMetadata>> {
        let accumulated_data = BytesExt::default();
        let mut data_size = 0u64;
        let mut net_size = 0u64;
        let mut pinned_chunks = pin!(chunks);
        loop {
            match ready!(pinned_chunks.as_mut().poll_next(cx)?) {
                None => {
                    return Poll::Ready(Err(Error::BadResponse(
                        "Could not read columns header".to_string(),
                    )));
                }
                Some(chunk) => {
                    net_size += chunk.net_size as u64;
                    data_size += chunk.data.len() as u64;

                    // SAFETY: TODO
                    unsafe { accumulated_data.extend_by_ref(chunk.data) }
                    let mut slice = accumulated_data.slice();
                    match parse_rbwnat_columns_header(&mut slice) {
                        Ok(columns) => {
                            accumulated_data.set_remaining(slice.len());
                            let row_metadata = RowMetadata::new_for_cursor::<T>(columns);
                            return Poll::Ready(Ok(ParsedRowMetadata {
                                row_metadata,
                                net_size,
                                data_size,
                                remaining_data: if accumulated_data.remaining() > 0 {
                                    Some(Bytes::from(accumulated_data.slice().to_vec()))
                                } else {
                                    None
                                },
                            }));
                        }
                        Err(TypesError::NotEnoughData(_)) => {
                            // Perhaps the header comes in multiple chunks
                            continue;
                        }
                        Err(err) => {
                            return Poll::Ready(Err(Error::InvalidColumnsHeader(err.into())));
                        }
                    }
                }
            }
        }
    }

    pub(crate) fn received_bytes(&self) -> u64 {
        match &self.0 {
            RawCursorState::Loading(state) => state.net_size,
            RawCursorState::Waiting(_) => 0,
        }
    }

    pub(crate) fn decoded_bytes(&self) -> u64 {
        match &self.0 {
            RawCursorState::Loading(state) => state.data_size,
            RawCursorState::Waiting(_) => 0,
        }
    }

    #[cfg(feature = "futures03")]
    pub(crate) fn is_terminated(&self) -> bool {
        match &self.0 {
            RawCursorState::Loading(state) => state.chunks.is_terminated(),
            RawCursorState::Waiting(_) => false,
        }
    }
}
