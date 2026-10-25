use crate::{Transformer, error::SocketError, protocol::StreamParser};
use futures::Stream;
use pin_project::pin_project;
use std::{
    collections::VecDeque,
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};

/// An [`ExchangeStream`] is a communication protocol agnostic [`Stream`]. It polls protocol
/// messages from the inner [`Stream`], and transforms them into the desired output data structure.
///
/// Requires both the `stream` and `protocol` features.
#[derive(Debug)]
#[pin_project]
pub struct ExchangeStream<Protocol, InnerStream, StreamTransformer>
where
    Protocol: StreamParser<StreamTransformer::Input>,
    InnerStream: Stream,
    StreamTransformer: Transformer,
{
    #[pin]
    pub stream: InnerStream,
    pub transformer: StreamTransformer,
    pub buffer: VecDeque<Result<StreamTransformer::Output, StreamTransformer::Error>>,
    pub protocol_marker: PhantomData<Protocol>,
}

impl<Protocol, InnerStream, StreamTransformer> Stream
    for ExchangeStream<Protocol, InnerStream, StreamTransformer>
where
    Protocol: StreamParser<StreamTransformer::Input>,
    InnerStream: Stream<Item = Result<Protocol::Message, Protocol::Error>> + Unpin,
    StreamTransformer: Transformer,
    StreamTransformer::Error: From<SocketError>,
{
    type Item = Result<StreamTransformer::Output, StreamTransformer::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            // Flush Self::Item buffer if it is not currently empty
            if let Some(output) = self.buffer.pop_front() {
                return Poll::Ready(Some(output));
            }

            // Poll inner `Stream` for next the next input protocol message
            let input = match self.as_mut().project().stream.poll_next(cx) {
                Poll::Ready(Some(input)) => input,
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            };

            // Parse input protocol message into `ExchangeMessage`
            let exchange_message = match Protocol::parse(input) {
                // `StreamParser` successfully deserialised `ExchangeMessage`
                Some(Ok(exchange_message)) => exchange_message,

                // If `StreamParser` returns an Err pass it downstream
                Some(Err(err)) => return Poll::Ready(Some(Err(err.into()))),

                // If `StreamParser` returns None it's a safe-to-skip message
                None => continue,
            };

            // Transform `ExchangeMessage` into `Transformer::OutputIter`
            // ie/ IntoIterator<Item = Result<Output, SocketError>>
            self.transformer
                .transform(exchange_message)
                .into_iter()
                .for_each(
                    |output_result: Result<StreamTransformer::Output, StreamTransformer::Error>| {
                        self.buffer.push_back(output_result)
                    },
                );
        }
    }
}

impl<Protocol, InnerStream, StreamTransformer>
    ExchangeStream<Protocol, InnerStream, StreamTransformer>
where
    Protocol: StreamParser<StreamTransformer::Input>,
    InnerStream: Stream,
    StreamTransformer: Transformer,
{
    pub fn new(
        stream: InnerStream,
        transformer: StreamTransformer,
        buffer: VecDeque<Result<StreamTransformer::Output, StreamTransformer::Error>>,
    ) -> Self {
        Self {
            stream,
            transformer,
            buffer,
            protocol_marker: PhantomData,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;

    /// Parses `0` as a message to skip, a negative number as a parse error, and anything else as
    /// itself.
    struct SkipZeroParser;

    impl StreamParser<i32> for SkipZeroParser {
        type Stream = futures::stream::Empty<i32>;
        type Message = i32;
        type Error = SocketError;

        fn parse(input: Result<i32, SocketError>) -> Option<Result<i32, SocketError>> {
            match input {
                Ok(0) => None,
                Ok(n) if n < 0 => Some(Err(SocketError::Sink)),
                other => Some(other),
            }
        }
    }

    /// Transforms each message into two outputs, so the buffer has to hold the second.
    struct TwoOutputs;

    impl Transformer for TwoOutputs {
        type Error = SocketError;
        type Input = i32;
        type Output = i32;
        type OutputIter = Vec<Result<i32, SocketError>>;

        fn transform(&mut self, input: i32) -> Self::OutputIter {
            vec![Ok(input), Ok(input * 10)]
        }
    }

    #[tokio::test]
    async fn exchange_stream_parses_skips_and_buffers_until_the_inner_stream_ends() {
        let inner = futures::stream::iter([Ok(1), Ok(0), Ok(-1), Ok(2)]);
        let stream =
            ExchangeStream::<SkipZeroParser, _, _>::new(inner, TwoOutputs, VecDeque::new());

        let outputs = stream
            .map(|result| result.map_err(|error| error.to_string()))
            .collect::<Vec<_>>()
            .await;

        assert_eq!(
            outputs,
            vec![
                Ok(1),
                Ok(10),
                Err(SocketError::Sink.to_string()),
                Ok(2),
                Ok(20),
            ]
        );
    }
}
