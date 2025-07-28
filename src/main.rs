use std::io::Error;
use std::time::Duration;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::{Stream, StreamExt};

// #[tokio::main]
fn main() {
    let builder = rtshark::RTSharkBuilder::builder()
        .input_path("loopback")
        .live_capture()
        .capture_filter("icmp");

    let mut rtshark = builder.spawn().expect("Failed to start RTShark");

    tokio::runtime::Runtime::new()
        .expect("Failed to create Tokio runtime")
        .block_on(async move {
            // Await the future to get the stream
            // let stream = rtshark.stream().await.timeout(Duration::from_millis(10));
            // let mut messages = std::pin::pin!(stream);

            let messages = rtshark.stream().await.timeout(Duration::from_millis(200));
            let intervals = co_process_count()
                .map(|count| format!("Interval: {count}"))
                .throttle(Duration::from_millis(100))
                .timeout(Duration::from_secs(20));
            let merged = messages.merge(intervals).take(30);
            let mut stream = std::pin::pin!(merged);

            while let Some(result) = stream.next().await {
                match result {
                    Ok(message) => println!("{message}"),
                    Err(reason) => eprintln!("Problem: {reason:?}"),
                }
            }
        });
}

#[derive(Debug)]
enum StreamItem {
    Net(Result<String, Error>),
    Count(u32),
}

fn co_process_count() -> impl Stream<Item = u32> {
    let x = tokio::sync::mpsc::channel(1);
    tokio::spawn(async move {
        let mut count = 0;
        loop {
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            count += 1;
            x.0.send(count).await.expect("Failed to send count");
        }
    });
    ReceiverStream::new(x.1)
}
