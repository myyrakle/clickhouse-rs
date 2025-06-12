use clickhouse::{Client, Compression};

use crate::{create_simple_table, SimpleRow};

// schema for `id UInt64, data String` will always be encoded with 23 bytes
const RBWNAT_HEADER_SIZE: u64 = 23;

async fn check(client: Client, expected_ratio: f64, first_chunk_size: u64) {
    create_simple_table(&client, "test").await;

    let mut insert = client.insert("test").unwrap();
    for i in 0..1_000 {
        insert.write(&SimpleRow::new(i, "foobar")).await.unwrap();
    }
    insert.end().await.unwrap();

    let mut cursor = client
        .query("SELECT * FROM test")
        .fetch::<SimpleRow>()
        .await
        .unwrap();

    let mut received = cursor.received_bytes();
    let mut decoded = cursor.decoded_bytes();
    assert_eq!(dbg!(received), first_chunk_size);
    assert_eq!(dbg!(decoded), RBWNAT_HEADER_SIZE);

    while cursor.next().await.unwrap().is_some() {
        assert!(cursor.received_bytes() >= received);
        assert!(cursor.decoded_bytes() >= decoded);
        received = cursor.received_bytes();
        decoded = cursor.decoded_bytes();
    }

    assert_eq!(decoded, 15000 + RBWNAT_HEADER_SIZE);
    assert_eq!(cursor.received_bytes(), dbg!(received));
    assert_eq!(cursor.decoded_bytes(), dbg!(decoded));
    assert_eq!(
        (decoded as f64 / received as f64 * 10.).round() / 10.,
        expected_ratio
    );
}

#[tokio::test]
async fn none() {
    let client = prepare_database!().with_compression(Compression::None);
    check(client, 1.0, 23).await;
}

#[cfg(feature = "lz4")]
#[tokio::test]
async fn lz4() {
    let client = prepare_database!().with_compression(Compression::Lz4);
    check(client, 3.7, 50).await;
}
