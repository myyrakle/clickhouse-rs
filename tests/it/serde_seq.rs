#![cfg(all(feature="uuid", feature="time"))]

use clickhouse::sql::Identifier;
use clickhouse::Row;
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::net::Ipv4Addr;
use std::ops::Add;
use std::str::FromStr;
use std::time::Duration;
use time::Month;
use uuid::Uuid;

#[tokio::test]
async fn serde_seq() {
    let client = prepare_database!();
    let table_name = "chrs_serde_seq";

    #[derive(Debug, PartialEq, Row, Serialize, Deserialize)]
    struct MyRow {
        id: u32,
        #[serde(with = "clickhouse::serde::ipv4::seq")]
        ipv4_seq: Vec<Ipv4Addr>,
        #[serde(with = "clickhouse::serde::uuid::seq")]
        uuid_seq: Vec<Uuid>,
        #[serde(with = "clickhouse::serde::time::date::seq")]
        date_seq: Vec<time::Date>,
        #[serde(with = "clickhouse::serde::time::date32::seq")]
        date32_seq: Vec<time::Date>,
        #[serde(with = "clickhouse::serde::time::datetime::seq")]
        datetime_seq: Vec<time::OffsetDateTime>,
        #[serde(with = "clickhouse::serde::time::datetime64::secs::seq")]
        datetime64_0_seq: Vec<time::OffsetDateTime>,
        #[serde(with = "clickhouse::serde::time::datetime64::millis::seq")]
        datetime64_3_seq: Vec<time::OffsetDateTime>,
        #[serde(with = "clickhouse::serde::time::datetime64::micros::seq")]
        datetime64_6_seq: Vec<time::OffsetDateTime>,
        #[serde(with = "clickhouse::serde::time::datetime64::nanos::seq")]
        datetime64_9_seq: Vec<time::OffsetDateTime>,
    }

    let row = MyRow {
        id: 1,
        ipv4_seq: vec![
            Ipv4Addr::from_str("192.168.0.1").unwrap(),
            Ipv4Addr::from_str("127.0.0.1").unwrap(),
        ],
        uuid_seq: vec![Uuid::new_v4(), Uuid::new_v4()],
        date_seq: vec![
            time::Date::from_calendar_date(2021, Month::January, 1).unwrap(),
            time::Date::from_calendar_date(2022, Month::April, 4).unwrap(),
        ],
        date32_seq: vec![
            time::Date::from_calendar_date(2150, Month::January, 1).unwrap(),
            time::Date::from_calendar_date(1900, Month::April, 4).unwrap(),
        ],
        datetime_seq: vec![gen_offset_datetime(0), gen_offset_datetime(0)],
        datetime64_0_seq: vec![gen_offset_datetime(0), gen_offset_datetime(0)],
        datetime64_3_seq: vec![
            gen_offset_datetime(123_000_000),
            gen_offset_datetime(456_000_000),
        ],
        datetime64_6_seq: vec![
            gen_offset_datetime(123_456_000),
            gen_offset_datetime(456_789_000),
        ],
        datetime64_9_seq: vec![
            gen_offset_datetime(123_456_789),
            gen_offset_datetime(456_789_123),
        ],
    };

    client
        .query(
            "
            CREATE TABLE ?(
                id               UInt32,
                ipv4_seq         Array(IPv4),
                uuid_seq         Array(UUID),
                date_seq         Array(Date),
                date32_seq       Array(Date32),
                datetime_seq     Array(DateTime('UTC')),
                datetime64_0_seq Array(DateTime64(0, 'UTC')),
                datetime64_3_seq Array(DateTime64(3, 'UTC')),
                datetime64_6_seq Array(DateTime64(6, 'UTC')),
                datetime64_9_seq Array(DateTime64(9, 'UTC'))
            )
            ENGINE = MergeTree
            ORDER BY id
            ",
        )
        .bind(Identifier(table_name))
        .execute()
        .await
        .unwrap();

    let mut insert = client.insert(table_name).unwrap();
    insert.write(&row).await.unwrap();
    insert.end().await.unwrap();

    let db_row = client
        .query("SELECT ?fields FROM ? ORDER BY id ASC LIMIT 1")
        .bind(Identifier(table_name))
        .fetch_one::<MyRow>()
        .await
        .unwrap();

    assert_eq!(db_row, row);
}

fn gen_offset_datetime(nanos: u32) -> time::OffsetDateTime {
    let duration = Duration::from_secs(rand::thread_rng().gen_range(0..100));
    time::OffsetDateTime::now_utc()
        .add(duration)
        .replace_nanosecond(nanos)
        .unwrap()
}
