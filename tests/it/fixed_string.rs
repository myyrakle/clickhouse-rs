use serde::{Deserialize, Serialize};

use clickhouse::sql::Identifier;
use clickhouse::Row;

#[tokio::test]
async fn fixed_string_serde() {
    let client = prepare_database!();
    let table_name = "chrs_fixed_string_serde";

    #[derive(Debug, Row, Serialize, Deserialize)]
    struct MyRow {
        id: u32,
        #[serde(serialize_with = "clickhouse::serde::serialize_fixed_string::<_, 6>")]
        #[serde(deserialize_with = "clickhouse::serde::deserialize_fixed_string::<_, 6>")]
        fixed_str: String,
    }

    let row1 = MyRow {
        id: 1,
        fixed_str: "foo".into(),
    };
    // let row2 = MyRow {
    //     id: 2,
    //     fixed_str: "foobar".into(),
    // };
    // let row3 = MyRow {
    //     id: 3,
    //     fixed_str: "".into(),
    // };
    // let row4 = MyRow {
    //     id: 4,
    //     fixed_str: "€".into(), // € = 3 bytes
    // };
    // let row5 = MyRow {
    //     id: 5,
    //     fixed_str: "И€".into(), // И = 2 bytes; 5 bytes total
    // };
    // let row6 = MyRow {
    //     id: 6,
    //     fixed_str: "€€".into(), // exactly 6 bytes
    // };
    let rows = vec![
        row1,
        // row2, row3, row4, row5, row6
    ];

    // Create a table.
    client
        .query(
            "
            CREATE TABLE ?(id UInt32, fixed_str FixedString(6))
            ENGINE = MergeTree
            ORDER BY id
            ",
        )
        .bind(Identifier(table_name))
        .execute()
        .await
        .unwrap();

    // Write to the table.
    let mut insert = client.insert(table_name).unwrap();
    for row in &rows {
        insert.write(row).await.unwrap();
    }
    insert.end().await.unwrap();

    // Read from the table.
    let db_rows = client
        .query("SELECT ?fields FROM ? ORDER BY id ASC")
        .bind(Identifier(table_name))
        .fetch_all::<MyRow>()
        .await
        .unwrap();

    assert_eq!(db_rows.len(), rows.len());
    for (db_row, row) in db_rows.iter().zip(rows.iter()) {
        assert_eq!(db_row.id, row.id);
        assert_eq!(db_row.fixed_str, row.fixed_str);
    }
}
