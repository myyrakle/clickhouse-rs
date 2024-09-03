use clickhouse_derive::Row;
use serde::{Deserialize, Serialize};

use clickhouse::sql::Identifier;
use clickhouse::{error::Result, Client};

#[tokio::main]
async fn main() -> Result<()> {
    let table_name = "chrs_nested";
    let client = Client::default().with_url("http://localhost:8123");
    client
        .query(
            "
            CREATE OR REPLACE TABLE ?(
                id UInt32,
                items Nested(
                    name String,
                    count UInt32
                )
            ) ORDER BY id
            ",
        )
        .bind(Identifier(table_name))
        .execute()
        .await?;

    #[derive(Debug, Row, Serialize, Deserialize)]
    struct MyRow {
        id: u32,
        #[serde(rename = "items.name")]
        items_name: Vec<String>,
        #[serde(rename = "items.count")]
        items_count: Vec<u32>,
    }

    let row1 = MyRow {
        id: 1,
        items_name: vec!["foo".to_string(), "bar".to_string()],
        items_count: vec![1, 2],
    };
    let row2 = MyRow {
        id: 2,
        items_name: vec!["baz".to_string(), "qux".to_string()],
        items_count: vec![3, 4],
    };
    let rows = vec![row1, row2];

    let mut insert = client.insert(table_name)?;
    for row in &rows {
        insert.write(row).await?;
    }
    insert.end().await?;

    let rows = client
        .query("SELECT ?fields FROM ? ORDER BY id ASC")
        .bind(Identifier(table_name))
        .fetch_all::<MyRow>()
        .await?;

    println!("{rows:#?}");
    Ok(())
}
