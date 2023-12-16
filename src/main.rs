mod table;

use std::io::prelude::*;
use std::fs::File;
use std::path::Path;

use table::schema::{TableDescriptor, ColumnDataType, DatabaseDescriptor};
use table::store::InMemoryByteStore;
use table::query::{SelectQuery};
use table::bytes::{ToNativeType};
use table::query::lex::RawSelectQuery;

use crate::table::db::Database;

fn run_db() {
    let mut db = Database::new("my_db");
    db.add_table(TableDescriptor::new("books", vec![
        ("id", ColumnDataType::SerialId),
        ("author", ColumnDataType::Byte(64)),
        ("title", ColumnDataType::Byte(64)),
        ("year_published", ColumnDataType::Int32),
        ("us_based_publisher", ColumnDataType::Boolean)
    ]).unwrap()).unwrap();

    let insertions: Vec<Vec<(&str, &str)>> = vec![
        vec![
            ("author", "Billy Bob"),
            ("title", "How to Sting Like a Bee"),
            ("year_published", "1932")
        ],
        vec![
            ("author", "Stink Williams"),
            ("title", "Singing for Frogs"),
            ("year_published", "1921")
        ],
        vec![
            ("author", "Stink Williams"),
            ("title", "Singing for Woodland Creatures"),
            ("year_published", "1923")
        ],
        vec![
            ("author", "Stink Williams"),
            ("title", "Dancing for the Everyday Man"),
            ("year_published", "1937"),
            ("us_based_publisher", "true")
        ],
        vec![
            ("author", "Stink Williams"),
            ("title", "O My Friend, How Art Thee"),
            ("year_published", "1924")
        ],
        vec![
            ("author", "Stink Williams"),
            ("title", "Singing for Woodland Creatures"),
            ("year_published", "1923")
        ],
        vec![
            ("author", "joseph"),
            ("title", "My Lumps My Bumps"),
            ("year_published", "1917")
        ]
    ];

    for ins in insertions {
        db.insert_columns("books", &ins[..]).unwrap();
    }

    let select_query = SelectQuery::parse_raw_query_against_db("select id, title, author, year_published, us_based_publisher from books where year_published >= 1930", &db).unwrap();
    let res = db.query(&select_query);
    dbg!(res);
}


fn main() {
    run_db();
}