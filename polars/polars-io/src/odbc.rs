use arrow::chunk::Chunk;
use arrow::io::odbc::{read, write};
use arrow::io::odbc::api;
use arrow::io::odbc::api::Cursor;
use arrow::error::Result;
use arrow::array::{Array, Int32Array, Utf8Array};

pub fn read_with_query(query: String, connection_str: String) {
    let env = arrow::io::odbc::api::Environment::new()?;
    let connection = env.connect_with_connection_string(connection_str)?;

    connection.execute(query, )
}

fn read(query: String, connection_str: String) -> Result<()> {
    let env = arrow::io::odbc::api::Environment::new()?;
    let connection = env.connect_with_connection_string(connection_str)?;
    let mut prepared = connection.prepare(query).unwrap();

    let schema = read::infer_schema(&prepared)?;
    let mut df = schema
        .iter()
        .map(|s| {
            let typ = DataType::from(s.data_type());
            Series::new_empty(&s.name, &typ)
        })
        .collect::<Vec<_>>();

    let max_batch_size = 100;
    let buffer = read::buffer_from_metadata(&prep, max_batch_size)?;

    let cursor = prep.execute(())?.unwrap();
    let mut cursor = cursor.bind_buffer(buffer)?;

    while let Some(batch) = cursor.fetch()? {
        for ((idx, field), df_elem) in (0..batch.num_cols()).zip(fields.iter()).zip(df.iter_mut()) {
            let column_view = batch.column(idx);
            let arr = Arc::from(read::deserialize(column_view, field.data_type.clone()));
            let series = Series::try_from((field.name.as_str(), vec![arr])).unwrap();
            df_elem.append(&series).unwrap();
        }
    }

    let dataframe = DataFrame::new(df).unwrap();
    dbg!(dataframe);
}

pub fn read(connection: &api::Connection<'_>, query: &str) -> Result<Vec<Chunk<Box<dyn Array>>>> {
    let a = connection.execute(query)?;
    let fields = read::infer_schema(&a)?;

    let max_batch_size = 100;
    let buffer = read::buffer_from_metadata(&a, max_batch_size)?;

    let cursor = a.execute(())?.unwrap();
    let mut cursor = cursor.bind_buffer(buffer)?;

    let mut chunks = vec![];
    while let Some(batch) = cursor.fetch()? {
        let arrays = (0..batch.num_cols())
            .zip(fields.iter())
            .map(|(index, field)| {
                let column_view = batch.column(index);
                read::deserialize(column_view, field.data_type.clone())
            })
            .collect::<Vec<_>>();
        chunks.push(Chunk::new(arrays));
    }

    Ok(chunks)
}