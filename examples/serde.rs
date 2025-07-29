use arrow::datatypes::{DataType, Field, Schema};
use arrow::json::ReaderBuilder;
use arrow_json::writer::ArrayWriter;
use serde_json::Value;
use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;
use arrow::record_batch::RecordBatch;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Read JSON file
    let file = File::open("examples/records.json")?;
    let reader = BufReader::new(file);
    let json_value: Value = serde_json::from_reader(reader)?;
    let json_array = json_value.as_array().unwrap();

    // Manually define Arrow schema
    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "measurements", 
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::List(Arc::new(Field::new("item", DataType::Float64, true))),
                true,
            ))),
            true,
        ),
        Field::new("name", DataType::Utf8, true),
        Field::new("pressure", DataType::Float64, true),
        Field::new("temp", DataType::Float64, true),
    ]));

    // Print schema
    println!("Inferred Arrow Schema:\n{schema:#?}\n");
    
    // Create JSON decoder
    let mut decoder = ReaderBuilder::new(schema.clone())
        .build_decoder()?;

    // Serialize JSON data to Arrow
    decoder.serialize(json_array)?;
    let batch: RecordBatch = decoder.flush()?.unwrap();
    println!("Arrow RecordBatch:\n{batch:#?}\n");
    
    // Create JSON writer
    let mut writer = ArrayWriter::new(vec![]);
    
    // Write the RecordBatch to JSON
    writer.write(&batch)?;
    writer.finish()?;
    
    // Get the JSON bytes and print them
    let json_data = writer.into_inner();
    let json_output = String::from_utf8(json_data)?;
    println!("Serialized JSON:\n{}", json_output);

    Ok(())
}
