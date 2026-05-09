use std::env::args;

use arrow::compute::kernels::{numeric, rank};
use parquet::record::{Row, reader};
use parquet::schema::types::Type;
use parquet::file::reader::{FileReader,SerializedFileReader};
use arrow::array::{Int64Array, StringArray};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::record::Field;
use std::sync::Arc;
use std::fs::File;
fn tmain() {
    // Create some data
    let ids = Int64Array::from(vec![1, 2, 3]);
    let names = StringArray::from(vec!["Alice", "Bob", "Charlie"]);
    let ages = Int64Array::from(vec![25, 30, 35]);
    
    // Create record batch
    let batch = RecordBatch::try_from_iter(vec![
        ("id", Arc::new(ids) as Arc<dyn arrow::array::Array>),
        ("name", Arc::new(names)),
        ("age", Arc::new(ages)),
    ]).unwrap();
    
    // Write to file
    let file = File::create("test.parquet").unwrap();
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    
    println!("Created test.parquet successfully!");
}

fn main() {
    let args:Vec<String>=args().collect();
    
    if args.len()==1{
        println!("{}",green("Usage: pq <command> <file> [options]"));
        println!("{}",green("Commands: "));
        println!("{}",green("  schema <file> Show schema of a Parquet file"));
        println!("{}",green("  head <file> Show first N rows of a Parquet file"));
    }
    match args[1].as_str(){
        "schema"=>{
            schema_command(&args[1..]);
        },
        "head"=>{
            head_command(&args[1..])
        },
        "count"=>{
            count_command(&args[1..]);
        },
        "columns"=>{
            columns_command(&args[1..]);
        }
        _=>{
            eprintln!("{}:{}",red("unknown command "),red(&args[1]));
            std::process::exit(1);
        }
    }

  
}

fn schema_command(parts:&[String]){
  if parts.len()==1{
    println!("{}",red("Error: missing file path"));
       std::process::exit(1);

   }else if parts.len()>2 {
       println!("{}:{}",red("Unexpected argument: "),parts[2]);
       std::process::exit(1);
   }
   else{
    let file=std::fs::File::open(&parts[1]).unwrap();
    let reader=SerializedFileReader::new(file).unwrap();
    let schema=reader.metadata().file_metadata().schema();
    let fields=schema.get_fields();
    println!("Columns in the Parquet file:");
    println!("{:-<50}","");
    for field in fields{
        let name=field.name();
       let physical_type = match field.as_ref() {
            Type::PrimitiveType { physical_type, .. } => {
                format!("{:?}", physical_type)
            }
            Type::GroupType { .. } => {
                "GROUP".to_string()
            }
        };
        
        
        let basic_info = field.get_basic_info();
        
        println!("- {}: {}", name, physical_type);
        if let Some(logical) = basic_info.logical_type_ref() {
            println!("  (Logical type: {:?})", logical);
        }
    }
    
    
   }
}

fn head_command(parts:&[String]){
   if parts.len()==1{
    println!("{}",red("Error: missing file path"));
    std::process::exit(1);

   }else if parts.len()>3 {
       println!("{}:{}",red("Unexpected argument:"),parts[3]);
       std::process::exit(1);
   }
   else if parts.len()==2{
    
    println!("{}",red("Error: missing number of rows"));
    std::process::exit(1);
}
   else{
    
    // println!("Show first {} rows from {}",green(parts[2].as_str()),green(parts[1].as_str()));
    let file=std::fs::File::open(&parts[1]).unwrap();
    let reader=SerializedFileReader::new(file).unwrap();
    let schema=reader.metadata().file_metadata().schema();
    let fields=schema.get_fields();
    
    println!("{:-<50}","");
    for field in fields{
        print!("| {} ",green(field.name()));
        
    }
    print!("|");
    println!("");
    println!("{:-<50}","");
    let mut row_iter = reader.get_row_iter(None).expect("Failed to get row iterator");
    let num_rows: usize=parts[2].parse().unwrap();
    let mut results:Vec<parquet::record::Row> = Vec::with_capacity(num_rows);
    for _ in 0..num_rows{
        match row_iter.next() {
            Some(Ok(row))=>{
                inspect_row_detail(&row);
                println!("|");
                println!("{:-<50}","");


            },
            Some(Err(e))=>panic!("Error reading row:{}",e),
            None=>break,
        }
    }
   }
}

fn count_command(parts:&[String]){
    let file=std::fs::File::open(&parts[1]).unwrap();
    let reader=SerializedFileReader::new(file).unwrap();
    let count=reader.metadata().file_metadata().num_rows();
    println!("Count of rows :{:?}",count);
}

fn columns_command(parts:&[String]){
    let file=std::fs::File::open(&parts[1]).unwrap();
    let reader=SerializedFileReader::new(file).unwrap();
    let schema=reader.metadata().file_metadata().schema();
    let fields=schema.get_fields();
    
    for field in fields{
        println!("{} ",green(field.name()));
    }
   
}





fn inspect_row_detail(row:&Row){
    for (col_name,value) in row.get_column_iter(){
        match value {
            Field::Str(s) => {
                print!("| '{}' ", s);
            }
            Field::Long(i) => {
                print!("| {} ", i);
            }
            Field::Double(f) => {
                print!("| {} ", f);
            }
            Field::Bool(b) => {
                print!("| {} ", b);
            }
            Field::Null => {
                print!("|  NULL value");
            }
            Field::Group(group_fields) => {
                print!("| {} ", group_fields.len());
            }
            _ => print!("|  Other type: {:?}", value),
        }
    }
}



fn green(text: &str) -> String {
    format!("\x1b[32m{}\x1b[0m", text)
}

fn red(text: &str) -> String {
    format!("\x1b[31m{}\x1b[0m", text)
}

