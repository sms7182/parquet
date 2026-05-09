use std::env::args;
use csv::Writer;
use arrow::compute::kernels::{numeric, rank};
use parquet::record::{Row, reader};
use parquet::schema::types::Type;
use parquet::file::reader::{FileReader,SerializedFileReader};
use arrow::array::{Int64Array, StringArray};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::record::Field;
use std::error::Error;

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

   }else if parts.len()>5 {
       println!("{}:{}",red("Unexpected argument:"),parts[3]);
       std::process::exit(1);
   }
   else if parts.len()==2{
    
    println!("{}",red("Error: missing number of rows"));
    std::process::exit(1);
}
   else{
    let mut csv_create=false;
    let mut csv_file="";
    if parts.len()==5{
        let mut pairs= parts[3].split("--");
        if let Some(space)=pairs.next() {
             csv_create=true; 
             if space==""{
                if let Some(value)=pairs.next(){
                    if value=="csv"{
                        csv_file=parts[4].as_str();
                    }
                    
                }
                else{
                println!("{}:{}",red("Unexpected argument:"),parts[4]);
                }
                
             }
        }
    }
    let file=std::fs::File::open(&parts[1]).unwrap();
    let reader=SerializedFileReader::new(file).unwrap();
    let schema=reader.metadata().file_metadata().schema();
    let fields=schema.get_fields();
    let mut row_iter = reader.get_row_iter(None).expect("Failed to get row iterator");
    let num_rows: usize=parts[2].parse().unwrap();
    let mut vls:Vec<String>=vec![];

    println!("{:-<50}","");
    for field in fields{
        print!("| {} ",green(field.name()));
        vls.push(field.name().to_string());
    }
    print!("|");
    println!("");
    println!("{:-<50}","");
    let mut all_rows: Vec<Vec<String>> = Vec::new();
    for _ in 0..num_rows{
        match row_iter.next() {
            Some(Ok(row))=>{
                let mut row_values=Vec::new();
                inspect_row_detail(&row,&mut row_values);
                all_rows.push(row_values);
                println!("|");
                println!("{:-<50}","");
               
                
            },
            Some(Err(e))=>panic!("Error reading row:{}",e),
            None=>break,
        }
    }
    if csv_create{
       let mut file_name=format!("{}.csv",csv_file);
        create_csv(&file_name, &vls,&all_rows);
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





fn inspect_row_detail(row:&Row, vls:&mut Vec<String>){
    for (_,value) in row.get_column_iter(){
        match value {
            Field::Str(s) => {
                print!("| '{}' ", s);
                vls.push(s.clone());
            }
            Field::Long(i) => {
                print!("| {} ", i);
                vls.push(i.to_string());
            }
            Field::Double(f) => {
                print!("| {} ", f);
                 vls.push(f.to_string());
            }
            Field::Bool(b) => {
                print!("| {} ", b);
                 vls.push(b.to_string());
            }
            Field::Null => {
                print!("|  NULL value");
                vls.push("NULL".to_string());
            }
            Field::Group(group_fields) => {
                print!("| {} ", group_fields.len());
                vls.push(format!("Group({})", group_fields.len()));
            }
            _ => {
                print!("|  Other type: {:?}", value);
                vls.push(format!("{:?}", value));
        },
        }
    }
    
}



fn green(text: &str) -> String {
    format!("\x1b[32m{}\x1b[0m", text)
}

fn red(text: &str) -> String {
    format!("\x1b[31m{}\x1b[0m", text)
}

fn create_csv(file_name:&str,headers:&[String],rows:&[Vec<String>])->Result<(),Box<dyn Error>>{
    let mut wtr=Writer::from_path(file_name)?;
    wtr.write_record(headers)?;
    for row in rows{
        wtr.write_record(row)?
    }
    wtr.flush();
    Ok(())
}