use std::env::args;
use std::fs::File;
use std::io::BufWriter;
use csv::Writer;
use arrow::compute::kernels::{numeric, rank};
use parquet::arrow::arrow_reader::RowFilter;
use parquet::record::reader::RowIter;
use parquet::record::{Row, reader};
use parquet::schema::types::Type;
use parquet::file::reader::{FileReader,SerializedFileReader};
use arrow::array::{Int64Array, StringArray};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::record::Field;
use std::error::Error;
use csv::WriterBuilder;

fn print_help() {
    println!("pq - Parquet file tools");
    println!("\nUsage:");
    println!("  pq schema <file.parquet>              - Show schema");
    println!("  pq head <file.parquet> <rows> [--csv filename] - Show first N rows");
    println!("  pq columns <file.parquet>             - List columns");
    println!("  pq count <file.parquet>               - Count rows");
    println!("  pq export <file.parquet> --output <file.csv> - Export to CSV");
    println!("\nExamples:");
    println!("  pq head data.parquet 5");
    println!("  pq head data.parquet 5 --csv output.csv");
    println!("  pq export data.parquet --output data.csv");
}
fn main() {
    let args:Vec<String>=args().collect();
    

    if args.len() < 2 || args[1] == "--help" || args[1] == "-h" {
        print_help();
        return;
    }
    

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
        },
        "export"=>{
            export_command(&args[1..]);
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



fn export_command(parts:&[String]){
    if parts[2]!="--output"{
        panic!("incorrect command for export");
    }
    let mut pairs= parts[3].split(".");
    let mut file_name="";
    if let Some(fl_name)=pairs.next(){
        if let Some(file_type)=pairs.next(){
            if file_type!="csv"{
                panic!("now this type for export not support,{:?}",file_type);
            }
            file_name=fl_name;
        }
    }
    let file=std::fs::File::open(&parts[1]).unwrap();
    let reader=SerializedFileReader::new(file).unwrap();
    

    let num_groups=reader.num_row_groups();

    let csv_file=File::create(format!("{}.csv",file_name)).unwrap();
    let mut csv_writer = WriterBuilder::new()
    .has_headers(false)
    .from_writer(BufWriter::with_capacity(128 * 1024, csv_file));
    
        
    let mut headers_written = false;
    let mut total_rows = 0;
    for group_idx in 0..num_groups{
        let row_group=match reader.get_row_group(group_idx){
            Ok(group)=>group,
            Err(e)=>{
                eprintln!("Failed to get row group:{}",e);
                return;
            }
        };
        let row_iter=match row_group.get_row_iter(None){
            Ok(iter)=>iter,
            Err(e)=>{
                eprintln!("Failed to get row iterator:{}",e);
                return;
            }
        };

        for row_result in row_iter{
            match row_result{
                Ok(row)=>{
                    if !headers_written{
                        let headers:Vec<String>=row.get_column_iter()
                            .map(|(name,_)|name.clone())
                            .collect();
                        if let Err(e)=csv_writer.write_record(&headers){
                            eprintln!("Failed to write headers: {}", e);
                            return;
                        }
                        headers_written=true;
                        
                    }
                    let csv_record=convert_to_csv(&row);
                    if let Err(e) = csv_writer.write_record(&csv_record) {
                        eprintln!("Failed to write row: {}", e);
                        return;
                    }
                    total_rows += 1;
                    
                    if total_rows % 100000 == 0 {
                        println!("Processed {} rows...", total_rows);
                        if let Err(e) = csv_writer.flush() {
                            eprintln!("Failed to flush: {}", e);
                        }
                    }

                },
                Err(e)=>{
                     eprintln!("Error reading row: {}", e);
                    return;
                }
            }
           
        }

    }
      if let Err(e) = csv_writer.flush() {
        eprintln!("Failed final flush: {}", e);
    } else {
        println!("Export completed! Written {} rows to {}.csv", total_rows, file_name);
    }


}

fn convert_to_csv(row:&Row)->Vec<String>{
    let mut record=Vec::new();
    for(_,value)in row.get_column_iter(){
          let value_str = match value {
            Field::Str(s) => s.clone(),
            Field::Long(i) => i.to_string(),
            Field::Int(i) => i.to_string(),
            Field::Double(f) => f.to_string(),
            Field::Float(f) => f.to_string(),
            Field::Bool(b) => b.to_string(),
            Field::Null => "".to_string(),
            
            Field::Group(group_fields) => format!("{:?}", group_fields),
            _ => format!("{:?}", value),
        };
        record.push(value_str);
    }
    record

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