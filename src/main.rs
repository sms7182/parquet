use std::env::args;
use std::fs::File;
use std::io::BufWriter;

use std::io::Write;
use csv::Writer;
use parquet::arrow::push_decoder::NoInput;
use parquet::file::writer::SerializedFileWriter;
use parquet::record::{Row, reader,RowAccessor};
use parquet::record::Field;       
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::schema::types::{SchemaDescPtr, SchemaDescriptor, Type};
use parquet::file::reader::{FileReader,SerializedFileReader};
use tokio_postgres::{Client, NoTls, Error};
use anyhow::Result;
use csv::WriterBuilder;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug)]
struct  Condition{
    column:String,
    operator:String,
    value:String,
}

#[derive(Debug)]
struct Expression{
    conditions:Vec<Condition>,
    operators:Vec<LogicOp>
}

#[derive(Debug)]
enum LogicOp{
    And,
    Or,
    None
}

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
    println!("  pq filter  data.parquet 'col1>=50 and col2=20 or col3=tehran--result.csv' ");

}
#[tokio::main]
async fn main() {
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
        },
        "filter"=>{
           
            filter_command(&args[1..]);
        },
        "dump-postgres"=>{
            if let Err(er)=dump_postgres_command(&args[1..]).await{

            };
        },

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


 async fn dump_postgres_command(second_parts:&[String])->Result<(),Box<dyn std::error::Error>>{
    if second_parts[2]!="--conn"{
        panic!("second param for connection to postgres incorrect");
    }
    if second_parts[4]!="--table"{
        panic!("table unknown command");
    }


    
    
    let mut query=second_parts[5].clone();
    //using hold cursor in postgres

    Ok(())
}   

fn filter_command(second_parts:&[String]){
  eprintln!("{:?}",second_parts);

    let file=File::open(&second_parts[1]).unwrap();
    let reader=SerializedFileReader::new(file).unwrap();
  
   let mut indx=0;
   let mut expression=Expression{
    conditions:vec![],
    operators:vec![]
   };
   if !second_parts[3].contains("--"){
    panic!("result unknown");
   }
   let mut output=second_parts[3].replace("--","");
   



   let mut parts:Vec<&str> =second_parts[2].split(" ").collect();
  
   while indx<=parts.len() {
       
       if parts[indx].contains("--"){
        let result=parts[indx].replace("--","");
        
       
        match parts[indx].clone(){
            s if s.contains(">=")=>{
                let mut pairs=s.split(">=");
                if let Some(field)=pairs.next(){
                    if let Some(value)=pairs.next(){
            
                let mut condition=create_condition(field.replace("--","").as_str(),">=",value);
                expression.conditions.push(condition);
                   match extract_operator(indx, &parts){
                    LogicOp::None=>{
                        break;
                    },
                    d=>expression.operators.push(d)
                }
                 indx=indx+2;
              
                continue;
            }
            }
                
            },
            s if s.contains("<=")=>{
             let mut pairs=s.split("<=");
                if let Some(field)=pairs.next(){
                    if let Some(value)=pairs.next(){
                println!("field is {} and value is {}",field,value);
                let mut condition=create_condition(field.replace("--","").as_str(),"<=",value);
                expression.conditions.push(condition);
                   match extract_operator(indx, &parts){
                    LogicOp::None=>{
                        break;
                    },
                    d=>expression.operators.push(d)
                }
                 indx=indx+2;
              
                continue;
            }
            }
            },
            s if s.contains(">")=>{
             let mut pairs=s.split(">");
                if let Some(field)=pairs.next(){
                    if let Some(value)=pairs.next(){
                println!("field is {} and value is {}",field,value);
                let mut condition=create_condition(field.replace("--","").as_str(),">",value);
                expression.conditions.push(condition);
                   match extract_operator(indx, &parts){
                    LogicOp::None=>{
                        break;
                    },
                    d=>expression.operators.push(d)
                }
                 indx=indx+2;
              
                continue;
            }
            }
            },
            s if s.contains("<")=>{
                  let mut pairs=s.split("<");
                if let Some(field)=pairs.next(){
                    if let Some(value)=pairs.next(){
                println!("field is {} and value is {}",field,value);
                let mut condition=create_condition(field.replace("--","").as_str(),"<",value);
                expression.conditions.push(condition);
                   match extract_operator(indx, &parts){
                    LogicOp::None=>{
                        break;
                    },
                    d=>expression.operators.push(d)
                }
                 indx=indx+2;
              
                continue;
            }
            }
                
            },
            s if s.contains("=")=>{
                    let mut pairs=s.split("=");
                if let Some(field)=pairs.next(){
                    if let Some(value)=pairs.next(){
                println!("field is {} and value is {}",field,value);
                let mut condition=create_condition(field.replace("--","").as_str(),"=",value);
                expression.conditions.push(condition);
                   match extract_operator(indx, &parts){
                    LogicOp::None=>{
                        break;
                    },
                    d=>expression.operators.push(d)
                }
                 indx=indx+2;
              
                continue;
            }
            }
                
            },
            _=>{
                panic!("unknow operand:")
            }
            
        }
        
       }else {
            panic!("not handle");
            
       }
   }
   let metadata=reader.metadata();
   let schema=metadata.file_metadata().schema_descr();
    let mut column_names = Vec::new();

   let mut column_indices=HashMap::new();
   for (idx,col) in schema.columns().iter().enumerate(){
    let name=col.path().string();
    column_indices.insert(name.clone(),idx);
     column_names.push(name);
   }

   let mut indexed_conditions=Vec::new();
   for cond in &expression.conditions{
      match  column_indices.get(&cond.column) {
          Some(idx)=>{
            indexed_conditions.push((*idx,cond));
          }
          None=>{
            eprintln!("Column '{}' not found. Available: {:?}", cond.column, column_indices.keys());
          }
      }
   }

   let row_iter = reader.get_row_iter(None).expect("Failed to get row iterator");
   let mut csv_file=File::create(output).unwrap();
    let header = column_names.join(",");
    writeln!(csv_file,"{}",header);
     let mut rows_written = 0;
    
   for row_res in row_iter{
            let row = row_res.expect("Failed to read row");
            let mut results=Vec::new();
            for (idx,cond)in &indexed_conditions{
                  let is_match = evaluate_condition(&row, *idx, cond);
                  results.push(is_match);
            }
              let final_match = combine_results(&results, &expression.operators);
        
        if final_match {
           let mut row_values = Vec::new();
            for name in &column_names {
                let idx = column_indices[name];
                let value = get_value_as_string(&row, idx);
                row_values.push(value);
            }
            writeln!(csv_file, "{}", row_values.join(","));
            rows_written += 1;
        }
   }

}

fn get_value_as_string(row: &Row, col_idx: usize) -> String {
    if let Ok(val) = row.get_string(col_idx) {
        return format!("\"{}\"", val);
    }
    if let Ok(val) = row.get_long(col_idx) {
        return val.to_string();
    }
    if let Ok(val) = row.get_int(col_idx) {
        return val.to_string();
    }
    if let Ok(val) = row.get_bool(col_idx) {
        return val.to_string();
    }
    "".to_string()
}
fn combine_results(results: &[bool], operators: &[LogicOp]) -> bool {
    if results.is_empty() {
        return false;
    }
    
    let mut final_result = results[0];
    for (i, op) in operators.iter().enumerate() {
        let next = results[i + 1];
        final_result = match op {
            LogicOp::And => final_result && next,
            LogicOp::Or => final_result || next,
            _=>continue
        };
    }
    final_result
}



fn evaluate_condition(row: &Row, col_idx: usize, condition: &Condition) -> bool {
    match condition.operator.as_str() {
        ">" => {
            if let Ok(val) = row.get_long(col_idx) {
                let expected: i64 = condition.value.parse().unwrap_or(0);
                val > expected
            } else {
                false
            }
        }
        "<" => {
            if let Ok(val) = row.get_long(col_idx) {
                let expected: i64 = condition.value.parse().unwrap_or(0);
                val < expected
            } else {
                false
            }
        }
        "=" => {
            if let Ok(val) = row.get_string(col_idx) {
                val == condition.value.as_str()
            }
            else if let Ok(val) = row.get_long(col_idx) {
                val.to_string() == condition.value
            }
            else {
                false
            }
        }
        ">=" => {
            if let Ok(val) = row.get_long(col_idx) {
                let expected: i64 = condition.value.parse().unwrap_or(0);
                val >= expected
            } else {
                false
            }
        }
        "<=" => {
            if let Ok(val) = row.get_long(col_idx) {
                let expected: i64 = condition.value.parse().unwrap_or(0);
                val <= expected
            } else {
                false
            }
        }
        _ => false,
    }
}


fn create_condition(column:&str,operator:&str,value:&str)->Condition{
    Condition { column:column.to_string(), operator:operator.to_string(), value:value.to_string() }
}

fn extract_operator( indx:usize,parts:&Vec<&str>)->LogicOp{
    if indx+1<parts.len(){
                   
                    match parts[indx+1].clone(){
                        op if op=="and"=>{
                          return LogicOp::And;
                        },
                        op if op=="or"=>{
                            return  LogicOp::Or;

                        },
                        _=>panic!("operand is not correct")
                        
                    }
                }
               
                println!("not found operand");
        return LogicOp::None ;
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

fn create_csv(file_name:&str,headers:&[String],rows:&[Vec<String>])->Result<()>{
    let mut wtr=Writer::from_path(file_name)?;
    wtr.write_record(headers)?;
    for row in rows{
        wtr.write_record(row)?
    }
    wtr.flush();
    Ok(())
}