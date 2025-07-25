use std::{collections::HashMap, env};

use anyhow::Result;
use postgres::{
    self,
    types::{ToSql, Type},
    Client, NoTls,
};
use rs_zephyr_common::{DatabaseError, ZephyrVal};
use serde::{Deserialize, Serialize};
use zephyr::{
    db::database::{WhereCond, ZephyrDatabase},
    ZephyrMock, ZephyrStandard,
};

use std::fs;

#[derive(Deserialize, Serialize, Clone)]
pub struct Config {
    pub network: String,
    pub min: Option<u32>,
    pub max: Option<u32>,
    pub frequency: u32,
    pub ws_address: String,
    pub database_conn: String,
}

mod symbol {
    const TAG: u8 = 14;

    #[derive(Debug)]
    pub enum SymbolError {
        InvalidSymbol,
    }

    pub struct Symbol(pub u64);

    impl Symbol {
        pub fn to_string(&self) -> Result<String, SymbolError> {
            let mut body = self.0;

            if (body & (TAG as u64)) != (TAG as u64) {
                return Err(SymbolError::InvalidSymbol);
            }

            body >>= 8; // Remove the tag
            let mut result = String::new();

            while body > 0 {
                let index = (body & 0x3F) as u8;
                body >>= 6;
                let ch = match index {
                    1 => '_',
                    2..=11 => (b'0' + index - 2) as char,
                    12..=37 => (b'A' + index - 12) as char,
                    38..=63 => (b'a' + index - 38) as char,
                    _ => return Err(SymbolError::InvalidSymbol),
                };
                result.push(ch);
            }

            Ok(result.chars().rev().collect())
        }
    }
}

#[derive(Clone)]
pub struct MercuryDatabase {
    pub postgres_arg: String,
}

impl ZephyrMock for MercuryDatabase {
    fn mocked() -> Result<Self> {
        Ok(MercuryDatabase {
            postgres_arg: "".to_string(),
        })
    }
}

pub enum WriteParam {
    Bytes(Vec<u8>),
    Integer(i64),
}

impl WriteParam {
    pub fn as_tosql(&self) -> &(dyn ToSql + Sync) {
        match self {
            WriteParam::Bytes(ref bytes) => bytes as &(dyn ToSql + Sync),
            WriteParam::Integer(ref int) => int as &(dyn ToSql + Sync),
        }
    }
}

impl ZephyrDatabase for MercuryDatabase {
    fn read_raw(
        &self,
        _: i64,
        read_point_hash: [u8; 16],
        read_data: &[i64],
        condition: Option<&[WhereCond]>,
        condition_args: Option<Vec<Vec<u8>>>,
    ) -> Result<Vec<u8>, DatabaseError> {
        let table_name = format!("zephyr_{}", hex::encode(read_point_hash).as_str());

        // auth is actually already performed in that it's the host
        // that hashes the table name for the user.

        let mut columns: Vec<String> = Vec::new();

        for val in read_data {
            if let Ok(res) = symbol::Symbol(*val as u64).to_string() {
                columns.push(res);
            } else {
                return Err(DatabaseError::ZephyrQueryError);
            }
        }

        let mut client = if let Ok(client) = Client::connect(&self.postgres_arg, NoTls) {
            client
        } else {
            return Err(DatabaseError::ZephyrQueryError);
        };

        let types_map = get_table_types(&mut client, &table_name);

        let mut columns_string = String::new();
        for (idx, column) in columns.iter().enumerate() {
            if idx == columns.len() - 1 {
                columns_string.push_str(&format!("{}", column))
            } else {
                columns_string.push_str(&format!("{}, ", column))
            }
        }

        let mut query = format!("SELECT {} FROM {}", columns_string, table_name);
        let mut owned_params: Vec<WriteParam> = Vec::new();

        let mut types = Vec::new();
        if let Some(condition) = condition {
            query.push_str(" WHERE ");

            for idx in 0..condition.len() {
                let colname = {
                    let (operator, column) = match condition[idx] {
                        WhereCond::ColEq(column) => ("=", column),
                        WhereCond::ColGt(column) => (">", column),
                        WhereCond::ColLt(column) => ("<", column),
                    };

                    let colname = symbol::Symbol(column as u64)
                        .to_string()
                        .map_err(|_| DatabaseError::WriteError)?;

                    let condition_str = format!("{} {} ${}", colname, operator, idx + 1);
                    if idx != condition.len() - 1 {
                        query.push_str(&format!("{} AND ", condition_str));
                    } else {
                        query.push_str(&condition_str);
                    }

                    colname
                };

                let col_type = types_map.get(&colname).ok_or(DatabaseError::WriteError)?;
                let param_raw = &condition_args.as_ref().unwrap()[idx];

                // Note: we check the column type rather than just trying a succeful deser
                // from an integer val for backwards compatibility.
                if col_type == "bigint" {
                    let param_deser = bincode::deserialize::<ZephyrVal>(&param_raw);
                    let native = match param_deser {
                        Ok(ZephyrVal::I128(num)) => num as i64,
                        Ok(ZephyrVal::I32(num)) => num as i64,
                        Ok(ZephyrVal::I64(num)) => num as i64,
                        Ok(ZephyrVal::U32(num)) => num as i64,
                        Ok(ZephyrVal::U64(num)) => num as i64,
                        _ => return Err(DatabaseError::WriteError),
                    };

                    owned_params.push(WriteParam::Integer(native));
                    types.push(Type::INT8)
                } else {
                    owned_params.push(WriteParam::Bytes(param_raw.clone()));
                    types.push(Type::BYTEA)
                }
            }
        }

        let stmt = if let Ok(stmt) = client.prepare_typed(&query, &types) {
            stmt
        } else {
            return Err(DatabaseError::ZephyrQueryMalformed);
        };

        let params: Vec<&(dyn ToSql + Sync)> =
            owned_params.iter().map(|param| param.as_tosql()).collect();
        let result = if let Ok(res) = client.query(&stmt, &params) {
            let mut rows = Vec::new();

            for row in res {
                let mut row_wrapped = Vec::new();

                let row_length = row.len();
                for in_row_idx in 0..row_length {
                    let bytes: Vec<u8> = if let Ok(bytes) = row.try_get(in_row_idx) {
                        bytes
                    } else {
                        let integer: i64 = row
                            .try_get(in_row_idx)
                            .map_err(|_| DatabaseError::ZephyrQueryError)?;
                        bincode::serialize(&ZephyrVal::I64(integer)).unwrap()
                    };

                    row_wrapped.push(TypeWrap(bytes))
                }

                rows.push(TableRow { row: row_wrapped })
            }

            TableRows { rows }
        } else {
            return Err(DatabaseError::ZephyrQueryError);
        };

        Ok(bincode::serialize(&result).unwrap())
    }

    fn write_raw(
        &self,
        _: i64,
        written_point_hash: [u8; 16],
        write_data: &[i64],
        written: Vec<Vec<u8>>,
    ) -> Result<(), DatabaseError> {
        let connection = Client::connect(&self.postgres_arg, NoTls);
        let mut client = if let Ok(client) = connection {
            client
        } else {
            println!("{:?}", connection.err().unwrap());
            return Err(DatabaseError::ZephyrQueryError);
        };
        let table_name = format!("zephyr_{}", hex::encode(written_point_hash).as_str());

        let types_map = get_table_types(&mut client, &table_name);
        let mut owned_params: Vec<WriteParam> = Vec::new();
        let mut types = Vec::new();

        let mut query = String::from("INSERT INTO ");
        query.push_str(&format!(
            "zephyr_{}",
            hex::encode(written_point_hash).as_str()
        ));
        query.push_str(" (");

        for idx in 0..write_data.len() {
            let col = if let Ok(string) = symbol::Symbol(write_data[idx] as u64).to_string() {
                string
            } else {
                return Err(DatabaseError::WriteError);
            };
            let bytes = &written[idx];

            query.push_str(&col);

            if types_map.get(&col).ok_or(DatabaseError::ZephyrQueryError)? == "bigint" {
                let param_deser: ZephyrVal =
                    bincode::deserialize(&bytes).map_err(|_| DatabaseError::WriteError)?;
                let param = match param_deser {
                    ZephyrVal::I128(num) => num as i64,
                    ZephyrVal::I32(num) => num as i64,
                    ZephyrVal::I64(num) => num as i64,
                    ZephyrVal::U32(num) => num as i64,
                    ZephyrVal::U64(num) => num as i64,
                    _ => return Err(DatabaseError::WriteError),
                };
                owned_params.push(WriteParam::Integer(param));
                types.push(Type::INT8)
            } else {
                owned_params.push(WriteParam::Bytes(bytes.clone()));
                types.push(Type::BYTEA)
            };

            if idx != write_data.len() - 1 {
                query.push_str(", ");
            }
        }

        query.push(')');

        query.push_str(" VALUES (");
        for n in 1..=owned_params.len() {
            if n == owned_params.len() {
                query.push_str(&format!("${}", n))
            } else {
                query.push_str(&format!("${}, ", n))
            }
        }
        query.push(')');

        /*for _ in 0..params.len() {
            types.push(Type::BYTEA)
        }*/

        let prepared = client.prepare_typed(&query, &types);
        let statement = if let Ok(stmt) = prepared {
            stmt
        } else {
            return Err(DatabaseError::WriteError);
        };

        let params: Vec<&(dyn ToSql + Sync)> =
            owned_params.iter().map(|param| param.as_tosql()).collect();
        let insert = client.execute(&statement, &params);
        if let Ok(_) = insert {
            Ok(())
        } else {
            Err(DatabaseError::WriteError)
        }
    }

    fn update_raw(
        &self,
        _: i64,
        written_point_hash: [u8; 16],
        write_data: &[i64],
        written: Vec<Vec<u8>>,
        condition: &[zephyr::db::database::WhereCond],
        condition_args: Vec<Vec<u8>>,
    ) -> Result<(), DatabaseError> {
        let connection = Client::connect(&self.postgres_arg, NoTls);
        let table_name = format!("zephyr_{}", hex::encode(written_point_hash).as_str());

        let mut client = if let Ok(client) = connection {
            client
        } else {
            println!("{:?}", connection.err().unwrap());
            return Err(DatabaseError::ZephyrQueryError);
        };

        let types_map = get_table_types(&mut client, &table_name);
        let mut owned_params: Vec<WriteParam> = Vec::new();

        let mut params: Vec<&(dyn ToSql + Sync)> = Vec::new();
        let mut types = Vec::new();

        let mut query = String::from("UPDATE ");
        query.push_str(&table_name);
        query.push_str(" SET ");

        for idx in 0..write_data.len() {
            let col = if let Ok(string) = symbol::Symbol(write_data[idx] as u64).to_string() {
                string
            } else {
                return Err(DatabaseError::WriteError);
            };
            let bytes = &written[idx];

            query.push_str(&col);

            if idx != write_data.len() - 1 {
                query.push_str(&format!(" = ${}, ", idx + 1));
            } else {
                query.push_str(&format!(" = ${}", idx + 1));
            }

            let col_type = types_map.get(&col).ok_or(DatabaseError::WriteError)?;

            // Note: we check the column type rather than just trying a succeful deser
            // from an integer val for backwards compatibility.
            if col_type == "bigint" {
                let param_deser = bincode::deserialize::<ZephyrVal>(&bytes);
                let native = match param_deser {
                    Ok(ZephyrVal::I128(num)) => num as i64,
                    Ok(ZephyrVal::I32(num)) => num as i64,
                    Ok(ZephyrVal::I64(num)) => num as i64,
                    Ok(ZephyrVal::U32(num)) => num as i64,
                    Ok(ZephyrVal::U64(num)) => num as i64,
                    _ => return Err(DatabaseError::WriteError),
                };

                owned_params.push(WriteParam::Integer(native));
                types.push(Type::INT8)
            } else {
                owned_params.push(WriteParam::Bytes(bytes.clone()));
                types.push(Type::BYTEA)
            }
        }

        query.push_str(" WHERE ");

        for idx in 0..condition.len() {
            let colname = {
                let (operator, column) = match condition[idx] {
                    WhereCond::ColEq(column) => ("=", column),
                    WhereCond::ColGt(column) => (">", column),
                    WhereCond::ColLt(column) => ("<", column),
                };

                let colname = symbol::Symbol(column as u64)
                    .to_string()
                    .map_err(|_| DatabaseError::WriteError)?;

                let condition_str =
                    format!("{} {} ${}", colname, operator, write_data.len() + idx + 1);
                if idx != condition.len() - 1 {
                    query.push_str(&format!("{} AND ", condition_str));
                } else {
                    query.push_str(&condition_str);
                }

                colname
            };

            let col_type = types_map.get(&colname).ok_or(DatabaseError::WriteError)?;
            let param_raw = &condition_args[idx];

            // Note: we check the column type rather than just trying a succeful deser
            // from an integer val for backwards compatibility.
            if col_type == "bigint" {
                let param_deser = bincode::deserialize::<ZephyrVal>(&param_raw);
                let native = match param_deser {
                    Ok(ZephyrVal::I128(num)) => num as i64,
                    Ok(ZephyrVal::I32(num)) => num as i64,
                    Ok(ZephyrVal::I64(num)) => num as i64,
                    Ok(ZephyrVal::U32(num)) => num as i64,
                    Ok(ZephyrVal::U64(num)) => num as i64,
                    _ => return Err(DatabaseError::WriteError),
                };

                owned_params.push(WriteParam::Integer(native));
                types.push(Type::INT8)
            } else {
                owned_params.push(WriteParam::Bytes(param_raw.clone()));
                types.push(Type::BYTEA)
            }
        }

        //for _ in 0..params.len() {
        //types.push(Type::BYTEA)
        //}

        let statement = if let Ok(stmt) = client.prepare_typed(&query, &types) {
            stmt
        } else {
            return Err(DatabaseError::WriteError);
        };

        let params: Vec<&(dyn ToSql + Sync)> =
            owned_params.iter().map(|param| param.as_tosql()).collect();
        if let Ok(_) = client.execute(&statement, &params) {
            Ok(())
        } else {
            Err(DatabaseError::WriteError)
        }
    }
}

fn get_table_types(client: &mut Client, table_name: &str) -> HashMap<String, String> {
    let mut types_map = HashMap::new();
    let query = format!(
        "
        SELECT
            a.attname as column_name,
            pg_catalog.format_type(a.atttypid, a.atttypmod) as data_type
        FROM
            pg_catalog.pg_attribute a
        JOIN
            pg_catalog.pg_class c ON a.attrelid = c.oid
        JOIN
            pg_catalog.pg_namespace n ON c.relnamespace = n.oid
        WHERE
            c.relname = $1
            AND a.attnum > 0
            AND NOT a.attisdropped;
        "
    );
    let statement = client.prepare(&query);
    let statement = if let Ok(statement) = statement {
        statement
    } else {
        return HashMap::new();
    };

    let rows = client.query(&statement, &[&table_name]);
    let rows = if let Ok(rows) = rows {
        rows
    } else {
        return HashMap::new();
    };

    for row in rows {
        let column_name: &str = row.get("column_name");
        let data_type: &str = row.get("data_type");
        types_map.insert(column_name.to_string(), data_type.to_string());
    }
    types_map
}

impl ZephyrStandard for MercuryDatabase {
    fn zephyr_standard() -> anyhow::Result<Self> {
        let toml_str = fs::read_to_string("././config/mercury.toml")?;
        let cfg: Config = toml::from_str(&toml_str)?;
        Ok(MercuryDatabase {
            postgres_arg: cfg.database_conn,
        })
    }
}

#[derive(Clone, Deserialize, Serialize, Debug)]
pub struct TableRows {
    pub rows: Vec<TableRow>,
}

#[derive(Clone, Deserialize, Serialize, Debug)]
pub struct TableRow {
    pub row: Vec<TypeWrap>,
}

#[derive(Clone, Deserialize, Serialize, Debug)]
pub struct TypeWrap(pub Vec<u8>);
