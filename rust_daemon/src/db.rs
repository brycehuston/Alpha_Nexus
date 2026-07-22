use rusqlite::{params, Connection};
use std::sync::Mutex;
use lazy_static::lazy_static;

lazy_static! {
    // We use a global mutex for simplicity since DB writes are low-frequency compared to the main loop.
    static ref DB_CONN: Mutex<Option<Connection>> = Mutex::new(None);
}

pub fn init_db() {
    let conn = match Connection::open("trade_telemetry.db") {
        Ok(c) => c,
        Err(e) => {
            eprintln!("⚠️  Failed to open trade_telemetry.db: {}", e);
            return;
        }
    };

    let create_table_sql = "
        CREATE TABLE IF NOT EXISTS trade_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            wallet_address TEXT,
            token_mint TEXT,
            trade_direction TEXT,
            trade_size_sol REAL,
            market_cap_usd REAL,
            execution_status TEXT
        )
    ";

    let create_open_positions_sql = "
        CREATE TABLE IF NOT EXISTS open_positions (
            token_mint TEXT PRIMARY KEY,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    ";

    if let Err(e) = conn.execute(create_open_positions_sql, []) {
        eprintln!("⚠️  Failed to create open_positions table: {}", e);
    }

    if let Err(e) = conn.execute(create_table_sql, []) {
        eprintln!("⚠️  Failed to create trade_logs table: {}", e);
    } else {
        // Index on (wallet_address, token_mint): get_whale_history() filters
        // on both columns. Without this index the query is a full table scan,
        // which becomes expensive after days of active trading.
        let index_sql = "CREATE INDEX IF NOT EXISTS idx_wallet_mint \
                         ON trade_logs(wallet_address, token_mint)";
        if let Err(e) = conn.execute(index_sql, []) {
            eprintln!("⚠️  Failed to create wallet_mint index: {}", e);
        }
        *DB_CONN.lock().unwrap_or_else(|e| e.into_inner()) = Some(conn);
    }
}

pub fn log_trade_telemetry(
    wallet_address: &str,
    token_mint: &str,
    trade_direction: &str,
    trade_size_sol: f64,
    market_cap_usd: f64,
    execution_status: &str,
) {
    let lock = DB_CONN.lock().unwrap_or_else(|e| e.into_inner());
    if let Some(conn) = lock.as_ref() {
        let sql = "
            INSERT INTO trade_logs (
                wallet_address, token_mint, trade_direction, 
                trade_size_sol, market_cap_usd, execution_status
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)
        ";
        if let Err(e) = conn.execute(
            sql,
            params![
                wallet_address,
                token_mint,
                trade_direction,
                trade_size_sol,
                market_cap_usd,
                execution_status
            ],
        ) {
            eprintln!("⚠️  Failed to insert trade log: {}", e);
        }
    }
}

pub struct WhaleHistory {
    pub buys: i32,
    pub sells: i32,
    pub net_sol: f64,
    pub status: String,
}

pub fn get_whale_history(wallet: &str, mint: &str) -> WhaleHistory {
    let mut history = WhaleHistory {
        buys: 0,
        sells: 0,
        net_sol: 0.0,
        status: "Unknown".to_string(),
    };

    let lock = DB_CONN.lock().unwrap_or_else(|e| e.into_inner());
    if let Some(conn) = lock.as_ref() {
        let sql = "
            SELECT trade_direction, trade_size_sol
            FROM trade_logs
            WHERE wallet_address = ?1 AND token_mint = ?2
        ";

        let mut stmt = match conn.prepare(sql) {
            Ok(s) => s,
            Err(_) => return history,
        };

        let rows = stmt.query_map(params![wallet, mint], |row| {
            let direction: String = row.get(0)?;
            let size_sol: f64 = row.get(1).unwrap_or(0.0);
            Ok((direction, size_sol))
        });

        if let Ok(iter) = rows {
            let mut buy_sol = 0.0;
            let mut sell_sol = 0.0;

            for row in iter.flatten() {
                let (direction, size) = row;
                if direction == "BUY" {
                    history.buys += 1;
                    buy_sol += size;
                } else if direction == "SELL" {
                    history.sells += 1;
                    sell_sol += size;
                }
            }

            history.net_sol = buy_sol - sell_sol;
            
            // Format to 4 decimal places
            history.net_sol = (history.net_sol * 10000.0).round() / 10000.0;

            if history.net_sol > 0.0 {
                history.status = "Holding/Accumulating".to_string();
            } else {
                history.status = "Exited/Sold All".to_string();
            }
        }
    }

    history
}

pub fn insert_open_position(mint: &str) {
    let lock = DB_CONN.lock().unwrap_or_else(|e| e.into_inner());
    if let Some(conn) = lock.as_ref() {
        let sql = "INSERT OR REPLACE INTO open_positions (token_mint) VALUES (?1)";
        if let Err(e) = conn.execute(sql, params![mint]) {
            eprintln!("⚠️  Failed to insert open position {}: {}", mint, e);
        }
    }
}

pub fn remove_open_position(mint: &str) {
    let lock = DB_CONN.lock().unwrap_or_else(|e| e.into_inner());
    if let Some(conn) = lock.as_ref() {
        let sql = "DELETE FROM open_positions WHERE token_mint = ?1";
        if let Err(e) = conn.execute(sql, params![mint]) {
            eprintln!("⚠️  Failed to remove open position {}: {}", mint, e);
        }
    }
}

pub fn get_all_open_positions() -> Vec<String> {
    let mut positions = Vec::new();
    let lock = DB_CONN.lock().unwrap_or_else(|e| e.into_inner());
    if let Some(conn) = lock.as_ref() {
        let sql = "SELECT token_mint FROM open_positions ORDER BY timestamp ASC";
        if let Ok(mut stmt) = conn.prepare(sql) {
            if let Ok(rows) = stmt.query_map([], |row| row.get(0)) {
                for row in rows.flatten() {
                    positions.push(row);
                }
            }
        }
    }
    positions
}
