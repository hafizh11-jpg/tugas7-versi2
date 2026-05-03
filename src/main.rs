//! ============================================================
//! TUGAS #7: Simulasi Race Condition & Redis Distributed Lock
//! Bahasa   : Rust | GUI: egui | Sinkron: Redis (SET NX PX)
//! ============================================================

use eframe::egui;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio::runtime::Runtime;
use redis::Client;
use std::error::Error;

// ============================================================================
// [STRUKTUR STATE]
// ============================================================================
pub struct AppState {
    pub sisa_tiket: i32,
    pub terjual: i32,
    pub logs: Vec<String>,
    pub sedang_berjalan: bool,
    pub waktu_eksekusi_ms: Option<u64>,
    pub pesan_error: Option<String>,
}

impl Default for AppState {
    fn default() -> Self {
        Self {
            sisa_tiket: 0,
            terjual: 0,
            logs: Vec::new(),
            sedang_berjalan: false,
            waktu_eksekusi_ms: None,
            pesan_error: None,
        }
    }
}

// ============================================================================
// [STRUKTUR APLIKASI EGUI]
// ============================================================================
pub struct MyApp {
    state: Arc<Mutex<AppState>>,
    jumlah_tiket_awal: u32,
    jumlah_request: u32,
    gunakan_redis_lock: bool,
    redis_url: String,
}

impl MyApp {
    pub fn new(_cc: &eframe::CreationContext<'_>) -> Self {
        Self {
            state: Arc::new(Mutex::new(AppState::default())),
            jumlah_tiket_awal: 100,
            jumlah_request: 500,
            gunakan_redis_lock: true,
            redis_url: "redis://127.0.0.1:6379".to_string(),
        }
    }

    fn jalankan_simulasi(&self) {
        let state = Arc::clone(&self.state);
        let tiket_awal = self.jumlah_tiket_awal;
        let req_count = self.jumlah_request;
        let use_lock = self.gunakan_redis_lock;
        let url = self.redis_url.clone();

        {
            let mut s = state.lock().unwrap();
            s.sedang_berjalan = true;
            s.waktu_eksekusi_ms = None;
            s.pesan_error = None;
            s.logs.clear();
            s.sisa_tiket = tiket_awal as i32;
            s.terjual = 0;
        }

        std::thread::spawn(move || {
            let rt = Runtime::new().unwrap();
            rt.block_on(async move {
                if let Err(e) = simulasi_tiket(Arc::clone(&state), use_lock, tiket_awal, req_count, &url).await {
                    let mut s = state.lock().unwrap();
                    s.pesan_error = Some(format!("❌ Gagal terhubung Redis: {}", e));
                    s.sedang_berjalan = false;
                }
            });
        });
    }
}

// ============================================================================
// [IMPLEMENTASI EGUI APP]
// ============================================================================
impl eframe::App for MyApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        egui::CentralPanel::default().show(ctx, |ui| {
            ui.heading("🎟️ Tugas #7: Race Condition vs Redis Distributed Lock");
            ui.separator();

            ui.horizontal(|ui| {
                ui.label("Stok Tiket:");
                ui.add(egui::DragValue::new(&mut self.jumlah_tiket_awal).range(10..=1000));
                ui.label("Request Konkuren:");
                ui.add(egui::DragValue::new(&mut self.jumlah_request).range(100..=2000));
            });

            ui.horizontal(|ui| {
                ui.label("Redis URL:");
                ui.text_edit_singleline(&mut self.redis_url);
                ui.checkbox(&mut self.gunakan_redis_lock, "✅ Aktifkan Redis Mutex (SET NX PX)");
            });

            ui.separator();

            ui.horizontal(|ui| {
                let state = self.state.lock().unwrap();
                let disabled = state.sedang_berjalan;
                if ui.add_enabled(!disabled, egui::Button::new("🚀 Mulai Simulasi")).clicked() {
                    drop(state);
                    self.jalankan_simulasi();
                }
                if ui.button("🔄 Reset Log").clicked() {
                    let mut s = self.state.lock().unwrap();
                    s.logs.clear();
                }
            });

            ui.separator();

            let state = self.state.lock().unwrap();
            ui.horizontal(|ui| {
                ui.label(format!("📦 Sisa Tiket: {}", state.sisa_tiket));
                ui.label(format!("✅ Terjual: {}", state.terjual));
                if let Some(ms) = state.waktu_eksekusi_ms {
                    ui.label(format!("⏱️ Waktu: {} ms", ms));
                }
                if let Some(err) = &state.pesan_error {
                    ui.colored_label(egui::Color32::RED, err);
                }
            });

            ui.separator();
            ui.label("📜 Log Eksekusi:");
            
            egui::ScrollArea::vertical().show(ui, |ui| {
                for log in &state.logs {
                    ui.monospace(log);
                }
            });

            ctx.request_repaint();
        });
    }
}

// ============================================================================
// [LOGIKA SIMULASI UTAMA]
// ============================================================================
async fn simulasi_tiket(
    state: Arc<Mutex<AppState>>,
    gunakan_lock: bool,
    tiket_awal: u32,
    jumlah_request: u32,
    redis_url: &str,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    
    let client = Client::open(redis_url)?;
    let start = Instant::now();
    let mut handles = Vec::new();

    for id in 1..=jumlah_request {
        let state_clone = Arc::clone(&state);
        let client_clone = client.clone();
        let lock_flag = gunakan_lock;
        let user_id = format!("User-{:04}", id);

        // ⚠️ PERBAIKAN: async block sekarang mengembalikan Result
        let handle = tokio::spawn(async move -> Result<(), Box<dyn Error + Send + Sync>> {
            tokio::time::sleep(tokio::time::Duration::from_millis(5)).await;

            if lock_flag {
                // ============================================================
                // 🔒 REDIS DISTRIBUTED LOCK (SET NX PX)
                // ============================================================
                // PERBAIKAN: menggunakan get_multiplexed_tokio_connection()
                let mut conn = client_clone.get_multiplexed_tokio_connection().await?;
                let lock_key = "ticket_mutex_lock";
                let ttl_ms = 3000;

                let mut locked = false;
                while !locked {
                    let res: Option<String> = redis::cmd("SET")
                        .arg(lock_key)
                        .arg(&user_id)
                        .arg("NX")
                        .arg("PX")
                        .arg(ttl_ms)
                        .query_async(&mut conn)
                        .await?;
                    
                    if res == Some("OK".to_string()) {
                        locked = true;
                    } else {
                        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
                    }
                }

                // 🎯 CRITICAL SECTION (DIPROTEKSI)
                let mut s = state_clone.lock().unwrap();
                if s.sisa_tiket > 0 {
                    s.sisa_tiket -= 1;
                    s.terjual += 1;
                    drop(s);
                    
                    let mut s_log = state_clone.lock().unwrap();
                    s_log.logs.push(format!("🔒 [{}] Tiket dibeli. Sisa: {}", user_id, s_log.sisa_tiket).to_string());
                    if s_log.logs.len() > 300 { s_log.logs.remove(0); }
                } else {
                    drop(s);
                    let mut s_log = state_clone.lock().unwrap();
                    s_log.logs.push(format!("❌ [{}] Gagal: Stok habis.", user_id).to_string());
                    if s_log.logs.len() > 300 { s_log.logs.remove(0); }
                }

                // 🔓 RELEASE LOCK
                let _: () = redis::cmd("DEL").arg(lock_key).query_async(&mut conn).await?;

            } else {
                // ⚠️ TANPA LOCK (RACE CONDITION)
                let mut s = state_clone.lock().unwrap();
                if s.sisa_tiket > 0 {
                    let current = s.sisa_tiket;
                    tokio::time::sleep(tokio::time::Duration::from_micros(100)).await;
                    s.sisa_tiket = current - 1;
                    s.terjual += 1;
                    
                    drop(s);
                    let mut s_log = state_clone.lock().unwrap();
                    s_log.logs.push(format!("⚡ [{}] Terjual (No Lock). Sisa: {}", user_id, s_log.sisa_tiket).to_string());
                    if s_log.logs.len() > 300 { s_log.logs.remove(0); }
                } else {
                    drop(s);
                }
            }
            Ok(()) // ⚠️ PERBAIKAN: Return Ok di akhir async block
        });
        handles.push(handle);
    }

    for h in handles {
        let _ = h.await;
    }

    let duration = start.elapsed();
    let mut final_state = state.lock().unwrap();
    final_state.waktu_eksekusi_ms = Some(duration.as_millis() as u64);
    final_state.sedang_berjalan = false;

    if final_state.sisa_tiket < 0 {
        final_state.logs.push(format!("🚨 RACE CONDITION! Sisa: {}", final_state.sisa_tiket).to_string());
    } else if final_state.sisa_tiket == 0 && final_state.terjual == tiket_awal as i32 {
        // ⚠️ PERBAIKAN: .to_string() untuk &str -> String
        final_state.logs.push("✅ KONSISTEN: Semua tiket terjual tepat.".to_string());
    } else {
        final_state.logs.push("⚠️ Hasil tidak sesuai ekspektasi.".to_string());
    }

    Ok(())
}

// ============================================================================
// [ENTRY POINT]
// ============================================================================
fn main() -> eframe::Result {
    let options = eframe::NativeOptions::default();
    // ⚠️ PERBAIKAN: Bungkus dengan Ok(...)
    eframe::run_native(
        "Tugas 7 - Rust & Redis Mutex",
        options,
        Box::new(|cc| Ok(Box::new(MyApp::new(cc)))),
    )
}