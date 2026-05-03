//! ============================================================================
//! TUGAS #7: Simulasi Race Condition & Redis Distributed Lock/Mutex
//! ============================================================================
//!  Problem: Overbooking tiket konser pada High Concurrency
//! 🛠️ Stack: Rust + egui (GUI) + Redis (Distributed Mutex)
//! 📚 Mapping: Setiap bagian kode diberi tag [TAHAP 1] s/d [TAHAP 4] sesuai PDF
//! ============================================================================

use eframe::egui;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio::runtime::Runtime;
use redis::Client;
use std::error::Error;

// ============================================================================
// [STATE APLIKASI]
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
            let rt = match Runtime::new() {
                Ok(r) => r,
                Err(e) => {
                    if let Ok(mut s) = state.lock() {
                        s.pesan_error = Some(format!("❌ Gagal buat runtime: {}", e));
                        s.sedang_berjalan = false;
                    }
                    return;
                }
            };
            rt.block_on(async move {
                if let Err(e) = simulasi_tiket(Arc::clone(&state), use_lock, tiket_awal, req_count, &url).await {
                    if let Ok(mut s) = state.lock() {
                        s.pesan_error = Some(format!("❌ Error simulasi: {}", e));
                        s.sedang_berjalan = false;
                    }
                }
            });
        });
    }
}

// ============================================================================
// [IMPLEMENTASI UI EGUI]
// ============================================================================
impl eframe::App for MyApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        egui::CentralPanel::default().show(ctx, |ui| {
            ui.heading("🎟️ Tugas #7: Race Condition vs Redis Distributed Lock");
            ui.separator();

            ui.horizontal(|ui| {
                ui.label("📦 Stok Tiket Awal:");
                ui.add(egui::DragValue::new(&mut self.jumlah_tiket_awal).range(10..=1000));
                ui.label("🔄 Request Konkuren:");
                ui.add(egui::DragValue::new(&mut self.jumlah_request).range(100..=2000));
            });

            ui.horizontal(|ui| {
                ui.label("🔗 Redis URL:");
                ui.text_edit_singleline(&mut self.redis_url);
                ui.checkbox(&mut self.gunakan_redis_lock, "✅ Aktifkan Redis Mutex (SET NX PX)");
            });

            ui.separator();

            ui.horizontal(|ui| {
                let sedang_berjalan = {
                    let state = self.state.lock().unwrap();
                    state.sedang_berjalan
                }; // ✅ FIX: Hapus has_error yang tidak dipakai
                
                if ui.add_enabled(!sedang_berjalan, egui::Button::new("🚀 Mulai Simulasi")).clicked() {
                    self.jalankan_simulasi();
                }
                
                if ui.button("🔄 Reset Log").clicked() {
                    if let Ok(mut s) = self.state.lock() {
                        s.logs.clear();
                        drop(s);
                    }
                }
            });

            ui.separator();

            let (sisa, terjual, waktu, err) = {
                let state = self.state.lock().unwrap();
                (state.sisa_tiket, state.terjual, state.waktu_eksekusi_ms, state.pesan_error.clone())
            };
            
            ui.horizontal(|ui| {
                ui.label(format!("📦 Sisa Tiket: {}", sisa));
                ui.label(format!("✅ Terjual: {}", terjual));
                if let Some(ms) = waktu {
                    ui.label(format!("⏱️ Waktu: {} ms", ms));
                }
                if let Some(e) = &err {
                    ui.colored_label(egui::Color32::RED, e);
                }
            });

            ui.separator();
            ui.label("📜 Log Eksekusi:");
            
            let logs_snapshot = {
                let state = self.state.lock().unwrap();
                state.logs.clone()
            };
            
            egui::ScrollArea::vertical().show(ui, |ui| {
                for log in &logs_snapshot {
                    ui.monospace(log);
                }
            });

            ctx.request_repaint();
        });
    }
}

// ============================================================================
// [LOGIKA PER USER]
// ============================================================================
async fn proses_pemesanan(
    state: Arc<Mutex<AppState>>,
    client: Client,
    gunakan_lock: bool,
    user_id: String,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    
    tokio::time::sleep(tokio::time::Duration::from_millis(5)).await;

    if gunakan_lock {
        // [TAHAP 2] REDIS DISTRIBUTED LOCK (SET NX PX) - Wait/P Operation
        let mut conn = client.get_multiplexed_tokio_connection().await?;
        let lock_key = "ticket_mutex_lock";
        let ttl_ms = 3000;

        let mut locked = false;
        for _ in 0..50 {
            let res: Option<String> = redis::cmd("SET")
                .arg(lock_key)
                .arg(&user_id)
                .arg("NX")
                .arg("PX")
                .arg(ttl_ms)
                .query_async(&mut conn)
                .await
                .unwrap_or(None);
            
            if res == Some("OK".to_string()) {
                locked = true;
                break;
            } else {
                tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
            }
        }
        if !locked { return Ok(()); }

        // [TAHAP 3] CRITICAL SECTION (DIPROTEKSI LOCK)
        {
            let mut s = state.lock().unwrap();
            if s.sisa_tiket > 0 {
                s.sisa_tiket -= 1;
                s.terjual += 1;
            }
        }

        let log_msg = {
            let s = state.lock().unwrap();
            format!("🔒 [{}] Tiket dibeli. Sisa: {}", user_id, s.sisa_tiket)
        };
        {
            let mut s_log = state.lock().unwrap();
            s_log.logs.push(log_msg);
            if s_log.logs.len() > 300 { s_log.logs.remove(0); }
        }

        // [TAHAP 2] Signal/V Operation
        let _ = redis::cmd("DEL")
            .arg(lock_key)
            .query_async::<_, ()>(&mut conn)
            .await
            .ok();

    } else {
        // [TAHAP 1] SIMULASI RACE CONDITION (TANPA LOCK)
        let nilai_baca = {
            let s = state.lock().unwrap();
            if s.sisa_tiket > 0 { Some(s.sisa_tiket) } else { None }
        };

        if let Some(current) = nilai_baca {
            tokio::time::sleep(tokio::time::Duration::from_micros(100)).await;
            
            {
                let mut s = state.lock().unwrap();
                if s.sisa_tiket > 0 {
                    s.sisa_tiket = current - 1;
                    s.terjual += 1;
                }
            }
            
            let log_msg = format!("⚡ [{}] Terjual (No Lock). Sisa: {}", user_id, {
                let s = state.lock().unwrap(); s.sisa_tiket
            });
            {
                let mut s_log = state.lock().unwrap();
                s_log.logs.push(log_msg);
                if s_log.logs.len() > 300 { s_log.logs.remove(0); }
            }
        }
    }
    Ok(())
}

// ============================================================================
// [ORCHESTRATOR SIMULASI]
// ============================================================================
async fn simulasi_tiket(
    state: Arc<Mutex<AppState>>,
    gunakan_lock: bool,
    tiket_awal: u32,
    jumlah_request: u32,
    redis_url: &str,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    
    let client = Client::open(redis_url)?;
    for attempt in 1..=3 {
        if client.get_multiplexed_tokio_connection().await.is_ok() { break; }
        tokio::time::sleep(tokio::time::Duration::from_millis(100 * attempt as u64)).await;
    }

    let start = Instant::now();
    let mut handles = Vec::new();

    // [TAHAP 1] Spawn 100-1000 task konkuren
    for id in 1..=jumlah_request {
        let st = Arc::clone(&state);
        let cl = client.clone();
        let uid = format!("User-{:04}", id);
        
        let handle = tokio::spawn(async move {
            let _ = proses_pemesanan(st, cl, gunakan_lock, uid).await;
        });
        handles.push(handle);
    }

    for h in handles { let _ = h.await; }

    // [TAHAP 3] Verifikasi Konsistensi Data
    let duration = start.elapsed();
    let mut final_state = state.lock().unwrap();  // ✅ FIX: Ganti 'final' jadi 'final_state'
    final_state.waktu_eksekusi_ms = Some(duration.as_millis() as u64);
    final_state.sedang_berjalan = false;

    if final_state.sisa_tiket < 0 {
        final_state.logs.push("🚨 [TAHAP 1] RACE CONDITION! Sisa negatif (overbooking).".to_string());
    } else if final_state.sisa_tiket == 0 && final_state.terjual == tiket_awal as i32 {
        final_state.logs.push("✅ [TAHAP 3] KONSISTEN: Semua tiket terjual tepat.".to_string());
    } else {
        final_state.logs.push("⚠️ Hasil tidak sesuai ekspektasi.".to_string());
    }

    // [TAHAP 4] Analisis Performa:
    // Tanpa Lock: Cepat (~10-20ms) tapi data korup ❌
    // Dengan Lock: Lebih lambat (~300-600ms) karena overhead network Redis + retry loop, 
    // tapi menjamin integritas data 100% ✅

    Ok(())
}

// ============================================================================
// [ENTRY POINT]
// ============================================================================
fn main() -> eframe::Result {
    let options = eframe::NativeOptions::default();
    std::panic::set_hook(Box::new(|p| eprintln!("🚨 Panic: {}", p)));
    
    eframe::run_native(
        "Tugas 7 - Rust & Redis Mutex",
        options,
        Box::new(|cc| Ok(Box::new(MyApp::new(cc)))),
    )
}