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
// [FUNGSI PROSES PEMESANAN PER USER] - Async dengan Return Result
// ============================================================================
/// Fungsi ini menangani logika pemesanan untuk SATU user.
/// Return type: Result<(), Box<dyn Error + Send + Sync>>
/// - Ok(())  = proses selesai tanpa error
/// - Err(e)  = terjadi error (misal: Redis down)
async fn proses_pemesanan(
    state: Arc<Mutex<AppState>>,
    client: Client,
    gunakan_lock: bool,
    user_id: String,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    
    tokio::time::sleep(tokio::time::Duration::from_millis(5)).await;

    if gunakan_lock {
        // ============================================================
        // 🔒 REDIS DISTRIBUTED LOCK (SET NX PX) - Tahap 2
        // ============================================================
        // Wait/P Operation: Coba ambil kunci dengan SET NX PX
        let mut conn = client.get_multiplexed_tokio_connection().await?;
        let lock_key = "ticket_mutex_lock";
        let ttl_ms = 3000; // Timeout 3 detik untuk mencegah deadlock

        // Retry loop: terus coba sampai lock berhasil diambil
        let mut locked = false;
        while !locked {
            let res: Option<String> = redis::cmd("SET")
                .arg(lock_key)
                .arg(&user_id)
                .arg("NX")  // Only set if Not eXists
                .arg("PX")  // Set expiry in milliseconds
                .arg(ttl_ms)
                .query_async(&mut conn)
                .await?;
            
            if res == Some("OK".to_string()) {
                locked = true; // ✅ Lock berhasil diambil
            } else {
                // 🔁 Lock sedang dipegang proses lain, tunggu sebentar lalu retry
                tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
            }
        }

        // ============================================================
        // 🎯 CRITICAL SECTION (DIPROTEKSI REDIS LOCK) - Tahap 3
        // ============================================================
        // Hanya 1 proses yang boleh masuk ke blok ini pada satu waktu
        {
            let mut s = state.lock().unwrap();
            if s.sisa_tiket > 0 {
                // ✅ Stok masih ada, kurangi 1
                s.sisa_tiket -= 1;
                s.terjual += 1;
                // Mutex otomatis dilepas saat scope berakhir (RAII)
            }
        } // 🔓 std::sync::Mutex dilepas di sini

        // Catat log (di luar critical section agar tidak memperlambat)
        {
            let mut s_log = state.lock().unwrap();
            if s_log.sisa_tiket >= 0 {
                s_log.logs.push(format!("🔒 [{}] Tiket dibeli. Sisa: {}", user_id, s_log.sisa_tiket));
            } else {
                s_log.logs.push(format!("❌ [{}] Gagal: Stok habis.", user_id));
            }
            // Batasi ukuran log agar tidak makan memori
            if s_log.logs.len() > 300 { 
                s_log.logs.remove(0); 
            }
        }

        // ============================================================
        // 🔓 Signal/V Operation: LEPAS KUNCI
        // ============================================================
        // Hapus key lock agar proses lain bisa masuk critical section
        let _: () = redis::cmd("DEL").arg(lock_key).query_async(&mut conn).await?;

    } else {
        // ============================================================
        // ⚠️ TANPA LOCK - Simulasi Race Condition (Tahap 1)
        // ============================================================
        // Banyak thread membaca `sisa_tiket` bersamaan → stale data
        // Lalu sama-sama menulis nilai usang → overbooking / sisa negatif
        {
            let mut s = state.lock().unwrap();
            if s.sisa_tiket > 0 {
                let current = s.sisa_tiket;  // 📖 READ (bisa dibaca thread lain juga)
                
                // 🕐 Simulasi delay / context switch (memperjelas race condition)
                tokio::time::sleep(tokio::time::Duration::from_micros(100)).await;
                
                s.sisa_tiket = current - 1;  // ✍️ WRITE (menimpa dengan nilai usang!)
                s.terjual += 1;
                // ⚠️ Di sinilah Race Condition terjadi: 2 thread baca nilai sama,
                // lalu sama-sama menulis current-1 → stok berkurang hanya 1 padahal 2 tiket terjual
            }
        }

        // Catat log
        {
            let mut s_log = state.lock().unwrap();
            s_log.logs.push(format!("⚡ [{}] Terjual (No Lock). Sisa: {}", user_id, s_log.sisa_tiket));
            if s_log.logs.len() > 300 { 
                s_log.logs.remove(0); 
            }
        }
    }

    Ok(()) // ✅ Fungsi selesai sukses
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

    // [Tahap 1] Spawn 100-1000 request konkuren
    for id in 1..=jumlah_request {
        let state_clone = Arc::clone(&state);
        let client_clone = client.clone();
        let user_id = format!("User-{:04}", id);

        // ✅ PERBAIKAN: Panggil fungsi terpisah yang return Result
        let handle = tokio::spawn(async move {
            // Ignor error per-user agar satu user gagal tidak menghentikan semua
            let _ = proses_pemesanan(state_clone, client_clone, gunakan_lock, user_id).await;
        });
        handles.push(handle);
    }

    // Tunggu semua task selesai
    for h in handles {
        let _ = h.await;
    }

    // [Tahap 3] Verifikasi Konsistensi Data
    let duration = start.elapsed();
    let mut final_state = state.lock().unwrap();
    final_state.waktu_eksekusi_ms = Some(duration.as_millis() as u64);
    final_state.sedang_berjalan = false;

    if final_state.sisa_tiket < 0 {
        final_state.logs.push("🚨 RACE CONDITION TERDETEKSI! Sisa tiket negatif.".to_string());
    } else if final_state.sisa_tiket == 0 && final_state.terjual == tiket_awal as i32 {
        final_state.logs.push("✅ KONSISTEN: Semua tiket terjual tepat. Tidak ada overbooking.".to_string());
    } else {
        final_state.logs.push("⚠️ Hasil tidak sesuai ekspektasi.".to_string());
    }

    // [Tahap 4] Analisis Performa:
    // - Tanpa Lock: Lebih cepat (tanpa overhead network Redis), tapi data korup ❌
    // - Dengan Lock: Sedikit lebih lambat (overhead SET NX + network RTT), tapi data 100% aman ✅
    // Trade-off: Integritas data > kecepatan murni untuk sistem finansial/tiket

    Ok(())
}

// ============================================================================
// [ENTRY POINT]
// ============================================================================
fn main() -> eframe::Result {
    let options = eframe::NativeOptions::default();
    eframe::run_native(
        "Tugas 7 - Rust & Redis Mutex",
        options,
        Box::new(|cc| Ok(Box::new(MyApp::new(cc)))),
    )
}