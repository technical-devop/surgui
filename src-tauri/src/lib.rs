pub mod automatic_hash;
pub mod commands;
pub mod file_hash;
pub mod hash_persistence;
pub mod hash_state;
pub mod managed_identity;
pub mod managed_process;
pub mod managed_server;
pub mod managed_state;
pub mod managed_windows;
pub mod open_location;
pub mod progress_stream;
pub mod surge_client;

use commands::ConnectionManager;
use tauri::Manager as _;

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    if let Some(code) = hash_persistence::run_helper_from_env() {
        std::process::exit(code);
    }
    let app = tauri::Builder::default()
        .manage(ConnectionManager::default())
        .invoke_handler(tauri::generate_handler![
            commands::start_managed_surge,
            commands::connect_surge,
            commands::list_downloads,
            commands::open_download_location,
            commands::get_global_rate_limit,
            commands::set_global_rate_limit,
            commands::get_max_concurrent_downloads,
            commands::set_max_concurrent_downloads,
            commands::disconnect_surge,
            commands::control_download,
            commands::bulk_control,
            commands::add_download,
            commands::add_download_with_options,
            commands::get_automatic_hash_settings,
            commands::set_automatic_hash_default,
            commands::set_download_hash_intents,
            commands::get_download_hashes,
            commands::refresh_download_hashes,
            commands::list_downloads_snapshot,
            commands::start_progress_stream
        ])
        .build(tauri::generate_context!())
        .expect("error while building tauri application");

    app.run(|app_handle, event| {
        if let tauri::RunEvent::Exit = event {
            if let Some(state) = app_handle.try_state::<ConnectionManager>() {
                // The exit hook has no reliable user-visible surface on
                // Windows, so it makes no success/failure claim. The bounded
                // managed shutdown still runs and keeps its own fences.
                let _ = state.shutdown_managed();
            }
        }
    });
}
