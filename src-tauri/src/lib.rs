pub mod commands;
pub mod managed_identity;
pub mod managed_process;
pub mod managed_server;
pub mod managed_state;
pub mod managed_windows;
pub mod open_location;
pub mod surge_client;

use commands::ConnectionManager;
use tauri::Manager as _;

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    let app = tauri::Builder::default()
        .manage(ConnectionManager::default())
        .invoke_handler(tauri::generate_handler![
            commands::start_managed_surge,
            commands::connect_surge,
            commands::list_downloads,
            commands::open_download_location,
            commands::get_global_rate_limit,
            commands::set_global_rate_limit,
            commands::disconnect_surge,
            commands::control_download,
            commands::add_download
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
