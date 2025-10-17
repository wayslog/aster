use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use once_cell::sync::Lazy;

use crate::metrics::{self, FrontCommandStats};

static START_TIME: Lazy<SystemTime> = Lazy::new(SystemTime::now);

#[derive(Debug, Clone, Copy)]
pub enum ProxyMode {
    Standalone,
    Cluster,
}

impl ProxyMode {
    pub fn as_str(self) -> &'static str {
        match self {
            ProxyMode::Standalone => "standalone",
            ProxyMode::Cluster => "cluster",
        }
    }
}

pub struct InfoContext<'a> {
    pub cluster: &'a str,
    pub mode: ProxyMode,
    pub listen_port: u16,
    pub backend_nodes: usize,
}

pub fn render_info(context: InfoContext<'_>, section: Option<&str>) -> Bytes {
    let uptime = SystemTime::now()
        .duration_since(*START_TIME)
        .unwrap_or_default();
    let uptime_seconds = uptime.as_secs();
    let uptime_days = uptime_seconds / 86_400;
    let startup_time_unix = START_TIME
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    let process_id = std::process::id();
    let version = env!("CARGO_PKG_VERSION");
    let arch_bits = (std::mem::size_of::<usize>() * 8) as u64;
    let build_target = format!("{}-{}", std::env::consts::OS, std::env::consts::ARCH);

    let connected_clients = metrics::front_connections_current(context.cluster);
    let total_connections = metrics::front_connections_total(context.cluster);
    let command_stats = metrics::front_command_stats(context.cluster);
    let memory_bytes = metrics::memory_usage_bytes();
    let cpu_percent = metrics::cpu_usage_percent();
    let global_errors = metrics::global_error_count();

    let sections = collect_sections(
        &context,
        uptime_seconds,
        uptime_days,
        startup_time_unix,
        process_id,
        version,
        arch_bits,
        &build_target,
        connected_clients,
        total_connections,
        command_stats,
        memory_bytes,
        cpu_percent,
        global_errors,
    );

    let filter = section
        .map(|s| s.trim().to_ascii_lowercase())
        .filter(|s| !s.is_empty());

    let mut output = String::new();
    for (name, entries) in sections {
        if !should_include(&filter, name) {
            continue;
        }
        if !output.is_empty() {
            output.push_str("\r\n");
        }
        output.push_str("# ");
        output.push_str(name);
        output.push_str("\r\n");
        for (key, value) in entries {
            output.push_str(&key);
            output.push(':');
            output.push_str(&value);
            output.push_str("\r\n");
        }
    }

    if output.is_empty() {
        output.push_str("\r\n");
    }

    Bytes::from(output)
}

fn collect_sections(
    context: &InfoContext<'_>,
    uptime_seconds: u64,
    uptime_days: u64,
    startup_time_unix: u64,
    process_id: u32,
    version: &str,
    arch_bits: u64,
    build_target: &str,
    connected_clients: u64,
    total_connections: u64,
    command_stats: FrontCommandStats,
    memory_bytes: u64,
    cpu_percent: f64,
    global_errors: u64,
) -> Vec<(&'static str, Vec<(String, String)>)> {
    let mut sections = Vec::new();

    sections.push((
        "Server",
        vec![
            ("aster_version".to_string(), version.to_string()),
            ("aster_mode".to_string(), context.mode.as_str().to_string()),
            ("cluster_name".to_string(), context.cluster.to_string()),
            ("process_id".to_string(), process_id.to_string()),
            ("tcp_port".to_string(), context.listen_port.to_string()),
            ("arch_bits".to_string(), arch_bits.to_string()),
            ("os".to_string(), std::env::consts::OS.to_string()),
            ("build_target".to_string(), build_target.to_string()),
            (
                "startup_time_unix".to_string(),
                startup_time_unix.to_string(),
            ),
            ("uptime_in_seconds".to_string(), uptime_seconds.to_string()),
            ("uptime_in_days".to_string(), uptime_days.to_string()),
        ],
    ));

    sections.push((
        "Clients",
        vec![
            (
                "connected_clients".to_string(),
                connected_clients.to_string(),
            ),
            (
                "total_connections_received".to_string(),
                total_connections.to_string(),
            ),
        ],
    ));

    sections.push((
        "Stats",
        vec![
            (
                "total_commands_processed".to_string(),
                command_stats.total().to_string(),
            ),
            (
                "total_commands_succeeded".to_string(),
                command_stats.total_ok().to_string(),
            ),
            (
                "total_commands_failed".to_string(),
                command_stats.total_fail().to_string(),
            ),
            ("global_error_count".to_string(), global_errors.to_string()),
        ],
    ));

    sections.push((
        "Memory",
        vec![
            ("used_memory".to_string(), memory_bytes.to_string()),
            ("used_memory_human".to_string(), format_bytes(memory_bytes)),
        ],
    ));

    sections.push((
        "CPU",
        vec![(
            "used_cpu_percent".to_string(),
            format!("{:.2}", cpu_percent),
        )],
    ));

    sections.push((
        "Proxy",
        vec![
            ("proxy_mode".to_string(), context.mode.as_str().to_string()),
            (
                "backend_nodes".to_string(),
                context.backend_nodes.to_string(),
            ),
            ("cluster".to_string(), context.cluster.to_string()),
        ],
    ));

    sections
}

fn should_include(filter: &Option<String>, section: &str) -> bool {
    match filter.as_deref() {
        None => true,
        Some("all") | Some("default") | Some("everything") => true,
        Some(candidate) => candidate == section.to_ascii_lowercase(),
    }
}

fn format_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KB", "MB", "GB", "TB"];
    if bytes == 0 {
        return "0B".to_string();
    }

    let mut value = bytes as f64;
    let mut unit = "B";
    for candidate in UNITS.iter() {
        unit = candidate;
        if value < 1024.0 {
            break;
        }
        if *candidate != "TB" {
            value /= 1024.0;
        }
    }
    if unit == "B" {
        format!("{bytes}{unit}")
    } else {
        format!("{value:.2}{unit}")
    }
}
