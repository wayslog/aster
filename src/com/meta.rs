use get_if_addrs::get_if_addrs;

use crate::com::ClusterConfig;
use std::cell::RefCell;
use std::env;
use std::net::IpAddr;

thread_local!(static TLS_META: RefCell<Option<Meta>> = RefCell::new(None));

#[derive(Debug, Clone)]
pub struct Meta {
    cluster: String,
    port: String,
    ip: String,
}

pub fn get_if_addr() -> String {
    if let Ok(if_adrs) = get_if_addrs() {
        for iface in if_adrs {
            if iface.is_loopback() {
                continue;
            }
            let addr = iface.ip();
            if !addr.is_unspecified() && addr.is_ipv4() && !addr.is_loopback() {
                return addr.to_string();
            }
        }
    }
    // get from env
    if let Ok(host_ip) = env::var("HOST") {
        if let Ok(addr) = host_ip.parse::<IpAddr>() {
            if !addr.is_unspecified() && addr.is_ipv4() && !addr.is_loopback() {
                return addr.to_string();
            }
        }
    }
    "127.0.0.1".to_string()
}

pub fn load_meta(cc: ClusterConfig, ip: Option<String>) -> Meta {
    let port = cc
        .listen_addr
        .split(':')
        .nth(1)
        .expect("listen_addr must contains port")
        .to_string();

    let ip = ip.unwrap_or_else(get_if_addr);

    Meta {
        cluster: cc.name,
        port,
        ip,
    }
}

pub fn meta_init(meta: Meta) {
    TLS_META.with(|gkd| {
        let mut handler = gkd.borrow_mut();
        handler.replace(meta);
    });
}

pub fn get_ip() -> String {
    TLS_META.with(|gkd| {
        gkd.borrow()
            .as_ref()
            .map(|x| x.ip.clone())
            .expect("get_ip must be called after init")
    })
}

pub fn get_port() -> String {
    TLS_META.with(|gkd| {
        gkd.borrow()
            .as_ref()
            .map(|x| x.port.clone())
            .expect("get_ip must be called after init")
    })
}

pub fn get_cluster() -> String {
    TLS_META.with(|gkd| {
        gkd.borrow()
            .as_ref()
            .map(|x| x.cluster.clone())
            .expect("get_ip must be called after init")
    })
}
