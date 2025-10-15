use anyhow::{anyhow, bail, Result};

use super::{RespValue, SLOT_COUNT};

#[derive(Debug, Clone)]
pub struct SlotMap {
    masters: Vec<String>,
    replicas: Vec<Vec<String>>,
}

impl SlotMap {
    pub fn new() -> Self {
        Self {
            masters: vec![String::new(); SLOT_COUNT as usize],
            replicas: vec![Vec::new(); SLOT_COUNT as usize],
        }
    }

    pub fn from_slots_response(resp: RespValue) -> Result<Self> {
        let mut layout = Self::new();
        let array = match resp {
            RespValue::Array(values) => values,
            _ => bail!("CLUSTER SLOTS must return an array"),
        };

        for item in array {
            layout.apply_slot_entry(item)?;
        }

        Ok(layout)
    }

    fn apply_slot_entry(&mut self, entry: RespValue) -> Result<()> {
        let fields = match entry {
            RespValue::Array(fields) => fields,
            _ => bail!("slot entry must be an array"),
        };
        if fields.len() < 3 {
            bail!("slot entry must contain start, end, and master");
        }

        let start = extract_integer(&fields[0])? as usize;
        let end = extract_integer(&fields[1])? as usize;
        let master = extract_endpoint(&fields[2])?;

        if start > end || end >= SLOT_COUNT as usize {
            bail!("slot range {}-{} out of bounds", start, end);
        }

        let replicas = fields
            .iter()
            .skip(3)
            .map(|field| extract_endpoint(field))
            .collect::<Result<Vec<_>>>()?;

        for slot in start..=end {
            self.masters[slot] = master.clone();
            self.replicas[slot] = replicas.clone();
        }

        Ok(())
    }

    pub fn master_for_slot(&self, slot: u16) -> Option<&str> {
        self.masters
            .get(slot as usize)
            .and_then(|s| if s.is_empty() { None } else { Some(s.as_str()) })
    }

    pub fn replica_for_slot(&self, slot: u16) -> Option<&str> {
        self.replicas
            .get(slot as usize)
            .and_then(|list| {
                list.get(0)
                    .and_then(|s| if s.is_empty() { None } else { Some(s.as_str()) })
            })
    }

    pub fn all_nodes(&self) -> Vec<String> {
        let mut set = std::collections::BTreeSet::new();
        for master in &self.masters {
            if !master.is_empty() {
                set.insert(master.clone());
            }
        }
        for entry in &self.replicas {
            for replica in entry {
                set.insert(replica.clone());
            }
        }
        set.into_iter().collect()
    }
}

fn extract_integer(value: &RespValue) -> Result<i64> {
    match value {
        RespValue::Integer(v) => Ok(*v),
        RespValue::BulkString(bs) | RespValue::SimpleString(bs) => {
            let text = std::str::from_utf8(bs)?;
            text.parse::<i64>()
                .map_err(|err| anyhow!("invalid integer '{}' - {err}", text))
        }
        _ => bail!("expected integer slot field"),
    }
}

fn extract_endpoint(value: &RespValue) -> Result<String> {
    let fields = match value {
        RespValue::Array(fields) => fields,
        _ => bail!("endpoint must be array"),
    };
    if fields.len() < 2 {
        bail!("endpoint must contain host and port");
    }
    let host = match &fields[0] {
        RespValue::BulkString(bs) | RespValue::SimpleString(bs) => {
            std::str::from_utf8(bs)?.to_string()
        }
        _ => bail!("endpoint host must be string"),
    };
    let port = extract_integer(&fields[1])?;
    Ok(format!("{}:{}", host, port))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn parse_basic_slot_layout() {
        let resp = RespValue::Array(vec![RespValue::Array(vec![
            RespValue::Integer(0),
            RespValue::Integer(2),
            RespValue::Array(vec![
                RespValue::BulkString(Bytes::from_static(b"127.0.0.1")),
                RespValue::Integer(7000),
            ]),
            RespValue::Array(vec![
                RespValue::BulkString(Bytes::from_static(b"127.0.0.1")),
                RespValue::Integer(7001),
            ]),
        ])]);

        let map = SlotMap::from_slots_response(resp).expect("slot map");
        assert_eq!(map.master_for_slot(1), Some("127.0.0.1:7000"));
        assert_eq!(map.replica_for_slot(1), Some("127.0.0.1:7001"));
        assert!(map.all_nodes().contains(&"127.0.0.1:7000".to_string()));
    }

    #[test]
    fn empty_slot_returns_none() {
        let map = SlotMap::new();
        assert_eq!(map.master_for_slot(0), None);
        assert_eq!(map.replica_for_slot(0), None);
    }
}
