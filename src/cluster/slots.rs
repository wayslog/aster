use crate::com::*;
use crate::redis::cmd::Cmd;
use crate::redis::resp::{Resp, RESP_ARRAY};
use crate::stringview::StringView;

use futures::unsync::mpsc::Sender;

use hashbrown::HashMap;

use std::convert::From;
use std::mem;

pub const SLOTS_COUNT: usize = 16384;

#[derive(Debug)]
pub struct SlotsMap {
    nodes: HashMap<String, Sender<Cmd>>,
    slots: Vec<String>,
}

impl Default for SlotsMap {
    fn default() -> Self {
        SlotsMap {
            nodes: HashMap::new(),
            slots: vec![],
        }
    }
}

impl SlotsMap {
    #[allow(clippy::needless_range_loop)]
    pub fn try_update_all(&mut self, resp: Resp) -> Result<bool, Error> {
        if resp.rtype != RESP_ARRAY {
            warn!(
                "fail to update slots map by `cluster slots` bad resp type {}",
                resp.rtype
            );
            return Err(Error::BadClusterSlotsReply);
        }

        if resp.array.is_none() {
            warn!(
                "fail to update slots map by `cluster slots` bad number {:?}",
                resp.array
            );
            return Err(Error::BadClusterSlotsReply);
        }

        let mut addrs = Vec::with_capacity(SLOTS_COUNT);
        addrs.resize(SLOTS_COUNT, String::new());

        let arr = resp.array.unwrap();
        for r in arr.into_iter() {
            if r.rtype != RESP_ARRAY {
                warn!(
                    "fail to update slots map by `cluster slots` bad resp type {}",
                    r.rtype
                );
                return Err(Error::BadClusterSlotsReply);
            }

            match r.array {
                Some(inner) => {
                    let mut iter = inner.into_iter();
                    let begin = resp_to_usize(iter.next().ok_or(Error::BadClusterSlotsReply)?)?;
                    let end = resp_to_usize(iter.next().ok_or(Error::BadClusterSlotsReply)?)?;

                    let endpoint = iter.next().ok_or(Error::BadClusterSlotsReply)?;
                    let mut epiter = endpoint
                        .array
                        .ok_or(Error::BadClusterSlotsReply)?
                        .into_iter();

                    let addr_resp = epiter.next().ok_or(Error::BadClusterSlotsReply)?;
                    let addr_data = addr_resp.data.ok_or(Error::BadClusterSlotsReply)?;
                    let addr = String::from_utf8_lossy(&addr_data);
                    let port = resp_to_usize(epiter.next().ok_or(Error::BadClusterSlotsReply)?)?;
                    let backend = format!("{}:{}", addr, port);

                    info!("slots begin={} end={} backend={}", begin, end, backend);
                    for i in begin..=end {
                        addrs[i] = backend.clone();
                    }
                }
                None => {
                    warn!("fail to update slots map by `cluster slots` bad inner number : 0");
                    return Err(Error::BadClusterSlotsReply);
                }
            }
        }

        if addrs.iter().any(|x| x == "") {
            warn!("fail to update slots map by `cluster slots` not full cover slots");
            return Err(Error::BadClusterSlotsReply);
        }

        if self.slots.is_empty()
            || self
                .slots
                .iter()
                .zip(addrs.iter())
                .any(|(my, other)| my != other)
        {
            mem::swap(&mut self.slots, &mut addrs);
            return Ok(true);
        }

        Ok(false)
    }

    pub fn add_node(&mut self, node: &str, sender: Sender<Cmd>) {
        self.nodes.insert(node.to_string(), sender);
    }

    pub fn get_sender_by_addr(&mut self, node: &str) -> Option<&mut Sender<Cmd>> {
        self.nodes.get_mut(node)
    }

    pub fn get_addr(&mut self, slot: usize) -> StringView {
        self.slots
            .get(slot)
            .map(|x| From::from(x.as_str()))
            .expect("slot must be full matched")
    }
}

fn resp_to_usize(resp: Resp) -> Result<usize, Error> {
    let data = resp.data.unwrap();
    Ok(btoi::btoi::<usize>(&data)?)
}
