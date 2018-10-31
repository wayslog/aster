use com::*;
use redis::cmd::Cmd;

use futures::unsync::mpsc::Sender;

use hashbrown::{HashMap, HashSet};
use std::mem;

pub const SLOTS_COUNT: usize = 16384;
pub static LF_STR: &'static str = "\n";

pub struct Slots(pub Vec<String>);

impl Slots {
    pub fn parse(data: &[u8]) -> AsResult<Slots> {
        let content = String::from_utf8_lossy(data);
        let mut slots = Vec::with_capacity(SLOTS_COUNT);
        slots.resize(SLOTS_COUNT, "".to_owned());
        let mapper = content.split(LF_STR).filter_map(|line| {
            if line.len() == 0 {
                return None;
            }

            let items: Vec<_> = line.split(" ").collect();
            if !items[2].contains("master") {
                return None;
            }
            let sub_slots: HashSet<_> = items[8..]
                .iter()
                .map(|x| x)
                .map(|item| Self::parse_item(item))
                .flatten()
                .collect();

            let addr = items[1].split("@").next().expect("must contains addr");

            Some((addr.to_owned(), sub_slots))
        });
        let mut count = 0;
        for (addr, ss) in mapper {
            for i in ss.into_iter() {
                slots[i] = addr.clone();
                count += 1;
            }
        }
        if count != SLOTS_COUNT {
            return Err(Error::BadSlotsMap);
        } else {
            Ok(Slots(slots))
        }
    }

    fn parse_item(item: &str) -> Vec<usize> {
        let mut slots = Vec::new();
        if item.len() == 0 {
            return slots;
        }

        if item.contains("->-") {
            debug!("parse cluster nodes result for migrating item={}", item);
            return item
                .split("->-")
                .take(1)
                .map(|x| x.trim_start_matches('['))
                .map(|num_str| num_str.parse::<usize>().expect("must parse integer done"))
                .collect();
        }

        let mut iter = item.split("-");
        let begin_str = iter.next().expect("must have integer");
        let begin = begin_str.parse::<usize>().expect("must parse integer done");
        if let Some(end_str) = iter.next() {
            let end = end_str
                .parse::<usize>()
                .expect("must parse end integer done");
            for i in begin..=end {
                slots.push(i);
            }
        } else {
            slots.push(begin);
        }
        slots
    }
}

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
    pub fn try_update_all(&mut self, data: &[u8]) -> bool {
        match Slots::parse(data) {
            Ok(slots) => {
                let mut slots = slots;
                if self.slots.is_empty() || self
                    .slots
                    .iter()
                    .zip(slots.0.iter())
                    .any(|(my, other)| my != other)
                {
                    mem::swap(&mut self.slots, &mut slots.0);
                    true
                } else {
                    false
                }
            }
            Err(err) => {
                warn!("fail to update slots map by given data due {:?}", err);
                false
            }
        }
    }

    pub fn add_node(&mut self, node: String, sender: Sender<Cmd>) {
        self.nodes.insert(node, sender);
    }

    pub fn get_sender_by_addr(&mut self, node: &String) -> Option<&mut Sender<Cmd>> {
        self.nodes.get_mut(node)
    }

    pub fn get_addr(&mut self, slot: usize) -> String {
        self.slots
            .get(slot)
            .cloned()
            .expect("slot must be full matched")
    }
}
