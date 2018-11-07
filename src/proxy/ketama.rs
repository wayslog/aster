use com::*;
use log::Level;
use md5;
use std::cmp::Ordering;
use std::hash::Hasher;
use std::marker::PhantomData;

const POINTER_PER_SERVER: f64 = 160.0;

#[derive(Eq, Ord, Debug)]
struct NodeHash {
    pub node: String,
    pub hash: u64,
}

impl PartialEq for NodeHash {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.hash == other.hash
    }
}

impl PartialOrd for NodeHash {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.hash.cmp(&other.hash))
    }
}

pub struct HashRing<T: Hasher + Default> {
    nodes: Vec<String>,
    spots: Vec<usize>,
    ticks: Vec<NodeHash>,
    _hash: PhantomData<T>,
}

impl<T: Hasher + Default> HashRing<T> {
    pub fn new(nodes: Vec<String>, spots: Vec<usize>) -> AsResult<Self> {
        if nodes.len() != spots.len() {
            return Err(Error::BadConfig);
        }

        let mut ring = HashRing {
            nodes,
            spots,
            ticks: Vec::new(),
            _hash: PhantomData,
        };
        ring.init();
        Ok(ring)
    }

    fn node_hash(key: &str, align: usize) -> u64 {
        let md5::Digest(bs) = md5::compute(key.as_bytes());
        (u64::from(bs[3 + align * 4]) & 0xFF << 24)
            | ((u64::from(bs[2 + align * 4]) & 0xFF) << 16)
            | ((u64::from(bs[1 + align * 4]) & 0xFF) << 8)
            | (u64::from(bs[align * 4]) & 0xFF)
    }

    fn init(&mut self) {
        self.ticks.clear();

        let ptr_per_hash = 4;
        let servern = self.nodes.len() as f64;

        let totalw = self.spots.iter().sum::<usize>() as f64;
        for (i, node) in self.nodes.iter().enumerate() {
            let percent = (self.spots[i] as f64) / totalw;
            let per_servern =
                ((percent * POINTER_PER_SERVER / 4.0 * servern + 0.000_000_000_1) * 4.0) as u64;
            for pidx in 1..=(per_servern / ptr_per_hash) {
                let host = format!("{}-{}", node, pidx - 1);
                for x in 0..ptr_per_hash {
                    let value = Self::node_hash(&host, x as usize);
                    let n = NodeHash {
                        node: node.clone(),
                        hash: value,
                    };
                    self.ticks.push(n);
                }
            }
        }
        self.ticks.sort();
        debug!("ring init for with ticks {:?}", self.ticks);
    }

    pub fn add_node(&mut self, node: String, spot: usize) {
        let mut tmp_nodes = self.nodes.clone();
        let mut tmp_spots = self.spots.clone();
        if let Some(pos) = tmp_nodes.iter().position(|x| x == &node) {
            tmp_spots[pos] = spot;
        } else {
            tmp_nodes.push(node);
            tmp_spots.push(spot);
        }
        self.nodes = tmp_nodes;
        self.spots = tmp_spots;
        self.init();
    }

    pub fn del_node(&mut self, node: &str) {
        if let Some(pos) = self.nodes.iter().position(|x| x == node) {
            self.nodes.remove(pos);
            self.spots.remove(pos);
            self.init();
        }
    }

    pub fn get_node<K: AsRef<[u8]>>(&self, key: K) -> String {
        let mut hash = T::default();
        hash.write(key.as_ref());
        let value = hash.finish();

        let find = self.ticks.binary_search_by(|x| x.hash.cmp(&value));

        let pos = match find {
            Ok(val) => val,
            Err(val) if self.ticks.len() == val => 0,
            Err(val) => val,
        };
        let node = self.ticks[pos].node.clone();
        if log_enabled!(Level::Trace) {
            trace!("get node of {}", &node);
        }
        node
    }
}

#[cfg(test)]
mod test_ketama {
    use self::super::*;
    use proxy::fnv::Fnv1a64;
    use test::Bencher;

    #[bench]
    fn ketma_dist_get_node(b: &mut Bencher) {
        let ring = HashRing::<Fnv1a64>::new(
            vec![
                "redis-1".to_owned(),
                "redis-2".to_owned(),
                "redis-3".to_owned(),
            ],
            vec![10, 10, 10],
        )
        .expect("create new hash ring success");
        let keys = vec![b"a".to_vec(), b"b".to_vec(), b"val-a".to_vec()];

        b.iter(|| {
            for key in &*keys {
                let _ = ring.get_node(key);
            }
        });
    }

    #[bench]
    fn ketma_dist_add_node(b: &mut Bencher) {
        let mut ring = HashRing::<Fnv1a64>::new(
            vec![
                "redis-1".to_owned(),
                "redis-2".to_owned(),
                "redis-3".to_owned(),
            ],
            vec![10, 10, 10],
        )
        .expect("create new hash ring success");

        b.iter(|| {
            ring.add_node("redis-4".to_owned(), 10);
        });
    }

    #[test]
    fn ketama_dist() {
        let ring = HashRing::<Fnv1a64>::new(
            vec![
                "mc-1".to_owned(),
                "mc-2".to_owned(),
                "mc-3".to_owned(),
                "mc-4".to_owned(),
                "mc-5".to_owned(),
                "mc-6".to_owned(),
                "mc-7".to_owned(),
                "mc-8".to_owned(),
                "mc-9".to_owned(),
                "mc-x".to_owned(),
            ],
            vec![10, 10, 10, 10, 10, 10, 10, 10, 10, 10],
        )
        .expect("create new hash ring success");
        let node = ring.get_node("a");
        assert_eq!(&node, "mc-1");
        assert_eq!(&ring.get_node("memtier-102"), "mc-x")
    }
}
