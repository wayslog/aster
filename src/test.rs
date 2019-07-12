use std::fmt::Debug;

pub trait Payload:
    Clone
    + Send
    + Sync
    + Default
    + Debug
    + PartialEq
    + Eq
    + 'static
{
}

impl<T> Payload for T where
    T: Clone
        + Send
        + Sync
        + Default
        + Debug
        + PartialEq
        + Eq
        + 'static
{
}

pub trait TxnManager: Send + Sync {
    type Payload;

    fn t(&self)->Self::Payload;
}

pub struct MempoolProxy {

}

impl TxnManager for MempoolProxy {
    type Payload = Vec<i32>;

    fn t(&self)->Self::Payload{
        let mut vec = Vec::new();
        vec.push(1);

        vec
    }

}

struct ProposalGenerator<T> {
    txn_manager:Box<dyn TxnManager<Payload = T>>,
}

impl<T: Payload> ProposalGenerator<T> {

    fn t(&self)->T{
        println!("{}",self.txn_manager.t().len());
        return self.txn_manager.t();
    }
}

fn main(){
    let mempool_proxy = MempoolProxy{};
    let proposalGenerator = ProposalGenerator{txn_manager: Box::new(mempool_proxy)};

    proposalGenerator.t();

}