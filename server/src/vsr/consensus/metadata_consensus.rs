use std::collections::HashMap;
use crate::vsr::{OpNumber, ReplicaCount};


pub struct MetadataConsensus<S> {
    //TODO: Somehow tame this map, so it doesn't grow into infinity.
    prepare_ok_counter: HashMap<OpNumber, ReplicaCount>,
    state_machine: S,
}