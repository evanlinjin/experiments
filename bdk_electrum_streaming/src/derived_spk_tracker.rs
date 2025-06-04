use std::collections::{btree_map, BTreeMap, HashMap};

use electrum_streaming_client::ElectrumScriptHash;
use miniscript::{Descriptor, DescriptorPublicKey};

/// Keeps track of spks.
///
/// This manages subscriptions to spk histories.
/// * When we reconnect with the Electrum server, we wish to resubscribe to all spks.
/// * When we receive history for a spk, we wish to ensure `lookahead` number of spks above are
///   also tracked.
/// * When we receive history for a spk, we wish to include a `last_active_index` update in case
///   `KeychainTxOutIndex` is not up-to-date.
#[derive(Debug, Clone)]
pub struct DerivedSpkTracker<K> {
    lookahead: u32,
    descriptors: BTreeMap<K, Descriptor<DescriptorPublicKey>>,
    derived_spks: BTreeMap<(K, u32), ElectrumScriptHash>,
    derived_spks_rev: HashMap<ElectrumScriptHash, (K, u32)>,
}

impl<K: Ord + Clone> DerivedSpkTracker<K> {
    pub fn new(lookahead: u32) -> Self {
        Self {
            lookahead,
            descriptors: BTreeMap::new(),
            derived_spks: BTreeMap::new(),
            derived_spks_rev: HashMap::new(),
        }
    }

    pub fn all_spk_hashes(&self) -> impl Iterator<Item = ElectrumScriptHash> + '_ {
        self.derived_spks.values().copied()
    }

    pub fn index_of_spk_hash(&self, spk_hash: ElectrumScriptHash) -> Option<(K, u32)> {
        self.derived_spks_rev.get(&spk_hash).cloned()
    }

    fn _add_derived_spk(&mut self, keychain: K, index: u32) -> Option<ElectrumScriptHash> {
        if let btree_map::Entry::Vacant(spk_hash_entry) =
            self.derived_spks.entry((keychain.clone(), index))
        {
            let descriptor = self
                .descriptors
                .get(&keychain)
                .expect("keychain must have associated descriptor");
            let spk = descriptor
                .at_derivation_index(index)
                .expect("descriptor must derive")
                .script_pubkey();
            let script_hash = ElectrumScriptHash::new(&spk);
            spk_hash_entry.insert(script_hash);
            assert!(self
                .derived_spks_rev
                .insert(script_hash, (keychain, index))
                .is_none());
            return Some(script_hash);
        }
        None
    }

    fn _clear_tracked_spks_of_keychain(&mut self, keychain: K) {
        let split = {
            let mut split = self.derived_spks.split_off(&(keychain.clone(), 0));
            let to_add_back = split.split_off(&(keychain, u32::MAX)); // `u32::MAX` is never derived
            self.derived_spks.extend(to_add_back);
            split
        };
        for script_hash in split.into_values() {
            self.derived_spks_rev.remove(&script_hash);
        }
    }

    pub fn insert_descriptor(
        &mut self,
        keychain: K,
        descriptor: Descriptor<DescriptorPublicKey>,
        next_index: u32,
    ) -> Vec<ElectrumScriptHash> {
        if let Some(old_descriptor) = self
            .descriptors
            .insert(keychain.clone(), descriptor.clone())
        {
            if old_descriptor == descriptor {
                return vec![];
            }
            self._clear_tracked_spks_of_keychain(keychain.clone());
        }
        (0_u32..=next_index + self.lookahead + 1)
            .filter_map(|index| self._add_derived_spk(keychain.clone(), index))
            .collect()
    }

    pub fn handle_script_status(
        &mut self,
        script_hash: ElectrumScriptHash,
    ) -> Option<(K, u32, Vec<ElectrumScriptHash>)> {
        let (k, this_index) = self.derived_spks_rev.get(&script_hash).cloned()?;
        let next_index = this_index + 1;

        let mut spk_hashes = Vec::new();
        // We iterate the derivation indices backwards so that we return script hashes that starts
        // with the latest spk, since we want to send request for later spks first.
        for index in (next_index..=next_index + 1 + self.lookahead).rev() {
            match self._add_derived_spk(k.clone(), index) {
                Some(spk_hash) => spk_hashes.push(spk_hash),
                None => break,
            }
        }
        Some((k, this_index, spk_hashes))
    }
}
