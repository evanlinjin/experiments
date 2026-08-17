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

    /// Track `descriptor` under `keychain` and return the newly derived script hashes.
    ///
    /// The derivation window is a per-keychain high-water mark: re-inserting the same descriptor
    /// with a larger `next_index` widens the window and returns only the script hashes derived by
    /// the widening, while an equal or smaller `next_index` is a no-op. Inserting a different
    /// descriptor discards the keychain's tracked spks and rebuilds the window from scratch.
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
            if old_descriptor != descriptor {
                self._clear_tracked_spks_of_keychain(keychain.clone());
            }
        }
        (0_u32..=next_index + self.lookahead + 1)
            .filter_map(|index| self._add_derived_spk(keychain.clone(), index))
            .collect()
    }

    pub fn mark_script_hash_used(&mut self, keychain: &K, index: u32) -> Vec<ElectrumScriptHash> {
        let next_index = index + 1;

        let mut spk_hashes = Vec::new();
        // We iterate the derivation indices backwards so that we return script hashes that starts
        // with the latest spk, since we want to send request for later spks first.
        for index in (next_index..=next_index + 1 + self.lookahead).rev() {
            match self._add_derived_spk(keychain.clone(), index) {
                Some(spk_hash) => spk_hashes.push(spk_hash),
                None => break,
            }
        }
        spk_hashes
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::str::FromStr;

    const LOOKAHEAD: u32 = 5;
    const XPUB: &str = "xpub661MyMwAqRbcFtXgS5sYJABqqG9YLmC4Q1Rdap9gSE8NqtwybGhePY2gZ29ESFjqJoCu1Rupje8YtGqsefD265TMg7usUDFdp6W1EGMcet8";

    fn descriptor(derivation_path: &str) -> Descriptor<DescriptorPublicKey> {
        Descriptor::from_str(&format!("wpkh({}/{}/*)", XPUB, derivation_path))
            .expect("must parse descriptor")
    }

    fn spk_hashes(
        descriptor: &Descriptor<DescriptorPublicKey>,
        range: impl IntoIterator<Item = u32>,
    ) -> Vec<ElectrumScriptHash> {
        range
            .into_iter()
            .map(|index| {
                ElectrumScriptHash::new(
                    descriptor
                        .at_derivation_index(index)
                        .expect("must derive")
                        .script_pubkey(),
                )
            })
            .collect()
    }

    #[test]
    fn reinsert_with_larger_next_index_widens_window() {
        let desc = descriptor("0");
        let mut tracker = DerivedSpkTracker::<&str>::new(LOOKAHEAD);

        let initial = tracker.insert_descriptor("keychain", desc.clone(), 0);
        assert_eq!(initial, spk_hashes(&desc, 0..=LOOKAHEAD + 1));

        let widened = tracker.insert_descriptor("keychain", desc.clone(), 10);
        assert_eq!(
            widened,
            spk_hashes(&desc, LOOKAHEAD + 2..=10 + LOOKAHEAD + 1)
        );

        let top_hash = *widened.last().expect("must have widened");
        assert_eq!(
            tracker.index_of_spk_hash(top_hash),
            Some(("keychain", 10 + LOOKAHEAD + 1)),
        );
    }

    #[test]
    fn reinsert_with_equal_or_smaller_next_index_is_noop() {
        let desc = descriptor("0");
        let mut tracker = DerivedSpkTracker::<&str>::new(LOOKAHEAD);

        tracker.insert_descriptor("keychain", desc.clone(), 10);
        let window_before = tracker.all_spk_hashes().collect::<Vec<_>>();

        assert!(tracker
            .insert_descriptor("keychain", desc.clone(), 10)
            .is_empty());
        assert!(tracker
            .insert_descriptor("keychain", desc.clone(), 3)
            .is_empty());
        assert_eq!(tracker.all_spk_hashes().collect::<Vec<_>>(), window_before);
    }

    #[test]
    fn changed_descriptor_clears_and_rebuilds() {
        let old_desc = descriptor("0");
        let new_desc = descriptor("1");
        let mut tracker = DerivedSpkTracker::<&str>::new(LOOKAHEAD);

        let old_hashes = tracker.insert_descriptor("keychain", old_desc, 3);
        let new_hashes = tracker.insert_descriptor("keychain", new_desc.clone(), 0);

        assert_eq!(new_hashes, spk_hashes(&new_desc, 0..=LOOKAHEAD + 1));
        for old_hash in old_hashes {
            assert_eq!(tracker.index_of_spk_hash(old_hash), None);
        }
        assert_eq!(tracker.all_spk_hashes().collect::<Vec<_>>(), new_hashes);
    }

    #[test]
    fn widen_past_window_extended_by_activity() {
        let desc = descriptor("0");
        let mut tracker = DerivedSpkTracker::<&str>::new(LOOKAHEAD);

        tracker.insert_descriptor("keychain", desc.clone(), 0);
        let from_activity = tracker.mark_script_hash_used(&"keychain", LOOKAHEAD + 1);
        let activity_top = LOOKAHEAD + 2 + 1 + LOOKAHEAD;
        assert_eq!(
            from_activity,
            spk_hashes(&desc, (LOOKAHEAD + 2..=activity_top).rev()),
        );

        let widened = tracker.insert_descriptor("keychain", desc.clone(), activity_top);
        assert_eq!(
            widened,
            spk_hashes(&desc, activity_top + 1..=activity_top + LOOKAHEAD + 1),
        );
    }
}
