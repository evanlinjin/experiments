use std::collections::{btree_map, BTreeMap, BTreeSet, HashMap};

use bdk_core::bitcoin::{ScriptBuf, Txid};
use electrum_streaming_client::ElectrumScriptHash;
use miniscript::{Descriptor, DescriptorPublicKey};

/// Why [`DerivedSpkTracker::insert_descriptor`] rejected a descriptor.
///
/// A script hash has a single owner, so a descriptor (or a spk it derives) can only be tracked
/// under one keychain. This mirrors `bdk_chain`'s `KeychainTxOutIndex`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InsertDescriptorError<K> {
    /// The descriptor is already assigned to `keychain`.
    DescriptorAlreadyAssigned { keychain: K },
    /// A spk in the derivation window is already tracked under `keychain` at `index`.
    SpkAlreadyTracked { keychain: K, index: u32 },
}

impl<K> std::fmt::Display for InsertDescriptorError<K> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DescriptorAlreadyAssigned { .. } => {
                write!(f, "descriptor is already assigned to another keychain")
            }
            Self::SpkAlreadyTracked { index, .. } => write!(
                f,
                "derived spk is already tracked under another keychain at index {index}"
            ),
        }
    }
}

impl<K: std::fmt::Debug> std::error::Error for InsertDescriptorError<K> {}

/// Keeps track of spks, and of the txids we expect the server to report for each of them.
///
/// This manages subscriptions to spk histories.
/// * When we reconnect with the Electrum server, we wish to resubscribe to all spks.
/// * When we receive history for a spk, we wish to ensure `lookahead` number of spks above are
///   also tracked.
/// * When we receive history for a spk, we wish to include a `last_active_index` update in case
///   `KeychainTxOutIndex` is not up-to-date.
///
/// A txid we expect under a spk but missing from that spk's history has been evicted.
#[derive(Debug, Clone)]
pub struct DerivedSpkTracker<K> {
    lookahead: u32,
    descriptors: BTreeMap<K, Descriptor<DescriptorPublicKey>>,
    derived_spks: BTreeMap<(K, u32), ElectrumScriptHash>,
    derived_spks_rev: HashMap<ElectrumScriptHash, (K, u32)>,
    expected_txids: HashMap<ElectrumScriptHash, BTreeSet<Txid>>,
}

impl<K: Ord + Clone> DerivedSpkTracker<K> {
    pub fn new(lookahead: u32) -> Self {
        Self {
            lookahead,
            descriptors: BTreeMap::new(),
            derived_spks: BTreeMap::new(),
            derived_spks_rev: HashMap::new(),
            expected_txids: HashMap::new(),
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
            let script_hash = derive_script_hash(descriptor, index);
            if self.derived_spks_rev.contains_key(&script_hash) {
                // `insert_descriptor` rejects overlapping windows, but widening can still reach a
                // spk owned by another keychain. Leave it with its owner.
                tracing::warn!(index, "Skipping spk already tracked under another keychain");
                return None;
            }
            spk_hash_entry.insert(script_hash);
            self.derived_spks_rev.insert(script_hash, (keychain, index));
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
            self.expected_txids.remove(&script_hash);
        }
    }

    /// Track `descriptor` under `keychain` and return the newly derived script hashes.
    ///
    /// The derivation window is a per-keychain high-water mark: re-inserting the same descriptor
    /// with a larger `next_index` widens the window and returns only the script hashes derived by
    /// the widening, while an equal or smaller `next_index` is a no-op. Inserting a different
    /// descriptor discards the keychain's tracked spks and rebuilds the window from scratch.
    ///
    /// Errors, leaving the tracker untouched, if `descriptor` is already assigned to another
    /// keychain or any spk of the new window is already tracked under another keychain.
    ///
    /// `expected_spk_txids` are the `(spk, txid)` pairs we expect the server to report, as
    /// produced by `TxGraph::list_expected_spk_txids`. Only these txids can be reported as
    /// evicted. They add to what is already expected.
    pub fn insert_descriptor(
        &mut self,
        keychain: K,
        descriptor: Descriptor<DescriptorPublicKey>,
        next_index: u32,
        expected_spk_txids: impl IntoIterator<Item = (ScriptBuf, Txid)>,
    ) -> Result<Vec<ElectrumScriptHash>, InsertDescriptorError<K>> {
        if let Some(other) = self
            .descriptors
            .iter()
            .find_map(|(k, d)| (*k != keychain && *d == descriptor).then_some(k))
        {
            return Err(InsertDescriptorError::DescriptorAlreadyAssigned {
                keychain: other.clone(),
            });
        }
        let same_descriptor = self.descriptors.get(&keychain) == Some(&descriptor);
        for index in 0_u32..=next_index + self.lookahead + 1 {
            if same_descriptor && self.derived_spks.contains_key(&(keychain.clone(), index)) {
                continue;
            }
            if let Some((other, other_index)) = self
                .derived_spks_rev
                .get(&derive_script_hash(&descriptor, index))
                .filter(|(other, _)| *other != keychain)
            {
                return Err(InsertDescriptorError::SpkAlreadyTracked {
                    keychain: other.clone(),
                    index: *other_index,
                });
            }
        }

        if let Some(old_descriptor) = self
            .descriptors
            .insert(keychain.clone(), descriptor.clone())
        {
            if old_descriptor != descriptor {
                self._clear_tracked_spks_of_keychain(keychain.clone());
            }
        }
        let new_script_hashes = (0_u32..=next_index + self.lookahead + 1)
            .filter_map(|index| self._add_derived_spk(keychain.clone(), index))
            .collect();
        for (spk, txid) in expected_spk_txids {
            self.expected_txids
                .entry(ElectrumScriptHash::new(&spk))
                .or_default()
                .insert(txid);
        }
        Ok(new_script_hashes)
    }

    /// Whether we expect any txids in `script_hash`'s history.
    pub fn has_expected_txids(&self, script_hash: ElectrumScriptHash) -> bool {
        self.expected_txids
            .get(&script_hash)
            .is_some_and(|txids| !txids.is_empty())
    }

    /// The txids we expect in `script_hash`'s history.
    ///
    /// Only replace it with a history the server reported: the caller's view may lag behind.
    pub fn expected_txids(&mut self, script_hash: ElectrumScriptHash) -> &mut BTreeSet<Txid> {
        self.expected_txids.entry(script_hash).or_default()
    }

    pub fn mark_script_hash_used(&mut self, keychain: &K, index: u32) -> Vec<ElectrumScriptHash> {
        let next_index = index + 1;

        let mut spk_hashes = Vec::new();
        // We iterate the derivation indices backwards so that we return script hashes that starts
        // with the latest spk, since we want to send request for later spks first.
        for index in (next_index..=next_index + 1 + self.lookahead).rev() {
            if self.derived_spks.contains_key(&(keychain.clone(), index)) {
                break;
            }
            spk_hashes.extend(self._add_derived_spk(keychain.clone(), index));
        }
        spk_hashes
    }
}

fn derive_script_hash(
    descriptor: &Descriptor<DescriptorPublicKey>,
    index: u32,
) -> ElectrumScriptHash {
    let spk = descriptor
        .at_derivation_index(index)
        .expect("descriptor must derive")
        .script_pubkey();
    ElectrumScriptHash::new(&spk)
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

    fn spk(descriptor: &Descriptor<DescriptorPublicKey>, index: u32) -> ScriptBuf {
        descriptor
            .at_derivation_index(index)
            .expect("must derive")
            .script_pubkey()
    }

    fn txid(byte: u8) -> Txid {
        use bdk_core::bitcoin::hashes::Hash;
        Txid::from_byte_array([byte; 32])
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

        let initial = tracker
            .insert_descriptor("keychain", desc.clone(), 0, [])
            .expect("must insert");
        assert_eq!(initial, spk_hashes(&desc, 0..=LOOKAHEAD + 1));

        let widened = tracker
            .insert_descriptor("keychain", desc.clone(), 10, [])
            .expect("must insert");
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

        tracker
            .insert_descriptor("keychain", desc.clone(), 10, [])
            .expect("must insert");
        let window_before = tracker.all_spk_hashes().collect::<Vec<_>>();

        assert!(tracker
            .insert_descriptor("keychain", desc.clone(), 10, [])
            .expect("must insert")
            .is_empty());
        assert!(tracker
            .insert_descriptor("keychain", desc.clone(), 3, [])
            .expect("must insert")
            .is_empty());
        assert_eq!(tracker.all_spk_hashes().collect::<Vec<_>>(), window_before);
    }

    #[test]
    fn changed_descriptor_clears_and_rebuilds() {
        let old_desc = descriptor("0");
        let new_desc = descriptor("1");
        let mut tracker = DerivedSpkTracker::<&str>::new(LOOKAHEAD);

        let old_hashes = tracker
            .insert_descriptor("keychain", old_desc, 3, [])
            .expect("must insert");
        let new_hashes = tracker
            .insert_descriptor("keychain", new_desc.clone(), 0, [])
            .expect("must insert");

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

        tracker
            .insert_descriptor("keychain", desc.clone(), 0, [])
            .expect("must insert");
        let from_activity = tracker.mark_script_hash_used(&"keychain", LOOKAHEAD + 1);
        let activity_top = LOOKAHEAD + 2 + 1 + LOOKAHEAD;
        assert_eq!(
            from_activity,
            spk_hashes(&desc, (LOOKAHEAD + 2..=activity_top).rev()),
        );

        let widened = tracker
            .insert_descriptor("keychain", desc.clone(), activity_top, [])
            .expect("must insert");
        assert_eq!(
            widened,
            spk_hashes(&desc, activity_top + 1..=activity_top + LOOKAHEAD + 1),
        );
    }

    /// Expectations the caller adds must survive until a history response contradicts them.
    #[test]
    fn caller_expectations_accumulate_and_only_the_server_retracts() {
        let desc = descriptor("0");
        let mut tracker = DerivedSpkTracker::<&str>::new(LOOKAHEAD);
        let hash = ElectrumScriptHash::new(spk(&desc, 0));

        tracker
            .insert_descriptor("keychain", desc.clone(), 0, [(spk(&desc, 0), txid(1))])
            .expect("must insert");
        assert_eq!(*tracker.expected_txids(hash), BTreeSet::from([txid(1)]));

        // A caller whose view has not caught up must not drop what it has not heard about yet.
        tracker
            .insert_descriptor("keychain", desc.clone(), 0, [(spk(&desc, 0), txid(2))])
            .expect("must insert");
        assert_eq!(
            *tracker.expected_txids(hash),
            BTreeSet::from([txid(1), txid(2)]),
        );
        tracker
            .insert_descriptor("keychain", desc.clone(), 0, [])
            .expect("must insert");
        assert_eq!(
            *tracker.expected_txids(hash),
            BTreeSet::from([txid(1), txid(2)]),
            "an empty caller view must not retract",
        );

        *tracker.expected_txids(hash) = [txid(2)].into();
        assert_eq!(
            *tracker.expected_txids(hash),
            BTreeSet::from([txid(2)]),
            "the server's report replaces what we expect",
        );
    }

    #[test]
    fn expectations_outside_the_derivation_window_apply_once_it_reaches_them() {
        let desc = descriptor("0");
        let mut tracker = DerivedSpkTracker::<&str>::new(LOOKAHEAD);
        let outside = LOOKAHEAD + 2;
        let hash = ElectrumScriptHash::new(spk(&desc, outside));

        tracker
            .insert_descriptor(
                "keychain",
                desc.clone(),
                0,
                [(spk(&desc, outside), txid(1))],
            )
            .expect("must insert");
        assert_eq!(tracker.index_of_spk_hash(hash), None);

        tracker
            .insert_descriptor("keychain", desc.clone(), outside, [])
            .expect("must insert");
        assert_eq!(tracker.index_of_spk_hash(hash), Some(("keychain", outside)));
        assert_eq!(*tracker.expected_txids(hash), BTreeSet::from([txid(1)]));
    }

    #[test]
    fn changed_descriptor_discards_expectations_with_its_spks() {
        let old_desc = descriptor("0");
        let new_desc = descriptor("1");
        let mut tracker = DerivedSpkTracker::<&str>::new(LOOKAHEAD);
        let old_hash = ElectrumScriptHash::new(spk(&old_desc, 0));

        tracker
            .insert_descriptor(
                "keychain",
                old_desc.clone(),
                0,
                [(spk(&old_desc, 0), txid(1))],
            )
            .expect("must insert");
        assert!(tracker.has_expected_txids(old_hash));

        tracker
            .insert_descriptor("keychain", new_desc, 0, [])
            .expect("must insert");
        assert!(!tracker.has_expected_txids(old_hash));
    }

    #[test]
    fn descriptor_assigned_to_another_keychain_is_rejected() {
        let desc_a = descriptor("0");
        let desc_b = descriptor("1");
        let mut tracker = DerivedSpkTracker::<&str>::new(LOOKAHEAD);
        let hash_a = ElectrumScriptHash::new(spk(&desc_a, 0));

        tracker
            .insert_descriptor("a", desc_a.clone(), 0, [(spk(&desc_a, 0), txid(1))])
            .expect("must insert");
        tracker
            .insert_descriptor("b", desc_b.clone(), 0, [])
            .expect("must insert");
        let window_before = tracker.all_spk_hashes().collect::<Vec<_>>();

        assert_eq!(
            tracker.insert_descriptor("c", desc_a.clone(), 10, []),
            Err(InsertDescriptorError::DescriptorAlreadyAssigned { keychain: "a" }),
        );
        // Replacing `a`'s descriptor with `b`'s must not clear `a` before failing.
        assert_eq!(
            tracker.insert_descriptor("a", desc_b, 10, []),
            Err(InsertDescriptorError::DescriptorAlreadyAssigned { keychain: "b" }),
        );
        assert_eq!(tracker.all_spk_hashes().collect::<Vec<_>>(), window_before);
        assert_eq!(tracker.index_of_spk_hash(hash_a), Some(("a", 0)));
        assert_eq!(*tracker.expected_txids(hash_a), BTreeSet::from([txid(1)]));

        // The same keychain may still widen.
        assert_eq!(
            tracker.insert_descriptor("a", desc_a.clone(), 1, []),
            Ok(spk_hashes(&desc_a, [LOOKAHEAD + 2])),
        );
    }

    #[test]
    fn overlapping_derivation_window_is_rejected() {
        let desc = descriptor("0");
        // A distinct descriptor whose every index derives `desc`'s spk at index 3.
        let overlapping = Descriptor::from_str(&format!("wpkh({}/0/3)", XPUB)).expect("must parse");
        let mut tracker = DerivedSpkTracker::<&str>::new(LOOKAHEAD);

        tracker
            .insert_descriptor("a", desc.clone(), 0, [])
            .expect("must insert");
        let window_before = tracker.all_spk_hashes().collect::<Vec<_>>();

        assert_eq!(
            tracker.insert_descriptor("b", overlapping, 0, []),
            Err(InsertDescriptorError::SpkAlreadyTracked {
                keychain: "a",
                index: 3
            }),
        );
        assert_eq!(tracker.all_spk_hashes().collect::<Vec<_>>(), window_before);
    }

    #[test]
    fn widening_into_another_keychains_spk_skips_it() {
        let desc = descriptor("0");
        let collision = LOOKAHEAD + 4;
        let other =
            Descriptor::from_str(&format!("wpkh({}/0/{})", XPUB, collision)).expect("must parse");
        let mut tracker = DerivedSpkTracker::<&str>::new(LOOKAHEAD);

        tracker
            .insert_descriptor("a", desc.clone(), 0, [])
            .expect("must insert");
        tracker
            .insert_descriptor("b", other, 0, [])
            .expect("must insert");

        let from_activity = tracker.mark_script_hash_used(&"a", LOOKAHEAD + 1);
        let activity_top = LOOKAHEAD + 2 + 1 + LOOKAHEAD;
        assert_eq!(
            from_activity,
            spk_hashes(
                &desc,
                (LOOKAHEAD + 2..=activity_top)
                    .rev()
                    .filter(|&i| i != collision)
            ),
        );
        assert_eq!(
            tracker.index_of_spk_hash(ElectrumScriptHash::new(spk(&desc, collision))),
            Some(("b", 0)),
        );
    }
}
