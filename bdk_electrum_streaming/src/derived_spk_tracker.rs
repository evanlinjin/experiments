use std::collections::{btree_map, BTreeMap, BTreeSet, HashMap};

use bdk_core::bitcoin::{ScriptBuf, Txid};
use electrum_streaming_client::ElectrumScriptHash;
use miniscript::{Descriptor, DescriptorPublicKey};

/// Keeps track of spks, and of the txids we expect the server to report for each of them.
///
/// This manages subscriptions to spk histories.
/// * When we reconnect with the Electrum server, we wish to resubscribe to all spks.
/// * When we receive history for a spk, we wish to ensure `lookahead` number of spks above are
///   also tracked.
/// * When we receive history for a spk, we wish to include a `last_active_index` update in case
///   `KeychainTxOutIndex` is not up-to-date.
///
/// A tracked spk also carries the txids we expect to appear in that spk's history. Detecting that
/// a transaction has been evicted from the mempool is a set difference, and this is its left-hand
/// side: a txid expected under a spk but absent from that spk's history has been evicted. The
/// expectations are supplied by the caller in [`insert_descriptor`], because when a subscription
/// is created only the caller knows what it already believes is there.
///
/// [`insert_descriptor`]: Self::insert_descriptor
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
    /// `expected_spk_txids` states which txids the server should report under which spk, in the
    /// `(spk, txid)` shape `bdk_chain`'s `TxGraph::list_expected_spk_txids` produces. Pass it the
    /// canonical view of `keychain`: a transaction the caller never says it expects cannot be
    /// reported as evicted, so a subscription created with nothing here is one that can only ever
    /// add transactions, never retract them.
    ///
    /// Expectations are cumulative, and only the server retracts them (see
    /// [`set_expected_txids`]). Re-calling with a view that has not yet caught up with the server
    /// therefore cannot drop an expectation, which is what makes this safe to over-call. Pairs
    /// naming a spk outside the derivation window are ignored: without a subscription there is no
    /// history to compare an expectation against.
    ///
    /// [`set_expected_txids`]: Self::set_expected_txids
    pub fn insert_descriptor(
        &mut self,
        keychain: K,
        descriptor: Descriptor<DescriptorPublicKey>,
        next_index: u32,
        expected_spk_txids: impl IntoIterator<Item = (ScriptBuf, Txid)>,
    ) -> Vec<ElectrumScriptHash> {
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
            let script_hash = ElectrumScriptHash::new(&spk);
            if self.derived_spks_rev.contains_key(&script_hash) {
                self.expected_txids
                    .entry(script_hash)
                    .or_default()
                    .insert(txid);
            }
        }
        new_script_hashes
    }

    /// The txids we expect the server to report in `script_hash`'s history.
    pub fn expected_txids(&self, script_hash: ElectrumScriptHash) -> Option<&BTreeSet<Txid>> {
        self.expected_txids.get(&script_hash)
    }

    /// Replace what we expect for `script_hash` with what the server has just reported for it.
    ///
    /// Only the server may retract an expectation. [`insert_descriptor`] lets the caller say what
    /// it believes is there, but a caller's view can lag the server's, so treating its silence as
    /// a retraction would drop transactions the server is still reporting.
    ///
    /// Ignored for an untracked spk, which has no subscription and so no history to report.
    ///
    /// [`insert_descriptor`]: Self::insert_descriptor
    pub fn set_expected_txids(
        &mut self,
        script_hash: ElectrumScriptHash,
        txids: impl IntoIterator<Item = Txid>,
    ) {
        if !self.derived_spks_rev.contains_key(&script_hash) {
            return;
        }
        let txids = txids.into_iter().collect::<BTreeSet<_>>();
        if txids.is_empty() {
            self.expected_txids.remove(&script_hash);
        } else {
            self.expected_txids.insert(script_hash, txids);
        }
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

        let initial = tracker.insert_descriptor("keychain", desc.clone(), 0, []);
        assert_eq!(initial, spk_hashes(&desc, 0..=LOOKAHEAD + 1));

        let widened = tracker.insert_descriptor("keychain", desc.clone(), 10, []);
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

        tracker.insert_descriptor("keychain", desc.clone(), 10, []);
        let window_before = tracker.all_spk_hashes().collect::<Vec<_>>();

        assert!(tracker
            .insert_descriptor("keychain", desc.clone(), 10, [])
            .is_empty());
        assert!(tracker
            .insert_descriptor("keychain", desc.clone(), 3, [])
            .is_empty());
        assert_eq!(tracker.all_spk_hashes().collect::<Vec<_>>(), window_before);
    }

    #[test]
    fn changed_descriptor_clears_and_rebuilds() {
        let old_desc = descriptor("0");
        let new_desc = descriptor("1");
        let mut tracker = DerivedSpkTracker::<&str>::new(LOOKAHEAD);

        let old_hashes = tracker.insert_descriptor("keychain", old_desc, 3, []);
        let new_hashes = tracker.insert_descriptor("keychain", new_desc.clone(), 0, []);

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

        tracker.insert_descriptor("keychain", desc.clone(), 0, []);
        let from_activity = tracker.mark_script_hash_used(&"keychain", LOOKAHEAD + 1);
        let activity_top = LOOKAHEAD + 2 + 1 + LOOKAHEAD;
        assert_eq!(
            from_activity,
            spk_hashes(&desc, (LOOKAHEAD + 2..=activity_top).rev()),
        );

        let widened = tracker.insert_descriptor("keychain", desc.clone(), activity_top, []);
        assert_eq!(
            widened,
            spk_hashes(&desc, activity_top + 1..=activity_top + LOOKAHEAD + 1),
        );
    }

    /// An expectation is the left-hand side of the eviction set difference, so it must survive
    /// from the moment the caller states it to the moment a history response contradicts it.
    #[test]
    fn caller_expectations_accumulate_and_only_the_server_retracts() {
        let desc = descriptor("0");
        let mut tracker = DerivedSpkTracker::<&str>::new(LOOKAHEAD);
        let hash = ElectrumScriptHash::new(spk(&desc, 0));

        tracker.insert_descriptor("keychain", desc.clone(), 0, [(spk(&desc, 0), txid(1))]);
        assert_eq!(tracker.expected_txids(hash), Some(&[txid(1)].into()));

        // A caller whose view has not caught up must not drop what it has not heard about yet.
        tracker.insert_descriptor("keychain", desc.clone(), 0, [(spk(&desc, 0), txid(2))]);
        assert_eq!(
            tracker.expected_txids(hash),
            Some(&[txid(1), txid(2)].into()),
        );
        tracker.insert_descriptor("keychain", desc.clone(), 0, []);
        assert_eq!(
            tracker.expected_txids(hash),
            Some(&[txid(1), txid(2)].into()),
            "an empty caller view must not retract",
        );

        tracker.set_expected_txids(hash, [txid(2)]);
        assert_eq!(
            tracker.expected_txids(hash),
            Some(&[txid(2)].into()),
            "the server's report replaces what we expect",
        );

        tracker.set_expected_txids(hash, []);
        assert_eq!(tracker.expected_txids(hash), None);
    }

    #[test]
    fn expectations_outside_the_derivation_window_are_ignored() {
        let desc = descriptor("0");
        let mut tracker = DerivedSpkTracker::<&str>::new(LOOKAHEAD);
        let outside = LOOKAHEAD + 2;

        tracker.insert_descriptor(
            "keychain",
            desc.clone(),
            0,
            [(spk(&desc, outside), txid(1))],
        );

        let hash = ElectrumScriptHash::new(spk(&desc, outside));
        assert_eq!(tracker.index_of_spk_hash(hash), None);
        assert_eq!(tracker.expected_txids(hash), None);
        tracker.set_expected_txids(hash, [txid(1)]);
        assert_eq!(tracker.expected_txids(hash), None);
    }

    #[test]
    fn changed_descriptor_discards_expectations_with_its_spks() {
        let old_desc = descriptor("0");
        let new_desc = descriptor("1");
        let mut tracker = DerivedSpkTracker::<&str>::new(LOOKAHEAD);
        let old_hash = ElectrumScriptHash::new(spk(&old_desc, 0));

        tracker.insert_descriptor(
            "keychain",
            old_desc.clone(),
            0,
            [(spk(&old_desc, 0), txid(1))],
        );
        assert!(tracker.expected_txids(old_hash).is_some());

        tracker.insert_descriptor("keychain", new_desc, 0, []);
        assert_eq!(tracker.expected_txids(old_hash), None);
    }
}
