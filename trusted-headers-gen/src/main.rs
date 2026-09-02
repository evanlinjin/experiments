//! Fetches a header from a Bitcoin Core node and writes it to `./trusted_headers.rs` (or
//! wherever `--out` says), for whichever network that node is running.
//!
//! Bitcoin Core only ever serves one network per RPC endpoint, so this is meant to be run once
//! per network you want a trusted header for, each time pointed at a node on that network. Every
//! run only touches its own network's section of the output file, adding a new height or
//! replacing an existing one — the other networks' sections are left exactly as they were.
//!
//! What comes out is a Rust module to vendor into whichever crate needs it, exposing a
//! `..._TRUSTED_HEADERS` per network plus a `trusted_headers()` map over them. Nothing ships
//! this data for you: trusting a header means asserting it is canonical, so it belongs in the
//! tree of whoever checked it, having been reviewed as the diff it is. That is also why the
//! output lands in the current directory by default and placing it takes an explicit `--out`.
//!
//! ```text
//! cargo run -p trusted-headers-gen -- \
//!     --url http://127.0.0.1:8332 \
//!     --cookie ~/.bitcoin/.cookie \
//!     --network bitcoin \
//!     [--height 903168] \
//!     [--out trusted_headers.rs]
//! ```
//!
//! `--height` defaults to the *second* highest difficulty-adjustment boundary (a multiple of
//! 2016) at or below the node's tip — a boundary, because a header-verifying client needs its
//! highest trusted header on one to recompute every retarget above it, and the second one
//! because the highest can sit right at the tip. A lower, explicit `--height` is for backfill:
//! a trusted block below where syncing starts needs no such alignment.

use std::{
    collections::BTreeMap,
    fmt::Write as _,
    fs,
    path::{Path, PathBuf},
    str::FromStr,
};

use bdk_core::bitcoin::{block::Header, consensus::encode::deserialize_hex, BlockHash, Network};
use corepc_client::client_sync::{v17::Client, Auth};

/// Per-network state: trusted heights and the raw header hex at each. Rebuilt from the existing
/// output file (if any) on every run, so a run for one network cannot lose another's entries.
type State = BTreeMap<Network, BTreeMap<u32, String>>;

const ALL_NETWORKS: [Network; 5] = [
    Network::Bitcoin,
    Network::Testnet,
    Network::Testnet4,
    Network::Signet,
    Network::Regtest,
];

/// The prefix this network's generated consts carry, e.g. `Network::Bitcoin` -> `MAINNET`.
fn const_prefix(network: Network) -> &'static str {
    match network {
        Network::Bitcoin => "MAINNET",
        Network::Testnet => "TESTNET",
        Network::Testnet4 => "TESTNET4",
        Network::Signet => "SIGNET",
        Network::Regtest => "REGTEST",
    }
}

/// What `getblockchaininfo` calls this network, as defined by BIP70.
fn chain_name(network: Network) -> &'static str {
    match network {
        Network::Bitcoin => "main",
        Network::Testnet => "test",
        Network::Testnet4 => "testnet4",
        Network::Signet => "signet",
        Network::Regtest => "regtest",
    }
}

struct Args {
    url: String,
    cookie: Option<PathBuf>,
    user: Option<String>,
    pass: Option<String>,
    network: Network,
    height: Option<u32>,
    out: PathBuf,
}

impl Args {
    fn parse() -> anyhow::Result<Self> {
        let mut url = None;
        let mut cookie = None;
        let mut user = None;
        let mut pass = None;
        let mut network = None;
        let mut height = None;
        let mut out = None;

        let mut args = std::env::args().skip(1);
        while let Some(flag) = args.next() {
            let mut value = || {
                args.next()
                    .ok_or_else(|| anyhow::anyhow!("{flag} needs a value"))
            };
            match flag.as_str() {
                "--url" => url = Some(value()?),
                "--cookie" => cookie = Some(PathBuf::from(value()?)),
                "--user" => user = Some(value()?),
                "--pass" => pass = Some(value()?),
                "--network" => {
                    network = Some(match value()?.as_str() {
                        "bitcoin" | "mainnet" => Network::Bitcoin,
                        "testnet" | "testnet3" => Network::Testnet,
                        "testnet4" => Network::Testnet4,
                        "signet" => Network::Signet,
                        "regtest" => Network::Regtest,
                        other => anyhow::bail!("unknown --network {other}"),
                    })
                }
                "--height" => height = Some(value()?.parse()?),
                "--out" => out = Some(PathBuf::from(value()?)),
                other => anyhow::bail!("unknown flag {other}"),
            }
        }

        Ok(Self {
            url: url.ok_or_else(|| anyhow::anyhow!("--url is required"))?,
            cookie,
            user,
            pass,
            network: network.ok_or_else(|| anyhow::anyhow!("--network is required"))?,
            height,
            out: out.unwrap_or_else(|| PathBuf::from("trusted_headers.rs")),
        })
    }
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse()?;

    let auth = match (&args.cookie, &args.user, &args.pass) {
        (Some(cookie), _, _) => Auth::CookieFile(cookie.clone()),
        (None, Some(user), Some(pass)) => Auth::UserPass(user.clone(), pass.clone()),
        _ => Auth::None,
    };
    let client = if matches!(auth, Auth::None) {
        Client::new(&args.url)
    } else {
        Client::new_with_auth(&args.url, auth).map_err(|e| anyhow::anyhow!(e.to_string()))?
    };

    let (height, hash, header_hex) = fetch_trusted_header(&client, args.network, args.height)?;

    let mut state = parse_existing(&args.out);
    state
        .entry(args.network)
        .or_default()
        .insert(height, header_hex);
    fs::write(&args.out, render_file(&state))?;
    let _ = std::process::Command::new("rustfmt")
        .arg(&args.out)
        .status();

    println!(
        "{:?}: trusted header at height {height} ({hash}) written to {}",
        args.network,
        args.out.display(),
    );
    Ok(())
}

/// Blocks between difficulty adjustments.
const RETARGET_INTERVAL: u32 = 2016;

/// The height to trust when the caller does not name one: the *second* highest
/// difficulty-adjustment boundary at or below `tip`.
///
/// A boundary, because the highest trusted block has to sit on one for every retarget above it
/// to be recomputable rather than taken on faith. The second one rather than the highest,
/// because the highest can be the tip itself — and a block a handful of confirmations deep is
/// no basis for trust. Stepping back a whole retarget period puts the anchor between 2016 and
/// 4032 blocks behind the tip, deep enough that reorging past it is not a live concern.
///
/// Chains too short for that (a fresh regtest, say) fall back to genesis.
fn default_trusted_height(tip: u32) -> u32 {
    (tip - tip % RETARGET_INTERVAL).saturating_sub(RETARGET_INTERVAL)
}

/// Ask the node for the header to trust, returning its height, hash and raw hex.
///
/// `height` defaults to [`default_trusted_height`]. The node is held to `network` first: a
/// header filed under the wrong network is a poisoned trust anchor, and the node is the one
/// thing that actually knows which chain it is serving. The header is then decoded and checked
/// to hash to the block the node named, so nothing further downstream has to.
fn fetch_trusted_header(
    client: &Client,
    network: Network,
    height: Option<u32>,
) -> anyhow::Result<(u32, BlockHash, String)> {
    // Read `chain` out of the raw response rather than through a typed one: the shape of the
    // rest of `getblockchaininfo` has changed across Core releases, and `chain` is the only
    // field here that matters.
    let info: serde_json::Value = client
        .call("getblockchaininfo", &[])
        .map_err(|e| anyhow::anyhow!("getblockchaininfo: {e}"))?;
    let chain = info
        .get("chain")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("getblockchaininfo gave no `chain`"))?;
    anyhow::ensure!(
        chain == chain_name(network),
        "node is on {chain}, but --network says {network:?} ({})",
        chain_name(network),
    );

    let tip = client
        .get_block_count()
        .map_err(|e| anyhow::anyhow!("getblockcount: {e}"))?
        .0 as u32;
    let height = height.unwrap_or_else(|| default_trusted_height(tip));
    anyhow::ensure!(
        height <= tip,
        "height {height} is above the node's tip {tip}"
    );

    let hash = client
        .get_block_hash(height as u64)
        .map_err(|e| anyhow::anyhow!("getblockhash({height}): {e}"))?
        .0;
    let hash =
        BlockHash::from_str(&hash).map_err(|e| anyhow::anyhow!("server gave a bad hash: {e}"))?;
    let header_hex = client
        .get_block_header(&hash)
        .map_err(|e| anyhow::anyhow!("getblockheader({hash}): {e}"))?
        .0;
    // Checked once here, so the generated file never needs to check it again.
    let header: Header = deserialize_hex(&header_hex)?;
    anyhow::ensure!(
        header.block_hash() == hash,
        "server gave a header for a different block than the hash it named"
    );
    Ok((height, hash, header_hex))
}

/// Read back whatever `(height, hex)` pairs the file already has, per network — empty if the
/// file does not exist yet, or has nothing for a given network.
fn parse_existing(path: &Path) -> State {
    let mut state = State::new();
    let Ok(text) = fs::read_to_string(path) else {
        return state;
    };
    for network in ALL_NETWORKS {
        let prefix = const_prefix(network);
        let Some(body) = section_body(&text, prefix) else {
            continue;
        };
        let mut heights = BTreeMap::new();
        for line in body.lines() {
            if let Some((height, hex)) = parse_entry_line(line) {
                heights.insert(height, hex);
            }
        }
        if !heights.is_empty() {
            state.insert(network, heights);
        }
    }
    state
}

fn section_body<'a>(text: &'a str, prefix: &str) -> Option<&'a str> {
    let begin = format!("// --- BEGIN {prefix} ---");
    let end = format!("// --- END {prefix} ---");
    text.split(&begin).nth(1)?.split(&end).next()
}

/// Parse a rendered `(height, "hex"),` entry line — the only shape of line this tool ever
/// writes inside a section body.
fn parse_entry_line(line: &str) -> Option<(u32, String)> {
    let rest = line.trim().strip_prefix('(')?;
    let (height, rest) = rest.split_once(',')?;
    let height = height.trim().parse().ok()?;
    let hex = rest.trim().strip_prefix('"')?;
    let hex = hex.split('"').next()?;
    Some((height, hex.to_string()))
}

/// Render the whole module. `state` is never empty — a run always has a header to add — so
/// every import and helper written here is used by at least one section below.
fn render_file(state: &State) -> String {
    let mut out = String::new();
    let w = &mut out;
    writeln!(
        w,
        "// @generated by `cargo run -p trusted-headers-gen`.\n\
         // Do not hand-edit — rerun the generator instead; each run only touches the network\n\
         // it points at."
    )
    .unwrap();
    writeln!(w).unwrap();
    writeln!(w, "use std::{{collections::BTreeMap, sync::LazyLock}};").unwrap();
    writeln!(w).unwrap();
    writeln!(
        w,
        "use bdk_core::bitcoin::{{block::Header, consensus::encode::deserialize_hex, Network}};"
    )
    .unwrap();
    writeln!(w).unwrap();
    writeln!(
        w,
        "/// Deserialize each `(height, hex)` pair — checked valid when this file was generated."
    )
    .unwrap();
    writeln!(
        w,
        "fn parse<const N: usize>(raw: [(u32, &str); N]) -> [(u32, Header); N] {{"
    )
    .unwrap();
    writeln!(
        w,
        "    raw.map(|(height, hex)| (height, deserialize_hex(hex).expect(\"checked at generation time\")))"
    )
    .unwrap();
    writeln!(w, "}}").unwrap();
    writeln!(w).unwrap();

    for network in ALL_NETWORKS {
        let Some(headers) = state.get(&network) else {
            continue;
        };
        let prefix = const_prefix(network);
        let n = headers.len();
        writeln!(w, "// --- BEGIN {prefix} ---").unwrap();
        writeln!(
            w,
            "const {prefix}_TRUSTED_HEADERS_HEX: [(u32, &str); {n}] = ["
        )
        .unwrap();
        for (height, hex) in headers {
            writeln!(w, "    ({height}, \"{hex}\"),").unwrap();
        }
        writeln!(w, "];").unwrap();
        writeln!(w).unwrap();
        writeln!(w, "/// Trusted headers for `Network::{network:?}`.").unwrap();
        writeln!(
            w,
            "pub static {prefix}_TRUSTED_HEADERS: LazyLock<[(u32, Header); {n}]> ="
        )
        .unwrap();
        writeln!(
            w,
            "    LazyLock::new(|| parse({prefix}_TRUSTED_HEADERS_HEX));"
        )
        .unwrap();
        writeln!(w, "// --- END {prefix} ---").unwrap();
        writeln!(w).unwrap();
    }

    writeln!(
        w,
        "/// Every network there are trusted headers here for, keyed by [`Network`]."
    )
    .unwrap();
    writeln!(
        w,
        "pub fn trusted_headers() -> BTreeMap<Network, &'static [(u32, Header)]> {{"
    )
    .unwrap();
    writeln!(w, "    let mut map = BTreeMap::new();").unwrap();
    for network in ALL_NETWORKS {
        if !state.contains_key(&network) {
            continue;
        }
        let prefix = const_prefix(network);
        writeln!(
            w,
            "    map.insert(Network::{network:?}, {prefix}_TRUSTED_HEADERS.as_slice());"
        )
        .unwrap();
    }
    writeln!(w, "    map").unwrap();
    writeln!(w, "}}").unwrap();

    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_state() -> State {
        let mut state = State::new();
        state.insert(
            Network::Bitcoin,
            BTreeMap::from([(902016, "aa".repeat(80))]),
        );
        state.insert(
            Network::Signet,
            BTreeMap::from([(0, "bb".repeat(80)), (2016, "cc".repeat(80))]),
        );
        state
    }

    /// The default height must always land on a retarget boundary, and always a whole period
    /// behind the tip — never the boundary the tip itself may be sitting on.
    #[test]
    fn default_height_steps_back_a_whole_retarget_period() {
        // 2016 * 450 == 907_200, so that is a boundary and 905_184 is the one below it.
        assert_eq!(
            default_trusted_height(907_200),
            905_184,
            "tip on a boundary"
        );
        assert_eq!(default_trusted_height(907_201), 905_184, "just past one");
        assert_eq!(default_trusted_height(907_199), 903_168, "just before one");

        for tip in [2016_u32, 4032, 100_000, 907_201, 1_000_000] {
            let height = default_trusted_height(tip);
            assert_eq!(height % RETARGET_INTERVAL, 0, "{height} is not a boundary");
            let depth = tip - height;
            assert!(
                (RETARGET_INTERVAL..=RETARGET_INTERVAL * 2).contains(&depth),
                "tip {tip} put the anchor {depth} blocks deep",
            );
        }

        // Chains with no such boundary behind them fall back to genesis.
        for tip in [0, 1, 5, 2015] {
            assert_eq!(default_trusted_height(tip), 0, "short chain at tip {tip}");
        }
    }

    /// The RPC path itself, against a real `bitcoind`: what the tool fetches has to be the
    /// header that node actually has at that height, and it has to survive the trip out into
    /// generated source and back.
    ///
    /// Needs a `bitcoind` executable — `BITCOIND_EXE`, a downloaded one, or one on `PATH`.
    /// Skipped when there is none, since that is a missing environment rather than a broken
    /// tool.
    #[test]
    fn fetches_a_real_header_from_bitcoin_core() -> anyhow::Result<()> {
        let Ok(exe) = bdk_testenv::bitcoind::exe_path() else {
            eprintln!("skipping: no bitcoind executable found");
            return Ok(());
        };
        let node = bdk_testenv::bitcoind::BitcoinD::new(exe)?;
        let address = node.client.new_address()?;
        node.client.generate_to_address(5, &address)?;

        let client = Client::new_with_auth(
            &node.rpc_url(),
            Auth::CookieFile(node.params.cookie_file.clone()),
        )
        .map_err(|e| anyhow::anyhow!(e.to_string()))?;

        // An explicit height, held to the hash the node itself reports for it.
        let (height, hash, hex) = fetch_trusted_header(&client, Network::Regtest, Some(3))?;
        assert_eq!(height, 3);
        assert_eq!(hash, node.client.get_block_hash(3)?.block_hash()?);
        assert_eq!(hex.len(), 160, "a header is 80 bytes, so 160 hex chars");

        // No height given: this chain is far too short to have a whole retarget period behind
        // a boundary, so it falls back to genesis.
        let (default_height, genesis, _) = fetch_trusted_header(&client, Network::Regtest, None)?;
        assert_eq!(default_height, 0);
        assert_eq!(genesis, node.client.get_block_hash(0)?.block_hash()?);

        // A height the node cannot answer for is refused, not quietly turned into something.
        assert!(fetch_trusted_header(&client, Network::Regtest, Some(500)).is_err());

        // A regtest node cannot be talked into supplying a header filed under mainnet: the
        // node knows which chain it serves, and mislabelling one poisons the trust anchor.
        let err = fetch_trusted_header(&client, Network::Bitcoin, Some(3))
            .expect_err("a regtest node must not answer for mainnet")
            .to_string();
        assert!(err.contains("node is on regtest"), "{err}");

        // What came back renders into source that parses back to the very same hex.
        let mut state = State::new();
        state
            .entry(Network::Regtest)
            .or_default()
            .insert(height, hex.clone());
        let file = tempfile();
        fs::write(&file, render_file(&state))?;
        assert_eq!(parse_existing(&file)[&Network::Regtest][&height], hex);
        let _ = fs::remove_file(&file);
        Ok(())
    }

    #[test]
    fn render_then_parse_round_trips() {
        let state = sample_state();
        let rendered = render_file(&state);
        let file = tempfile();
        fs::write(&file, &rendered).unwrap();
        assert_eq!(parse_existing(&file), state);
        let _ = fs::remove_file(&file);
    }

    /// Regenerating for one network must not disturb another's entries, or a run for testnet
    /// would wipe out mainnet's trusted header.
    #[test]
    fn updating_one_network_preserves_the_others() {
        let mut state = sample_state();
        let rendered = render_file(&state);
        let file = tempfile();
        fs::write(&file, &rendered).unwrap();

        let mut reloaded = parse_existing(&file);
        reloaded
            .entry(Network::Testnet)
            .or_default()
            .insert(4032, "dd".repeat(80));
        fs::write(&file, render_file(&reloaded)).unwrap();

        state
            .entry(Network::Testnet)
            .or_default()
            .insert(4032, "dd".repeat(80));
        assert_eq!(parse_existing(&file), state);
        let _ = fs::remove_file(&file);
    }

    /// A fresh height for a network already in the file adds to it; the same height replaces.
    #[test]
    fn same_height_replaces_new_height_adds() {
        let state = sample_state();
        let file = tempfile();
        fs::write(&file, render_file(&state)).unwrap();

        let mut reloaded = parse_existing(&file);
        reloaded
            .get_mut(&Network::Bitcoin)
            .unwrap()
            .insert(902016, "ee".repeat(80)); // replace
        reloaded
            .get_mut(&Network::Bitcoin)
            .unwrap()
            .insert(904032, "ff".repeat(80)); // add
        fs::write(&file, render_file(&reloaded)).unwrap();

        let mainnet = &parse_existing(&file)[&Network::Bitcoin];
        assert_eq!(mainnet.get(&902016), Some(&"ee".repeat(80)));
        assert_eq!(mainnet.get(&904032), Some(&"ff".repeat(80)));
        assert_eq!(mainnet.len(), 2);

        let _ = fs::remove_file(&file);
    }

    fn tempfile() -> PathBuf {
        std::env::temp_dir().join(format!(
            "trusted_headers_gen_test_{:?}_{}",
            std::thread::current().id(),
            std::process::id(),
        ))
    }
}
