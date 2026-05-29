// meow

use std::{
    collections::{HashMap, HashSet, VecDeque},
    env,
    sync::LazyLock,
    time::Duration,
};

use anyhow::{Context, Result, bail};
use base64::{Engine as _, engine::general_purpose};
use futures_util::{SinkExt, StreamExt};
use reqwest::Client;
use serde::Serialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use solana_pubkey::Pubkey;
use tokio::{signal, time};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, info, warn};
use tracing_subscriber::EnvFilter;

const MPL_CORE_PROGRAM_ID: &str = "CoREENxT6tW1HoK8ypY1SxRMZTcVPm7R94rH4PZNhX7d";
const FAMOUS_FOX_FEE_ACCOUNT: &str = "98Ni7vVRR1tggtWWruPVcfFXHTH11bPbNryJZGkCGvaD";
const DING_NAME: &str = "DING!";
const DING_DESCRIPTION_MARKER: &str = "famousfoxes.com";
const DISCORD_GREY: u32 = 0x2b2d31;
const HASH_PREFIX: &str = "SPL Name Service";
const SNS_HEADER_LEN: usize = 96;
const SOLSCAN_ACCOUNT: &str = "https://solscan.io/account/";
const DEFAULT_TEST_SIGNATURE: &str =
    "8UhRGNFLPzfSwsvVA5LFD7fpxdXxf9drCHn92GA4eJCfNXHDLM9ptgc8FfGi3EM1gQCB9fRzNrsECDh1ofrJRpA";

static NAME_PROGRAM_ID: LazyLock<Pubkey> = LazyLock::new(|| {
    "namesLPneVptA9Z5rqUDD9tMTWEJwofgaYwp8cawRkX"
        .parse()
        .unwrap()
});
static NAME_OFFERS_ID: LazyLock<Pubkey> = LazyLock::new(|| {
    "85iDfUvr3HJyLM2zcq5BXSiDvUWfw6cSE1FfNBo8Ap29"
        .parse()
        .unwrap()
});
static REVERSE_LOOKUP_CLASS: LazyLock<Pubkey> = LazyLock::new(|| {
    "33m47vH6Eav6jr5Ry86XjhRft2jRBLDnDgPSHoquXi2Z"
        .parse()
        .unwrap()
});
static ROOT_DOMAIN_ACCOUNT: LazyLock<Pubkey> = LazyLock::new(|| {
    "58PwtjSDuFHuUkYjH9BYnnQKHfwo9reZhC2zMJv9JPkx"
        .parse()
        .unwrap()
});

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let config = Config::from_env()?;
    let client = Client::builder()
        .timeout(Duration::from_secs(20))
        .user_agent("ding-monitor/0.1.0")
        .build()?;

    let mut seen = SeenSignatures::new(2_048);
    let mut sns = SnsResolver::new(client.clone(), config.rpc_url.clone());
    let mut reconnect_delay = Duration::from_secs(1);

    if config.test_webhook {
        send_test_webhook(&config, &client, &mut sns).await?;
    }

    loop {
        tokio::select! {
            result = run_ws(&config, &client, &mut sns, &mut seen) => {
                match result {
                    Ok(()) => info!("websocket closed"),
                    Err(err) => warn!("websocket disconnected: {err:#}"),
                }

                info!("reconnecting in {}s", reconnect_delay.as_secs());
                time::sleep(reconnect_delay).await;
                reconnect_delay = (reconnect_delay * 2).min(Duration::from_secs(60));
            }
            _ = signal::ctrl_c() => {
                info!("shutdown requested");
                return Ok(());
            }
        }
    }
}

async fn run_ws(
    config: &Config,
    client: &Client,
    sns: &mut SnsResolver,
    seen: &mut SeenSignatures,
) -> Result<()> {
    let safe_ws_url = redact_url(&config.ws_url);
    info!(
        mode = config.ws_mode.as_str(),
        url = %safe_ws_url,
        "connecting websocket"
    );

    let (ws, _) = connect_async(config.ws_url.as_str())
        .await
        .with_context(|| {
            format!(
                "websocket connect failed: mode={} url={}",
                config.ws_mode.as_str(),
                safe_ws_url
            )
        })?;
    let (mut write, mut read) = ws.split();
    info!(
        mode = config.ws_mode.as_str(),
        url = %safe_ws_url,
        "websocket connected"
    );

    let subscription = subscription_request(config);
    let method = subscription
        .get("method")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    info!(
        mode = config.ws_mode.as_str(),
        method = method,
        fee_account = %config.fee_account,
        "sending websocket subscription"
    );
    write
        .send(Message::Text(subscription.to_string().into()))
        .await
        .with_context(|| {
            format!(
                "websocket subscription send failed: mode={} url={}",
                config.ws_mode.as_str(),
                safe_ws_url
            )
        })?;
    info!(
        mode = config.ws_mode.as_str(),
        fee_account = %config.fee_account,
        "subscribed to mpl core ding candidates"
    );

    let mut ping = time::interval(Duration::from_secs(60));
    let mut stats = time::interval(Duration::from_secs(5));
    let mut txs_seen = 0_u64;

    loop {
        tokio::select! {
            _ = ping.tick() => write.send(Message::Ping(Vec::new().into())).await.with_context(|| {
                format!(
                    "websocket ping failed: mode={} url={}",
                    config.ws_mode.as_str(),
                    safe_ws_url
                )
            })?,
            _ = stats.tick() => {
                info!("seen {txs_seen} txs");
                txs_seen = 0;
            }
            message = read.next() => {
                match message.transpose().with_context(|| {
                    format!(
                        "websocket read failed: mode={} url={}",
                        config.ws_mode.as_str(),
                        safe_ws_url
                    )
                })? {
                    Some(Message::Text(text)) => {
                        if handle_ws_text(text.as_ref(), config, client, sns, seen).await? {
                            txs_seen += 1;
                        }
                    }
                    Some(Message::Ping(payload)) => write.send(Message::Pong(payload)).await.with_context(|| {
                        format!(
                            "websocket pong failed: mode={} url={}",
                            config.ws_mode.as_str(),
                            safe_ws_url
                        )
                    })?,
                    Some(Message::Close(frame)) => bail!("websocket closed: {frame:?}"),
                    Some(_) => {}
                    None => bail!("websocket stream ended"),
                }
            }
        }
    }
}

fn subscription_request(config: &Config) -> Value {
    match config.ws_mode {
        WsMode::Enhanced => json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "transactionSubscribe",
            "params": [
                {
                    "failed": false,
                    "accountRequired": [MPL_CORE_PROGRAM_ID, config.fee_account.as_str()]
                },
                {
                    "commitment": "confirmed",
                    "encoding": "jsonParsed",
                    "transactionDetails": "full",
                    "showRewards": false,
                    "maxSupportedTransactionVersion": 0
                }
            ]
        }),
        WsMode::Standard => json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "logsSubscribe",
            "params": [
                { "mentions": [config.fee_account.as_str()] },
                { "commitment": "confirmed" }
            ]
        }),
    }
}

async fn handle_ws_text(
    text: &str,
    config: &Config,
    client: &Client,
    sns: &mut SnsResolver,
    seen: &mut SeenSignatures,
) -> Result<bool> {
    let payload: Value = serde_json::from_str(text).context("invalid websocket JSON")?;

    if payload.get("id").and_then(Value::as_i64) == Some(1) {
        if let Some(subscription_id) = payload.get("result") {
            info!(
                mode = config.ws_mode.as_str(),
                "websocket subscription id: {subscription_id}"
            );
        }
        return Ok(false);
    }

    match config.ws_mode {
        WsMode::Enhanced => handle_enhanced_ws_payload(&payload, config, client, sns, seen).await,
        WsMode::Standard => handle_standard_ws_payload(&payload, config, client, sns, seen).await,
    }
}

async fn handle_enhanced_ws_payload(
    payload: &Value,
    config: &Config,
    client: &Client,
    sns: &mut SnsResolver,
    seen: &mut SeenSignatures,
) -> Result<bool> {
    if payload.get("method").and_then(Value::as_str) != Some("transactionNotification") {
        return Ok(false);
    }

    let Some(result) = payload.pointer("/params/result") else {
        return Ok(false);
    };
    let Some(signature) = result.get("signature").and_then(Value::as_str) else {
        return Ok(true);
    };

    if !seen.insert(signature) {
        return Ok(true);
    }

    process_transaction_result(result, signature, config, client, sns).await?;
    Ok(true)
}

async fn handle_standard_ws_payload(
    payload: &Value,
    config: &Config,
    client: &Client,
    sns: &mut SnsResolver,
    seen: &mut SeenSignatures,
) -> Result<bool> {
    if payload.get("method").and_then(Value::as_str) != Some("logsNotification") {
        return Ok(false);
    }

    let Some(value) = payload.pointer("/params/result/value") else {
        return Ok(false);
    };
    if value.get("err").is_some_and(|err| !err.is_null()) {
        return Ok(true);
    }

    let Some(signature) = value.get("signature").and_then(Value::as_str) else {
        return Ok(true);
    };
    if !logs_contain_ding_candidate(value.get("logs")) {
        return Ok(true);
    }
    if !seen.insert(signature) {
        return Ok(true);
    }

    info!(
        signature,
        rpc_url = %redact_url(&config.rpc_url),
        "standard websocket candidate matched; fetching transaction"
    );
    let transaction = fetch_transaction(client, &config.rpc_url, signature)
        .await
        .with_context(|| format!("standard websocket transaction fetch failed: {signature}"))?;
    process_transaction_result(&transaction, signature, config, client, sns).await?;
    Ok(true)
}

async fn process_transaction_result(
    result: &Value,
    signature: &str,
    config: &Config,
    client: &Client,
    sns: &mut SnsResolver,
) -> Result<()> {
    let Some(candidate) = extract_asset_candidate(result) else {
        debug!(
            signature,
            "createv2 log without expected mpl core instruction shape"
        );
        return Ok(());
    };

    info!(signature, asset = %candidate.asset, "processing ding candidate");

    match build_ding_event(client, sns, &candidate, signature).await? {
        Some(ding) => send_ding_webhook(client, &config.discord_webhook_url, &ding).await?,
        None => debug!(signature, asset = candidate.asset, "not a famous fox ding"),
    }

    Ok(())
}

async fn send_test_webhook(config: &Config, client: &Client, sns: &mut SnsResolver) -> Result<()> {
    let signature = config
        .test_signature
        .as_deref()
        .unwrap_or(DEFAULT_TEST_SIGNATURE);
    let transaction = fetch_transaction(client, &config.rpc_url, signature).await?;
    let candidate = extract_asset_candidate(&transaction).with_context(|| {
        format!("test transaction has no MPL Core asset candidate: {signature}")
    })?;
    let ding = build_ding_event(client, sns, &candidate, signature)
        .await?
        .with_context(|| format!("test transaction is not a Famous Fox Ding: {signature}"))?;

    send_ding_webhook(client, &config.discord_webhook_url, &ding).await?;
    info!(signature, "sent test ding webhook");
    Ok(())
}

async fn fetch_transaction(client: &Client, rpc_url: &str, signature: &str) -> Result<Value> {
    let safe_rpc_url = redact_url(rpc_url);
    let response = client
        .post(rpc_url)
        .json(&json!({
            "jsonrpc": "2.0",
            "id": "getTransaction",
            "method": "getTransaction",
            "params": [
                signature,
                { "encoding": "jsonParsed", "maxSupportedTransactionVersion": 0 }
            ]
        }))
        .send()
        .await
        .map_err(|err| {
            anyhow::anyhow!(
                "getTransaction request failed: url={} signature={} error={}",
                safe_rpc_url,
                signature,
                err.without_url()
            )
        })?;

    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        bail!(
            "getTransaction HTTP {status}: url={} signature={} body={}",
            safe_rpc_url,
            signature,
            compact_body(&body)
        );
    }

    let response: Value = response
        .json()
        .await
        .with_context(|| format!("getTransaction JSON parse failed: url={safe_rpc_url}"))?;

    if let Some(error) = response.get("error") {
        bail!(
            "getTransaction RPC error: url={} signature={} error={error}",
            safe_rpc_url,
            signature
        );
    }

    response
        .get("result")
        .filter(|result| !result.is_null())
        .cloned()
        .with_context(|| format!("transaction not found: {signature}"))
}

async fn build_ding_event(
    client: &Client,
    sns: &mut SnsResolver,
    candidate: &AssetCandidate,
    signature: &str,
) -> Result<Option<DingEvent>> {
    let Some(mut ding) = fetch_ding(client, candidate).await? else {
        return Ok(None);
    };

    ding.signature = signature.to_owned();
    ding.sender_sns = sns.resolve(&ding.sender).await.unwrap_or_else(|err| {
        debug!(address = ding.sender, "sns lookup failed: {err:#}");
        None
    });
    ding.receiver_sns = sns.resolve(&ding.receiver).await.unwrap_or_else(|err| {
        debug!(address = ding.receiver, "sns lookup failed: {err:#}");
        None
    });

    Ok(Some(ding))
}

async fn send_ding_webhook(client: &Client, webhook_url: &str, ding: &DingEvent) -> Result<()> {
    send_discord(client, webhook_url, ding).await?;
    info!(
        signature = %ding.signature,
        asset = %ding.asset,
        sender = ding.sender,
        receiver = ding.receiver,
        "sent ding embed"
    );
    Ok(())
}

fn extract_asset_candidate(result: &Value) -> Option<AssetCandidate> {
    let instructions = result
        .pointer("/transaction/transaction/message/instructions")
        .or_else(|| result.pointer("/transaction/message/instructions"))?
        .as_array()?;

    instructions.iter().find_map(|ix| {
        if ix.get("programId")?.as_str()? != MPL_CORE_PROGRAM_ID {
            return None;
        }
        let create_v2 = decode_create_v2_data(ix.get("data")?.as_str()?)?;
        if create_v2.name != DING_NAME {
            return None;
        }

        let accounts = ix.get("accounts")?.as_array()?;
        Some(AssetCandidate {
            asset: account_at(accounts, 0)?,
            fallback_sender: account_at(accounts, 3)?,
            fallback_receiver: account_at(accounts, 4)?,
            uri: create_v2.uri,
        })
    })
}

fn logs_contain_ding_candidate(logs: Option<&Value>) -> bool {
    let Some(logs) = logs.and_then(Value::as_array) else {
        return false;
    };

    let mut has_core = false;
    let mut has_create_v2 = false;
    for log in logs.iter().filter_map(Value::as_str) {
        has_core |= log.contains(MPL_CORE_PROGRAM_ID);
        has_create_v2 |= log.contains("Instruction: CreateV2");
    }

    has_core && has_create_v2
}

fn decode_create_v2_data(data: &str) -> Option<CreateV2Data> {
    let bytes = bs58::decode(data).into_vec().ok()?;
    let mut cursor = Cursor::new(&bytes);

    if cursor.read_u8()? != 20 {
        return None;
    }

    cursor.read_u8()?;
    let name = cursor.read_string()?;
    let uri = cursor.read_string()?;

    Some(CreateV2Data { name, uri })
}

struct Cursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn read_u8(&mut self) -> Option<u8> {
        let value = *self.bytes.get(self.offset)?;
        self.offset += 1;
        Some(value)
    }

    fn read_string(&mut self) -> Option<String> {
        let len = u32::from_le_bytes(
            self.bytes
                .get(self.offset..self.offset + 4)?
                .try_into()
                .ok()?,
        ) as usize;
        self.offset += 4;
        let value =
            String::from_utf8(self.bytes.get(self.offset..self.offset + len)?.to_vec()).ok()?;
        self.offset += len;
        Some(value)
    }
}

fn account_at(accounts: &[Value], index: usize) -> Option<String> {
    accounts.get(index)?.as_str().map(ToOwned::to_owned)
}

async fn fetch_ding(client: &Client, candidate: &AssetCandidate) -> Result<Option<DingEvent>> {
    let metadata: Value = client
        .get(&candidate.uri)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let description = metadata
        .get("description")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if !description.contains(DING_DESCRIPTION_MARKER) {
        return Ok(None);
    }

    let image = metadata
        .get("image")
        .and_then(Value::as_str)
        .or_else(|| {
            metadata
                .pointer("/properties/files/0/uri")
                .and_then(Value::as_str)
        })
        .context("Ding metadata missing image")?
        .to_owned();
    let sender = metadata
        .get("attributes")
        .and_then(Value::as_array)
        .and_then(|attrs| {
            attrs.iter().find_map(|attr| {
                (attr.get("trait_type").and_then(Value::as_str) == Some("From"))
                    .then(|| attr.get("value").and_then(Value::as_str))
                    .flatten()
            })
        })
        .unwrap_or(&candidate.fallback_sender)
        .to_owned();

    Ok(Some(DingEvent {
        signature: String::new(),
        asset: candidate.asset.clone(),
        sender,
        receiver: candidate.fallback_receiver.clone(),
        image,
        sender_sns: None,
        receiver_sns: None,
    }))
}

async fn send_discord(client: &Client, webhook_url: &str, ding: &DingEvent) -> Result<()> {
    let description = format!(
        "{} sent a ding to {}",
        account_link(&ding.sender, ding.sender_sns.as_deref()),
        account_link(&ding.receiver, ding.receiver_sns.as_deref())
    );
    let body = DiscordWebhook {
        embeds: vec![DiscordEmbed {
            color: DISCORD_GREY,
            description,
            image: DiscordImage { url: &ding.image },
        }],
    };

    client
        .post(webhook_url)
        .json(&body)
        .send()
        .await?
        .error_for_status()?;
    Ok(())
}

fn account_link(address: &str, sns: Option<&str>) -> String {
    format!(
        "[{}]({SOLSCAN_ACCOUNT}{address})",
        sns.unwrap_or(&abbreviate(address))
    )
}

fn abbreviate(address: &str) -> String {
    if address.len() <= 10 {
        return address.to_owned();
    }
    format!("{}...{}", &address[..4], &address[address.len() - 4..])
}

struct Config {
    rpc_url: String,
    ws_url: String,
    ws_mode: WsMode,
    fee_account: String,
    discord_webhook_url: String,
    test_webhook: bool,
    test_signature: Option<String>,
}

impl Config {
    fn from_env() -> Result<Self> {
        let mut test_webhook = false;
        let mut test_signature = env::var("DING_TEST_SIGNATURE")
            .ok()
            .filter(|value| !value.is_empty());
        let mut args = env::args().skip(1);

        while let Some(arg) = args.next() {
            if arg == "-t" || arg == "--test" {
                test_webhook = true;
                if let Some(next) = args.next().filter(|value| !value.starts_with('-')) {
                    test_signature = Some(next);
                }
            }
        }

        Ok(Self {
            rpc_url: env_first("SOLANA_RPC_URL", "HELIUS_RPC_URL")?,
            ws_url: env_first("SOLANA_WS_URL", "HELIUS_WS_URL")?,
            ws_mode: optional_env("DING_WS_MODE")
                .as_deref()
                .map(WsMode::parse)
                .transpose()?
                .unwrap_or(WsMode::Enhanced),
            fee_account: optional_env("DING_FEE_ACCOUNT")
                .unwrap_or_else(|| FAMOUS_FOX_FEE_ACCOUNT.to_owned()),
            discord_webhook_url: required_env("DISCORD_WEBHOOK_URL")?,
            test_webhook,
            test_signature,
        })
    }
}

#[derive(Clone, Copy)]
enum WsMode {
    Enhanced,
    Standard,
}

impl WsMode {
    fn parse(value: &str) -> Result<Self> {
        match value.to_ascii_lowercase().as_str() {
            "enhanced" | "helius" => Ok(Self::Enhanced),
            "standard" | "normal" | "solana" => Ok(Self::Standard),
            _ => bail!("DING_WS_MODE must be enhanced or standard"),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Enhanced => "enhanced",
            Self::Standard => "standard",
        }
    }
}

fn optional_env(key: &str) -> Option<String> {
    env::var(key).ok().filter(|value| !value.is_empty())
}

fn env_first(primary: &str, fallback: &str) -> Result<String> {
    optional_env(primary)
        .or_else(|| optional_env(fallback))
        .with_context(|| format!("missing {primary} or {fallback}"))
}

fn required_env(key: &str) -> Result<String> {
    env::var(key).with_context(|| format!("missing {key}"))
}

fn redact_url(url: &str) -> String {
    match url.split_once('?') {
        Some((base, _)) => format!("{base}?<redacted>"),
        None => url.to_owned(),
    }
}

fn compact_body(body: &str) -> String {
    let body = body.trim();
    let mut chars = body.chars();
    let compact: String = chars.by_ref().take(240).collect();
    if chars.next().is_some() {
        format!("{compact}...")
    } else {
        body.to_owned()
    }
}

#[derive(Debug)]
struct AssetCandidate {
    asset: String,
    fallback_sender: String,
    fallback_receiver: String,
    uri: String,
}

struct CreateV2Data {
    name: String,
    uri: String,
}

struct DingEvent {
    signature: String,
    asset: String,
    sender: String,
    receiver: String,
    image: String,
    sender_sns: Option<String>,
    receiver_sns: Option<String>,
}

#[derive(Serialize)]
struct DiscordWebhook<'a> {
    embeds: Vec<DiscordEmbed<'a>>,
}

#[derive(Serialize)]
struct DiscordEmbed<'a> {
    color: u32,
    description: String,
    image: DiscordImage<'a>,
}

#[derive(Serialize)]
struct DiscordImage<'a> {
    url: &'a str,
}

struct SeenSignatures {
    max: usize,
    set: HashSet<String>,
    order: VecDeque<String>,
}

impl SeenSignatures {
    fn new(max: usize) -> Self {
        Self {
            max,
            set: HashSet::new(),
            order: VecDeque::new(),
        }
    }

    fn insert(&mut self, signature: &str) -> bool {
        if self.set.contains(signature) {
            return false;
        }

        let signature = signature.to_owned();
        self.set.insert(signature.clone());
        self.order.push_back(signature);

        while self.order.len() > self.max {
            if let Some(oldest) = self.order.pop_front() {
                self.set.remove(&oldest);
            }
        }

        true
    }
}

struct SnsResolver {
    client: Client,
    rpc_url: String,
    cache: HashMap<String, Option<String>>,
}

impl SnsResolver {
    fn new(client: Client, rpc_url: String) -> Self {
        Self {
            client,
            rpc_url,
            cache: HashMap::new(),
        }
    }

    async fn resolve(&mut self, address: &str) -> Result<Option<String>> {
        if let Some(cached) = self.cache.get(address) {
            return Ok(cached.clone());
        }

        let resolved = self.resolve_uncached(address).await?;
        self.cache.insert(address.to_owned(), resolved.clone());
        Ok(resolved)
    }

    async fn resolve_uncached(&self, address: &str) -> Result<Option<String>> {
        let owner: Pubkey = address.parse().context("invalid pubkey for SNS lookup")?;
        let favorite_key = favorite_domain_key(&owner);
        let Some(favorite_data) = self.get_account_data(&favorite_key).await? else {
            return Ok(None);
        };
        if favorite_data.len() < 33 {
            return Ok(None);
        }

        let name_account = pubkey_from_slice(&favorite_data[1..33])?;
        let Some(name_data) = self.get_account_data(&name_account).await? else {
            return Ok(None);
        };
        if name_data.len() < SNS_HEADER_LEN {
            return Ok(None);
        }

        let parent = pubkey_from_slice(&name_data[0..32])?;
        let mut domain = if parent == *ROOT_DOMAIN_ACCOUNT {
            self.reverse_lookup(&name_account, None).await?
        } else {
            let child = self.reverse_lookup(&name_account, Some(&parent)).await?;
            let parent_name = self.reverse_lookup(&parent, None).await?;
            format!("{child}.{parent_name}")
        };

        if domain.is_empty() {
            return Ok(None);
        }
        if !domain.ends_with(".sol") {
            domain.push_str(".sol");
        }

        Ok(Some(domain))
    }

    async fn reverse_lookup(&self, domain: &Pubkey, parent: Option<&Pubkey>) -> Result<String> {
        let reverse_key = reverse_key_from_domain_key(domain, parent);
        let data = self
            .get_account_data(&reverse_key)
            .await?
            .with_context(|| format!("missing SNS reverse account for {domain}"))?;
        if data.len() < SNS_HEADER_LEN + 4 {
            bail!("short SNS reverse account for {domain}");
        }
        deserialize_reverse(&data[SNS_HEADER_LEN..])
    }

    async fn get_account_data(&self, pubkey: &Pubkey) -> Result<Option<Vec<u8>>> {
        let response: Value = self
            .client
            .post(&self.rpc_url)
            .json(&json!({
                "jsonrpc": "2.0",
                "id": "getAccountInfo",
                "method": "getAccountInfo",
                "params": [pubkey.to_string(), { "encoding": "base64" }]
            }))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;

        if let Some(error) = response.get("error") {
            bail!("getAccountInfo RPC error: {error}");
        }

        let Some(value) = response.pointer("/result/value") else {
            return Ok(None);
        };
        if value.is_null() {
            return Ok(None);
        }

        let data = value
            .get("data")
            .and_then(Value::as_array)
            .and_then(|data| data.first())
            .and_then(Value::as_str)
            .context("getAccountInfo missing base64 data")?;

        Ok(Some(general_purpose::STANDARD.decode(data)?))
    }
}

fn favorite_domain_key(owner: &Pubkey) -> Pubkey {
    Pubkey::find_program_address(&[b"favourite_domain", &owner.to_bytes()], &NAME_OFFERS_ID).0
}

fn reverse_key_from_domain_key(domain: &Pubkey, parent: Option<&Pubkey>) -> Pubkey {
    let hash = hashed_name(&domain.to_string());
    name_account_key(&hash, Some(&REVERSE_LOOKUP_CLASS), parent)
}

fn name_account_key(
    hashed_name: &[u8; 32],
    name_class: Option<&Pubkey>,
    parent: Option<&Pubkey>,
) -> Pubkey {
    let empty = Pubkey::default();
    let class = name_class.unwrap_or(&empty).to_bytes();
    let parent = parent.unwrap_or(&empty).to_bytes();
    Pubkey::find_program_address(&[hashed_name, &class, &parent], &NAME_PROGRAM_ID).0
}

fn hashed_name(name: &str) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(HASH_PREFIX.as_bytes());
    hasher.update(name.as_bytes());
    hasher.finalize().into()
}

fn pubkey_from_slice(slice: &[u8]) -> Result<Pubkey> {
    let bytes: [u8; 32] = slice.try_into().context("invalid pubkey byte length")?;
    Ok(Pubkey::new_from_array(bytes))
}

fn deserialize_reverse(data: &[u8]) -> Result<String> {
    if data.len() < 4 {
        bail!("SNS reverse data is too short");
    }

    let len = u32::from_le_bytes(data[0..4].try_into()?) as usize;
    if data.len() < 4 + len {
        bail!("SNS reverse data length mismatch");
    }

    Ok(String::from_utf8(data[4..4 + len].to_vec())?
        .trim_start_matches('\0')
        .trim_end_matches('\0')
        .to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config(ws_mode: WsMode) -> Config {
        Config {
            rpc_url: "https://rpc.example".to_owned(),
            ws_url: "wss://ws.example".to_owned(),
            ws_mode,
            fee_account: FAMOUS_FOX_FEE_ACCOUNT.to_owned(),
            discord_webhook_url: "https://discord.example".to_owned(),
            test_webhook: false,
            test_signature: None,
        }
    }

    #[test]
    fn extracts_mpl_core_candidate() {
        let payload = json!({
            "transaction": {
                "transaction": {
                    "message": {
                        "instructions": [{
                            "programId": MPL_CORE_PROGRAM_ID,
                            "accounts": ["asset", "program", "program", "sender", "receiver"],
                            "data": "244ozbo9fMciRkLCDva17sK9ZUY6T1gWAFmQFFkmX8S4yWAENFNsU31qxCf1HKNme6cXvhoo36bLq5BZsiRECah1Wp575uzkETQND7bDWwRWQ69ydetiEngnCHWSL7"
                        }]
                    }
                }
            }
        });

        let candidate = extract_asset_candidate(&payload).unwrap();
        assert_eq!(candidate.asset, "asset");
        assert_eq!(candidate.fallback_sender, "sender");
        assert_eq!(candidate.fallback_receiver, "receiver");
        assert_eq!(
            candidate.uri,
            "https://ipfs.io/ipfs/QmVxTv3EcqQqDW7e4ZqwtXLHEvpCgDCm8Vi8Tc29rjsPCX"
        );
    }

    #[test]
    fn formats_account_links() {
        assert_eq!(
            account_link(
                "jewishBC8etWX2663FW5CEQErVnP28ftRsfhJEShvwn",
                Some("ding.sol")
            ),
            "[ding.sol](https://solscan.io/account/jewishBC8etWX2663FW5CEQErVnP28ftRsfhJEShvwn)"
        );
        assert_eq!(
            abbreviate("jewishBC8etWX2663FW5CEQErVnP28ftRsfhJEShvwn"),
            "jewi...hvwn"
        );
    }

    #[test]
    fn builds_enhanced_subscription_with_required_accounts() {
        let request = subscription_request(&test_config(WsMode::Enhanced));

        assert_eq!(
            request.get("method").and_then(Value::as_str),
            Some("transactionSubscribe")
        );
        assert_eq!(
            request.pointer("/params/0/accountRequired"),
            Some(&json!([MPL_CORE_PROGRAM_ID, FAMOUS_FOX_FEE_ACCOUNT]))
        );
    }

    #[test]
    fn builds_standard_subscription_with_fee_mention() {
        let request = subscription_request(&test_config(WsMode::Standard));

        assert_eq!(
            request.get("method").and_then(Value::as_str),
            Some("logsSubscribe")
        );
        assert_eq!(
            request.pointer("/params/0/mentions"),
            Some(&json!([FAMOUS_FOX_FEE_ACCOUNT]))
        );
    }

    #[test]
    fn filters_standard_logs_to_core_create_v2() {
        assert!(logs_contain_ding_candidate(Some(&json!([
            format!("Program {MPL_CORE_PROGRAM_ID} invoke [1]"),
            "Program log: Instruction: CreateV2"
        ]))));
        assert!(!logs_contain_ding_candidate(Some(&json!([
            "Program 11111111111111111111111111111111 invoke [1]",
            "Program log: Instruction: Transfer"
        ]))));
    }

    #[test]
    fn redacts_url_query_strings() {
        assert_eq!(
            redact_url("wss://mainnet.helius-rpc.com/?api-key=secret"),
            "wss://mainnet.helius-rpc.com/?<redacted>"
        );
        assert_eq!(
            redact_url("https://api.example/path"),
            "https://api.example/path"
        );
    }
}
