<p align="center">
  <img src="ding.png" width="200" alt="ding logo">
</p>

<p align="center">
  small rust monitor for famous fox <code>ding!</code> transactions on solana.
</p>

<p align="center">
  it listens to helius enhanced or standard solana websockets, decodes mpl core <code>createv2</code> instruction data, fetches the token metadata uri for the image, resolves primary sns names when present, and sends a compact discord embed.
</p>

<p align="center">
  <a href="https://railway.com/new/github?repo=https%3A%2F%2Fgithub.com%2Fneko%2Fding"><img src="https://img.shields.io/badge/deploy%20on-railway-5016A1?logo=railway&logoColor=white" alt="Deploy on Railway"></a>
</p>

## setup

copy `.env.example` to `.env` and fill in the values.

```env
HELIUS_RPC_URL=https://mainnet.helius-rpc.com/?api-key=<API_KEY>
HELIUS_WS_URL=wss://mainnet.helius-rpc.com/?api-key=<API_KEY>
DING_WS_MODE=enhanced
DING_FEE_ACCOUNT=98Ni7vVRR1tggtWWruPVcfFXHTH11bPbNryJZGkCGvaD
DISCORD_WEBHOOK_URL=https://discord.com/api/webhooks/<ID>/<TOKEN>
```

## railway [![Deploy on Railway](https://img.shields.io/badge/deploy%20on-railway-5016A1?logo=railway&logoColor=white)](https://railway.com/new/github?repo=https%3A%2F%2Fgithub.com%2Fneko%2Fding)

before the first deploy, add the required service variables from `.env.example`:

```env
HELIUS_RPC_URL=https://mainnet.helius-rpc.com/?api-key=<API_KEY>
HELIUS_WS_URL=wss://mainnet.helius-rpc.com/?api-key=<API_KEY>
DING_WS_MODE=enhanced
DING_FEE_ACCOUNT=98Ni7vVRR1tggtWWruPVcfFXHTH11bPbNryJZGkCGvaD
DISCORD_WEBHOOK_URL=https://discord.com/api/webhooks/<ID>/<TOKEN>
```

railway uses `railway.json` to build with railpack and keep the monitor running as a long-lived worker.

## run

```sh
cargo run --release
```

send a test webhook from the default example tx:

```sh
cargo run -- -t
```

send a test webhook from a specific tx:

```sh
cargo run -- -t <signature>
```

## env

`HELIUS_RPC_URL` is used for test tx fetches and sns account lookups.

`HELIUS_WS_URL` is used for websocket subscriptions.

`SOLANA_RPC_URL` and `SOLANA_WS_URL` may be used instead of `HELIUS_RPC_URL` and `HELIUS_WS_URL`.

`DING_WS_MODE` controls the websocket mode. `enhanced` uses helius `transactionSubscribe` with required mpl core and fee accounts. `standard` uses normal `logsSubscribe` on the fee account, then fetches matching transactions by signature.

`DING_FEE_ACCOUNT` defaults to the famous fox fee account used to prefilter ding candidates.

`DISCORD_WEBHOOK_URL` is where ding embeds are sent.

`DING_TEST_SIGNATURE` optionally overrides the default test tx.

standard websocket mode example:

```env
HELIUS_RPC_URL=https://mainnet.helius-rpc.com/?api-key=<API_KEY>
HELIUS_WS_URL=wss://mainnet.helius-rpc.com/?api-key=<API_KEY>
DING_WS_MODE=standard
DING_FEE_ACCOUNT=98Ni7vVRR1tggtWWruPVcfFXHTH11bPbNryJZGkCGvaD
DISCORD_WEBHOOK_URL=https://discord.com/api/webhooks/<ID>/<TOKEN>
```
