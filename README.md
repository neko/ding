<p align="center">
  <img src="ding.png" width="200" alt="ding logo">
</p>

<p align="center">
  small rust monitor for famous fox <code>ding!</code> transactions on solana.
</p>

<p align="center">
  it listens to helius enhanced websockets, decodes mpl core <code>createv2</code> instruction data, fetches the token metadata uri for the image, resolves primary sns names when present, and sends a compact discord embed.
</p>

## setup

copy `.env.example` to `.env` and fill in the values.

```env
HELIUS_RPC_URL=https://mainnet.helius-rpc.com/?api-key=<API_KEY>
HELIUS_WS_URL=wss://beta.helius-rpc.com/?api-key=<API_KEY>
DISCORD_WEBHOOK_URL=https://discord.com/api/webhooks/<ID>/<TOKEN>
```

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

`HELIUS_WS_URL` is used for enhanced websocket transaction subscriptions.

`DISCORD_WEBHOOK_URL` is where ding embeds are sent.

`DING_TEST_SIGNATURE` optionally overrides the default test tx.
