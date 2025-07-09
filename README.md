
# Snowballin Grid Bot

A high-frequency DCA grid trading bot for Kraken, with Coingecko-based indicators, built for continuous deployment on a lightweight VPS (e.g., t2.medium). It auto-manages capital using volatility, trend, and RSI filters with full restart and recovery support.

---
## 🚀 About Snowballbot

Snowballbot began as a personal challenge: could I use AI to teach myself technical analysis — and then turn that knowledge into something autonomous? Six months later, this project became a reality: a fully AI-generated, market-adaptive trading strategist.

It’s designed for security and survivability (well, in terms of money anyway - I'm security minded but who knows what bugs are in this).

Kraken was chosen specifically because of its withdrawal key model — funds can't move unless your account is compromised and the destination is pre-approved.

I honestly didn’t expect this to work. But it is — at least for me. If it helps you too, that’s a win. This isn’t financial advice. It’s a tool. Use it at your own risk. There’s a donation wallet if Snowballbot earns its keep.

### 🧠 For the curious hacker:

This README holds just enough to get your hands dirty. It’s not about turnkey solutions — it’s about sparking ideas. AI isn’t just some passive chatbot. It’s leverage. It’s force multiplication. Use it to learn faster, automate deeper, and explore smarter.

---

**Want to join me on Kraken?**  
Use my referral and we’ll both get \$50 when you trade $200:  
🔗 [Kraken Referral Link](https://kraken.onelink.me/JDNW/zf45lmjz)  
🧾 Code: `s4frjfbr`



---
## 🚀 Features

- 📈 **ATR-Scaled DCA Grid:** Dynamically sizes laddered buy orders based on volatility.
- 🔄 **Adaptive RSI Gating:** Filters entries using RSI with volume-aware thresholds.
- 🧮 **VWAP & Order Book Skew Filters:** Biases entries toward stronger market structure.
- 🎯 **Live TP & ATR Crash Stops:** Actively manages trailing profit exits and market dumps.
- ♻️ **Full Position Recovery:** Rebuilds internal state from Kraken trade history on reboot.
- 💰 **Profit Tracking & Auto-Withdrawals:** Tracks realized gains and triggers withdrawals by asset.
- ⚙️ **Per-Asset Config:** Grid levels and TTL ranges are independently configurable.
- 🔔 **Discord Notifications:** Optional webhook alerts for buys, sells, fills, and errors.
- ⏱️ **Rate-Limit Aware Price Fetching:** Batched Coingecko calls to prevent API throttling.
- 💾 **Restart-Safe:** All core data is persisted in a disk-backed `state.json`.


---
## 🧱 Prerequisites

- **VPS:** t2.medium (2 vCPU / 4GB RAM) or equivalent
- **OS:** Ubuntu 20.04+ or Debian-based distro
- **Go:** 1.20+
- **Git**
- **[Kraken API Key/Secret](https://support.kraken.com/hc/en-us/articles/360000919966-How-to-create-an-API-key)** [with trade & withdraw perms
- **Build Tools**: A C compiler toolchain (like GCC/build-essential) is required for the SQLite database driver.

---

## 🔧 Installation

```bash
# Install Go and Git
# Install Go, Git, and the necessary C compiler tools
sudo apt update && sudo apt install -y golang-go git build-essential

# Clone the repository
git clone https://github.com/Ohmjones/Snowballin.git /home/ubuntu/snowballin
cd /home/ubuntu/snowballin

# Build the binary with CGO enabled (critical for the database)
CGO_ENABLED=1 go build -o snowballin main.go
```

---

## ⚙️ Configuration

Create `config.json`:

```json
{
  "kraken_api_key": "YOUR_KRAKEN_API_KEY",
  "kraken_api_secret": "YOUR_KRAKEN_API_SECRET",
  "withdraw_keys": {
    "XBT": "YOUR_KRAKEN_WITHDRAW_KEY",
    "ETH": "YOUR_KRAKEN_WITHDRAW_KEY",
    "SOL": "YOUR_KRAKEN_WITHDRAW_KEY"
  },
  "assets":       ["XBT","ETH"],
  "grid_levels":  3,
  "stop_loss_pct":0.15,
  "withdraw_usd": 500.0,
  "withdraw_reserve": 0.0,
  "rsi_gate":     50.0,
  "rsi_slack":    10.0,
  "base_ma_lookback": 50,
  "atr_lookback": 14,
  "discord_webhook": "YOUR_DISCORD_WEBHOOK",
  "state": {
    "file": "state.json"
  },
  "orders": {
    "enable_watcher": true,
    "min_order_age_minutes": 5,
    "max_order_age_minutes": 45
  },
  "dynamic_tp": {
    "parabolic_strength_threshold": 0.6,
    "parabolic_tp_multiplier": 1.2,
    "parabolic_trail_ratio": 1.5
  },
  "dynamic_allocation": {
    "rs_enabled": true,
    "parabolic_enabled": true,
    "rs_allocation_weight": 0.5,
    "parabolic_allocation_weight": 0.5,
    "allocation_multiplier": 2.0,
    "benchmark_asset": "XBT"
  },
  "trend_intervals": {
    "short": 15,
    "mid":   60,
    "long":  240
  },
  "indicators": {
    "bb_period": 20
  },
  "consensus": {
    "threshold_buy":  0.6,
    "threshold_sell": 0.4
  },
  "dipbuy": {
    "short_ema_lookback": 8,
    "rsi_lookback": 14,
    "weight": 0.6,
    "aggressive_rsi_threshold": 25
  },
  "marketdata": {
    "cache_interval_sec": 30
  },
  "maker_fee":      0.0010,
  "taker_fee":      0.0020,
  "step_multiplier":2.5,
  "tp_multiplier":  2.0,
  "trail_ratio":    1.0
}
```

---

## 🧪 Running the Bot
### 🔁 Daemonizing Snowballbot with `systemd` (Ubuntu/Debian)

```bash
# Create the systemd service file.
# IMPORTANT: Replace /home/ubuntu/snowballin with the actual absolute path to your project.
sudo tee /etc/systemd/system/snowballin.service > /dev/null <<EOF
[Unit]
Description=Snowballin Trading Bot
After=network-online.target

[Service]
Type=simple
User=ubuntu
Group=ubuntu
WorkingDirectory=/home/ubuntu/snowballin
ExecStart=/home/ubuntu/snowballin/snowballin
Restart=on-failure
RestartSec=5s

[Install]
WantedBy=multi-user.target
EOF

# Reload systemd, enable the service to start on boot, and start it now
sudo systemctl daemon-reload
sudo systemctl enable snowballin.service
sudo systemctl start snowballin.service

# To check the status and see the latest logs:
sudo systemctl status snowballin.service
journalctl -u snowballin.service -f --no-pager`
```
## 🔁 Restart & Recovery

- Rebuilds open positions by scanning Kraken trade history (last 24h)
- Filters out test trades, dust, and stale fills
- Falls back to midpoint price if history is unavailable
- Persists key state data: `AvgEntry`, `PositionVol`, `PriceHistory`, and `ProfitCumulative`

---

## 🧠 Tips

- ✅ Set a **custom nonce window** in your Kraken API key settings to avoid `EAPI:Invalid nonce` errors
- 📅 Trade reconstruction defaults to the **last 24h** — ensure your actual position aligns with that window
- 🎯 Snowballbot auto-adjusts for volatility, but you can override **RSI gates**, **slack**, and **grid spacing** per asset if needed
  - This line is accurate as long as users understand that configuration is optional, not mandatory.
- 🕒 **Batch price fetches** from Coingecko every **30–60 seconds** to stay within API rate limits

---

## 🧾 Example Output

```text
[MAIN] loaded config: assets=[XBT ETH SOL]  gridLevels=3
[RECOVER] ETH: reconstructed position 0.20568 @ avg entry 1806.72 from trade history
[BUY] XBT: grid leg 2 filled — 0.00147 @ 61,480.00
[TP] SOL: trailing take-profit triggered — sold 0.26401 @ 149.20 (+$35.11 realized)
[INFO] Updating price cache… (3 assets via Coingecko)
[CHECK] ETH: RSI=38.7 volF=1.6 slope=-0.87 — HOLD (no signal)
```

---

### ✍️ Author

Built by **[Shane Jones](https://x.com/OhmSecurities)** — a security consultant and market automation enthusiast.
This project was designed to snowball crypto profits through smart grid logic and lightweight VPS deployment.
Development assistance provided by AI (ChatGPT/Gemini) — all logic, flow, and architecture hand-validated.

---


---

## 💸 Donations

If Snowballbot helped you level up your trading or gave you a spark of inspiration, feel free to toss a tip my way — no pressure, and definitely not expected.

**BTC Address:** `bc1qnu8pxqp0mf8794fyn6cnxcxllceaywysde3uu2`

Just knowing it's helping others is already a win. Thanks for being here.

---

## 🛡 License

Provided for educational and research purposes only. No warranty, no guarantee of profitability, and absolutely no liability — use at your own risk.

You're free to study, modify, and run this code for personal use. If you break something, lose funds, or blow up your Kraken account, that’s on you — not me.
