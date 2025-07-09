# Snowballin Grid Bot

A high-frequency DCA grid trading bot for Kraken, with indicators firstly leveraging the Kraken broker data, then optionally, CoinGecko and CoinMarketCap, built for continuous deployment on a lightweight VPS (e.g., t2.medium). It auto-manages capital using volatility, trend, and RSI filters with full restart and recovery support.

---
## 🚀 About Snowballbot

Snowballbot began as a personal challenge. Could I use AI to teach myself technical analysis — and then turn that knowledge into something autonomous? Six months later, this project became a reality: a fully AI-generated, market-adaptive trading strategist. That version of this was a large single golang file around 1,000 lines of code.  This version is entirely refactored. 

It’s designed for security and survivability (well, in terms of money anyway - I'm security minded but who knows what bugs are in this).

Kraken was chosen specifically because of its pro trading bot policies and withdrawal key model — funds can't move unless your account is compromised and the destination is pre-approved.

I honestly didn’t expect this to work. But it is — at least for me. If it helps you too, that’s a win. This isn’t financial advice. It’s a tool. Use it at your own risk.

### 🧠 For the curious hacker:

This README holds just enough to get your hands dirty. It’s not about turnkey solutions — it’s about sparking ideas. AI isn’t just some passive chatbot. It’s leverage. It’s a force multiplier. Use it to learn faster, automate deeper, and explore smarter.  

If you absolutely need help setting this up, I'm happy to help for a recommended donation amount.

---

### **Want to join me on Kraken?** 
Use my referral and we’ll both get $50 when you trade \$200:  

🔗 [Kraken Referral Link](https://kraken.onelink.me/JDNW/zf45lmjz)  
🧾 Code: `s4frjfbr`

---
## ✨ How It Works
Snowballin' is not a simple grid bot that buys on fixed price drops. It operates on a multi-source consensus model to make intelligent, risk-managed decisions. On every cycle, it:

- Gathers Data: It pulls market data (price, OHLCV, order book) from your primary broker (Kraken) and optional secondary sources (CoinGecko, CoinMarketCap).
- Calculates Consensus: For each data source, it calculates a score based on a confluence of technical indicators (RSI, StochRSI, MACD), liquidity hunt patterns, and order book depth.
- Weighs & Decides: It combines these scores using your configured weights to produce a single, final consensus score.
- Executes with Risk Management: If the consensus score crosses your buy/sell threshold, it calculates a position size based on current market volatility (ATR) and portfolio risk, adjusts it for market sentiment (Fear & Greed Index), and then places the trade.
- Manages Autonomously: It monitors the order until filled, manages the open position with a trailing stop-loss, and looks for the next exit signal, all while persisting its state to a local database to survive restarts.

---
## 🚀 Key Features
- Multi-Source Consensus Engine: Blends weighted data from Kraken, CoinGecko, and CoinMarketCap to form high-conviction trading signals.

**Advanced Risk Management Suite:**

- Portfolio Circuit Breaker: Halts trading and exits positions if portfolio drawdown exceeds a set percentage.
- ATR-Adjusted Sizing: Dynamically sizes positions based on current market volatility.
- ATR-Adjusted Stop-Loss & Take-Profit: Sets dynamic, volatility-aware exit targets.

**Autonomous Trade Lifecycle:**

- Persistent State: Uses an SQLite database to save and load all pending orders and open positions, ensuring a full recovery after a restart.
- Active Position Management: Manages open trades with a trailing stop-loss to lock in profits.
- Stale Order Reaping: Automatically cancels limit orders that remain unfilled for too long, freeing up capital.

**Intelligent Signal Modulators:**

- Order Book Analysis: Reads live order book depth to gauge buy/sell pressure, influencing the consensus score.
- Sentiment-Adjusted Sizing: Uses the Fear & Greed Index to increase or decrease trade size based on market sentiment.

**Operational Robustness:**

- Pre-Flight Checks: Verifies all API connections and configurations on startup to prevent errors.
- Live Discord Notifications: Sends real-time alerts for filled orders, new positions, exits, and errors.


---
## 🧱 Prerequisites

- **VPS:** t2.medium (2 vCPU / 4GB RAM) or equivalent
- **OS:** Ubuntu 20.04+ or Debian-based distro
- **Go:** 1.20+
- **Git**
- **[Kraken API Key/Secret](https://support.kraken.com/hc/en-us/articles/360000919966-How-to-create-an-API-key)** with trade & withdraw permissions
- **Build Tools**: A C compiler toolchain (like GCC/build-essential) is required for the SQLite database driver.

---

## 🔧 Installation

```bash
# Install Go, Git, and the necessary C compiler tools
sudo apt update && sudo apt install -y golang-go git build-essential

# Clone the repository
git clone [https://github.com/Ohmjones/Snowballin.git](https://github.com/Ohmjones/Snowballin.git) /home/ubuntu/snowballin
cd /home/ubuntu/snowballin

# Build the binary with CGO enabled (critical for the database)
CGO_ENABLED=1 go build -o snowballin main.go
```

## 🧪 Running the Bot

### 🔁 Daemonizing Snowballbot with systemd (Ubuntu/Debian)
``` Bash

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
journalctl -u snowballin.service -f --no-pager
```

## 🔁 Restart & Recovery
### Rebuilds open positions by scanning Kraken trade history (last 24h)

- Filters out test trades, dust, and stale fills 
- Falls back to midpoint price if history is unavailable 
- Persists key state data: AvgEntry, PositionVol, PriceHistory, and ProfitCumulative

## 🧠 Tips
- ✅ Set a custom nonce window in your Kraken API key settings to avoid EAPI:Invalid nonce errors.
- 📅 Trade reconstruction defaults to the last 24h — ensure your actual position aligns with that window.
- 🎯 Snowballbot auto-adjusts for volatility, but you can override RSI gates, slack, and grid spacing per asset if needed.
- 🕒 Batch price fetches from Coingecko every 30–60 seconds to stay within API rate limits.

## 🧾 Example Output
```Plaintext

[MAIN] loaded config: assets=[XBT ETH SOL]  gridLevels=3
[RECOVER] ETH: reconstructed position 0.20568 @ avg entry 1806.72 from trade history
[BUY] XBT: grid leg 2 filled — 0.00147 @ 61,480.00
[TP] SOL: trailing take-profit triggered — sold 0.26401 @ 149.20 (+$35.11 realized)
[INFO] Updating price cache… (3 assets via Coingecko)
[CHECK] ETH: RSI=38.7 volF=1.6 slope=-0.87 — HOLD (no signal)
```

## ✍️ Author
Built by Shane Jones — a security consultant and market automation enthusiast.
This project was designed to snowball crypto profits through smart grid logic and lightweight VPS deployment.
Development assistance provided by AI (ChatGPT/Gemini) — all logic, flow, and architecture hand-validated.

## 💸 Donations
If Snowballin helps you level up your trading or gives you a spark of inspiration, feel free to toss a tip my way — no pressure, and definitely not expected.

**BTC Address:** bc1qnu8pxqp0mf8794fyn6cnxcxllceaywysde3uu2

Just knowing it's helping others is already a win. Thanks for being here.

## 🛡 License
Provided for educational and research purposes only. No warranty, no guarantee of profitability, and absolutely no liability — use at your own risk.

You're free to study, modify, and run this code for personal use. **If you break something, lose funds, or blow up your Kraken account, that’s on you — not me.**

