# ❄️ Snowballin' - A Crypto DCA Trading Bot

```
      _________              ___.        .__  .__  .__   /\
     /   _____/ ____   ______ _\_ |__ _____  |  | |  | |__| ___)/
     \_____  \ /    \ /  _ \ \/ \/ /| __ \\__  \ |  | |  | |  |/    \
     /        \  |  (  <_> )     / | \_\ \/ __ \|  |_|  |_|  |   |  \
    /_______  /___|  /\____/ \/\_/  |___  (____  /____/____/__|___|  /
            \/     \/                   \/    \/                  \/
```

**A resilient, multi-timeframe, Dollar-Cost-Averaging (DCA) cryptocurrency trading bot designed for strategic accumulation and risk-managed profit-taking.**

---
## 🚀 About Snowballin'

Snowballin' began as a personal challenge. Could I use AI to teach myself technical analysis? If so, could I then turn that knowledge into something autonomous? Six months later, this project became a reality: a fully AI-generated, market-adaptive trading strategist. The initial version was a several thousand lines of Go file; this version is entirely refactored for robustness and clarity.

It’s designed for security and survivability. Kraken was chosen specifically because of its pro-trading bot policies and withdrawal key model — funds can't move unless your account is compromised *and* the destination is pre-approved.

This isn’t financial advice. It’s a tool. Use it at your own risk.

### 🧠 For the curious hacker:

This README holds just enough to get your hands dirty. It’s not about turnkey solutions — it’s about sparking ideas. AI isn’t just some passive chatbot. It’s leverage. It’s a force multiplier. Use it to learn faster, automate deeper, and explore smarter.

---

### **Want to join me on Kraken?** Use my referral and we’ll both get $50 when you trade $200:

🔗 [Kraken Referral Link](https://invite.kraken.com/JDNW/4lfv2ftb)  
🧾 Code: ksdqyb9d

---
## ✨ How It Works
Snowballin' is not a simple grid bot that buys on fixed price drops. It operates on a multi-source consensus model to make intelligent, risk-managed decisions. On every cycle, it:

1.  **Gathers Data:** It pulls market data (price, OHLCV, order book) from your primary broker (Kraken) and optional secondary sources (CoinGecko, CoinMarketCap).
2.  **Calculates Consensus:** It analyzes the data using a confluence of technical indicators (RSI, StochRSI, MACD), liquidity hunt patterns, and order book depth across multiple timeframes.
3.  **Weighs & Decides:** It combines these signals to produce a single, high-conviction trading decision (Buy, Sell, or Hold).
4.  **Executes with Risk Management:** If a buy signal is triggered, it calculates a position size based on your configuration, checks your available balance, and places the trade.
5.  **Manages Autonomously:** It monitors the order until filled, manages the open position with a sophisticated DCA engine and a hybrid trailing stop-loss, and looks for the next exit signal, all while persisting its state to a local database to survive restarts.

---
## 🚀 Key Features
* **Multi-Source Consensus Engine:** Blends weighted data from Kraken, CoinGecko, and CoinMarketCap to form high-conviction trading signals and prevent trades on bad data.
* **Resilient DCA Engine:** Automatically manages positions by placing scaled safety orders at progressively lower prices to average down the entry cost during drawdowns.
* **Advanced Risk Management Suite:**
    * **Portfolio Circuit Breaker:** Halts trading and exits all positions if portfolio drawdown exceeds a set percentage.
    * **Pre-flight Balance Check:** Never attempts a trade it cannot afford.
    * **Strategy-Based Exits:** Identifies bearish chart patterns to exit a losing trade before it gets worse.
* **Autonomous Trade Lifecycle:**
    * **Persistent State:** Uses an SQLite database to save and load all pending orders and open positions, ensuring a full recovery after a restart.
    * **Hybrid Trailing Stop-Loss:** Secures partial profits once a target is hit and then activates a trailing stop on the remaining position to capture additional upside.
    * **Stale Order Reaping:** Automatically cancels limit orders that remain unfilled for too long, freeing up capital.
* **Intelligent Signal Modulators:**
    * **Order Book Analysis:** Reads live order book depth to place limit orders on high-volume "buy walls."
    * **Sentiment-Adjusted Logic:** Uses the Fear & Greed Index to adjust strategy parameters based on market sentiment.
* **Operational Robustness:**
    * **Pre-Flight Checks:** Verifies all API connections and configurations on startup to prevent errors.
    * **Live Discord Notifications:** Sends real-time alerts for filled orders, new positions, exits, and errors.

---
## 🧱 Prerequisites

-   **VPS:** `t2.medium` (2 vCPU / 4GB RAM) or equivalent
-   **OS:** Ubuntu 20.04+ or other Debian-based distro
-   **Go:** Version 1.18 or later
-   **Git**
-   **Build Tools:** A C compiler toolchain (`build-essential` on Debian/Ubuntu) is required for the SQLite database driver.
-   **Kraken API Key/Secret** with trade permissions.

---

## 🛠️ Configuration

All bot behavior is controlled via the `config.json` file.

1.  **API Keys:**
    * `kraken`: Enter your Kraken API Key and API Secret.
    * `coingecko` / `coinmarketcap`: Enter your API keys for these data providers. They are highly recommended for data integrity.
2.  **Discord:**
    * `webhook_url`: Paste your Discord webhook URL to receive notifications.
3.  **Trading Parameters (`trading` section):**
    * `asset_pairs`: A list of assets to trade (e.g., `["BTC/USD", "ETH/USD"]`).
    * `quote_currency`: The currency to trade with (e.g., `"USD"`).
    * `base_order_size`: The dollar amount for the initial entry into a new position (e.g., `15.0`).
    * `safety_order_size`: The dollar amount for the first safety order.
    * `max_safety_orders`: The maximum number of times the bot will average down.
    * `price_deviation_to_open_safety_orders`: The percentage the price must drop before the first safety order is placed.
    * `safety_order_step_scale`: Multiplies the price deviation for each subsequent safety order, creating wider gaps as the price drops.
    * `safety_order_volume_scale`: Multiplies the size of each subsequent safety order.
    * `take_profit_percentage`: The target net profit percentage for a position.
    * `trailing_stop_enabled`: Set to `true` to enable the hybrid take-profit/trailing stop feature.

---

## 🚀 Installation & Running

### Installation
```bash
# Install Go, Git, and the necessary C compiler tools
sudo apt update && sudo apt install -y golang-go git build-essential

# Clone the repository
git clone [https://github.com/Ohmjones/Snowballin.git](https://github.com/Ohmjones/Snowballin.git) /home/ubuntu/snowballin
cd /home/ubuntu/snowballin

# Build the binary with CGO enabled (critical for the database)
CGO_ENABLED=1 go build -o snowballin .
```

### Running as a Service (systemd)
This is the recommended way to run the bot continuously on a server.

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
journalctl -u snowballin.service -f --no-pager
```

---
## 📈 Strategy Deep Dive

### Entry Logic
A new position is only opened if all gates are passed in sequence:

1.  **Gate 1: Daily Trend Filter**
    * The asset's price must be above its 50-day EMA.
    * **Override:** This gate can be bypassed if the Crypto Fear & Greed Index is below the `fear_override_threshold` (e.g., 25), indicating extreme market fear.
2.  **Gate 2: Intermediate Confirmation**
    * The bot must detect either a bullish momentum shift on the 4-hour MACD or a low RSI on the 1-hour chart.
3.  **Gate 3: Final Confirmation**
    * The bot performs a final check for oversold conditions on multiple 1-hour indicators (RSI and StochRSI) to confirm the dip is ready to be bought.
    * The final limit price is optimized by finding the strongest nearby "buy wall" in the order book.

### Position Management
Once a position is open, the bot's goal is to exit profitably.

* **If Price Rises:** It waits for the price to hit the fee-aware take-profit target.
* **If Price Drops:** It holds the position and waits for the price to fall to the next calculated safety order level. When hit, it places a new, larger buy order to lower the position's average entry price. This also recalculates and lowers the take-profit target, making it easier to exit profitably on a smaller bounce.

### Exit Logic
The bot exits a position under three conditions:

1.  **Take-Profit:** The price hits the target. If trailing stops are enabled, it sells a partial amount and lets the rest run. Otherwise, it closes the full position.
2.  **Strategy Exit (`GenerateExitSignal`):** In every cycle, the bot analyzes the chart for specific bearish patterns. If a high-conviction bearish signal appears, it will market-sell the entire position to cut losses and prevent further drawdown.
3.  **Circuit Breaker:** If the entire portfolio value drops by a defined percentage, the bot assumes a catastrophic market event, liquidates all positions, and halts trading.

---
## 🧠 Tips
- ✅ Set a custom nonce window in your Kraken API key settings to avoid `EAPI:Invalid nonce` errors.
- 📅 The bot features a robust position reconstruction mechanism on startup. It scans recent trade history to rebuild its state if the database is lost.
- 🎯 Snowballin' auto-adjusts for volatility, but you can override RSI gates, slack, and grid spacing per asset if needed.
- 🕒 Batch price fetches from Coingecko every 30–60 seconds to stay within API rate limits.

---
## 🧾 Example Output
```plaintext
[INFO] AppRun: Starting pre-flight checks...
[INFO] Pre-Flight: Broker verification passed. Initial portfolio value: 500.01 USD
[INFO] AppRun: Loaded 1 open position(s) and 0 pending order(s) from database.
[INFO] Reconciliation: State verification complete.
[INFO] AppRun: Pre-flight checks complete. Starting main trading loop every 5m0s.
-------------------- New Trading Cycle --------------------
[INFO] GenerateSignals: SOL/USD -> HOLD - Reason: Daily trend is bearish. Price (163.06) is below Daily EMA(50) (167.89)
[INFO] GenerateSignals: ETH/USD -> BUY - Reason: Daily trend is bullish. | 4H Momentum Shift & 1H RSI Low (43.64) | Bullish Confirmation: RSI (24.10) and StochRSI (0.00) are both oversold.
[INFO] SeekEntry [ETH/USD]: BUY signal confirmed. Calculating order...
[WARN] SeekEntry [ETH/USD]: Insufficient funds to place buy order. Required: ~29.88 USD, Available: 15.45 USD. The order will be skipped.
[INFO] ManagePosition [BTC/USD]: Managing position. AvgPrice: 113389.00, Vol: 0.0002, SOs: 1, TP: 119058.45
[INFO] ManagePosition [BTC/USD]: Price condition met for Safety Order #2. Placing limit buy.
[INFO] ManagePosition [BTC/USD]: Placed Safety Order #2. ID: OABCDE-FGHIJ-KLMNOP
```

---
## ✍️ Author
Built by Shane Jones — a security consultant and market automation enthusiast. This project was designed to snowball crypto profits through smart grid logic and lightweight VPS deployment. Development assistance provided by AI (ChatGPT/Gemini) — all logic, flow, and architecture hand-validated.

## 💸 Donations
If Snowballin' helps you level up your trading or gives you a spark of inspiration, feel free to toss a tip my way — no pressure, and definitely not expected.

**BTC Address:** `bc1qnu8pxqp0mf8794fyn6cnxcxllceaywysde3uu2`

Just knowing it's helping others is already a win. Thanks for being here.

## 🛡 License
Provided for educational and research purposes only. No warranty, no guarantee of profitability, and absolutely no liability — use at your own risk.

You're free to study, modify, and run this code for personal use. **If you break something, lose funds, or blow up your Kraken account, that’s on you — not me.**
