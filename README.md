# Trade Seeker — Multi-Rule Spot Scanner (Binance)

Monitors all USDT spot pairs and runs 4 detection rules:

1. Pump spike (fast +% with volume burst)  
2. Dump spike (fast −% with volume burst)  
3. Breakout Up (range breakout + confirmation vs EMA)  
4. Breakdown Down (range breakdown + confirmation vs EMA)  

Rules are configurable per timeframe in `config.yaml`. Tiers (S/A/B/…) let you apply different minimum notional filters per symbol.

---

## 🚀 Quick start

```bash
# on a fresh Ubuntu 24.04 VPS
apt update && apt install -y git python3 python3-venv python3-pip
git clone https://github.com/<you>/trade-seeker.git
cd trade-seeker
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env   # then edit
