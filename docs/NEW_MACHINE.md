# Setting up sol-sniper on a new machine

This repo has the code only. Your **secrets and history are not in it** (on purpose — the repo is public):
`config/sniper.yaml` (RPC / Jupiter / PumpPortal / email keys), `logs/active-wallet.json` (wallet private
key), `logs/` (trade logs, Candles history, runtime settings) and `sqlitebase/` (state).

## 1. Get the code

```bash
git clone https://github.com/ssisaa/solsniper.git sol-sniper
cd sol-sniper
```

On Windows use WSL (Ubuntu). The scripts expect the folder at `/home/user/sol-sniper`; if yours is
elsewhere, change the `cd /home/user/sol-sniper` line at the top of `scripts/*.sh`.

## 2. Python environment

Python 3.12 or newer (the bot was last run on 3.14).

```bash
python3 -m venv venv
venv/bin/pip install -r requirements.txt
```

## 3. Configuration (secrets stay on the machine)

```bash
cp config/sniper.example.yaml config/sniper.yaml
```

Fill in `config/sniper.yaml`: the `rpc` section (paid RPC URLs under `rpc.paid`, fastest first; public ones
under `rpc.urls` as fallback), Jupiter key, PumpPortal API key
(`pumpportal.api_key`, Lightning wallet = the bot wallet), email settings. `config/candle.yaml` (Candles
settings) is already in the repo.

**Moving from the old machine instead?** Copy these over privately (USB / scp), never through Git:
`config/sniper.yaml`, `logs/active-wallet.json`, `logs/runtime_state.json` (Settings page), `logs/candle_momo.sqlite`
(Candles history), `logs/ledger.jsonl`, `sqlitebase/`.

## 4. Wallet

Start the bot (step 5), open http://localhost:8080 → **Dashboard → Wallet → Change wallet**, upload the Solana
keypair JSON. It is saved as `logs/active-wallet.json`. Keep at least 0.05 SOL in it.

## 5. Run

Quick start (foreground):

```bash
venv/bin/python -m src.main --mode live-feed --config config/sniper.yaml --duration 0
```

Normal use (supervised, restarts after crashes):

```bash
bash scripts/start_bot.sh
bash scripts/stop_bot.sh
bash scripts/safe_restart.sh
```

On Windows, to start it at logon: create a scheduled task (e.g. **SolSniperBot**, at logon + every 5 min,
"do not start a new instance") that runs
`conhost --headless wsl.exe -d Ubuntu -u <your-user> -- /home/<your-user>/sol-sniper/scripts/run_forever.sh`,
and in `C:\Users\<you>\.wslconfig` set `vmIdleTimeout=-1` and `instanceIdleTimeout=-1` so WSL stays up.

## 6. Check it works

```bash
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 venv/bin/python -m pytest -q tests     # 3 hard_filters tests fail already
tail -f logs/run.out                                                      # look for "heartbeat" lines
```

Open the dashboard, confirm the wallet balance, then set **Auto Trade** and the **Live Controls source**.
New settings start safe: check Max position, Auto Trade and Candles' Live switch before trading.

## 7. Continue with Claude Code

Open the folder in Claude Code. It reads [CLAUDE.md](../CLAUDE.md) automatically — the project state, the
rules you set, and what was built — so it carries on from where the last session stopped. To push changes
from the new machine, log in once (`git push` asks for your GitHub user and a personal access token).
