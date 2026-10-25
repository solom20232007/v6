# Binance Early-Pump Scanner V6 — Experimental

V6 is a separate experimental copy based on the supplied V5 archive. It does not overwrite V4 or V5 files/state.

## Carried forward from V5
- Smart Money heuristic using buy pressure, volume ratios/acceleration, spot order-book imbalance and (when available) futures open-interest change.
- Closed 1-minute candle entry confirmation.
- Signal outcome tracking with TP1 / SL / TIMEOUT and MFE/MAE reporting.
- Self-learning threshold evaluator with bounded adjustments and minimum sample sizes.
- Candidate ranking, wash-trading heuristics, BTC market guard, cross-exchange confirmation, and Telegram alerts.

## V6 changes
- **Fake-breakout rejection (configurable):** rejects a marginal breakout only when the breakout is within a configurable distance above the previous high, the candle closes weakly in its range, and the upper wick is large. This is a heuristic, not a guarantee.
- **Track protection:** suppresses a new FINAL alert for a symbol while an earlier signal is still retained in `active_tracks`, preventing its tracking record from being overwritten and preserving its MFE/MAE/outcome history.
- **State isolation:** uses V6-specific state, CSV, log, workflow concurrency group, script/module names and Telegram tag.

## Configuration
- `FAKE_BREAKOUT_FILTER_ENABLED=1` (default on)
- `FAKE_BREAKOUT_MIN_CLOSE_LOCATION=0.45`
- `FAKE_BREAKOUT_MIN_UPPER_WICK=0.50`
- `FAKE_BREAKOUT_MAX_DISTANCE_PCT=2.0`
- Existing V5 environment settings remain supported.

## Important limitations
- Smart Money Score is a heuristic and does not prove institutional activity.
- No profitability is claimed. Validate with historical data and paper trading before relying on signals.
- This archive was syntax-checked and unit-tested for the V6 fake-breakout/track-protection logic. No live Binance run or profitability backtest was performed here.
- GitHub Actions secrets `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID` must be configured in the V6 repository. Review the workflow and cron trigger before enabling it.
