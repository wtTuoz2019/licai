# 币安保本活期当保证金 + 多空对冲循环

保证金是理财仓位（USDT 活期 / BFUSD）。平仓套出的是盈利 USDT，再申购理财，保证金变大，对冲仓位两边一起加。

## 循环怎么走

1. 闲置 USDT 申购 USDT 活期或 BFUSD，仓位自动当保证金。
2. 按当前杠杆和 uniMMR 安全线尽量开满对冲（低杠杆最多用约 85% 保证金；100x 仍留约 40% 给平仓补仓的单边窗口）。
3. 一边够盈利：平掉 → 补回平衡 → 盈利 USDT 再申购理财。
4. 理财仓位变大后，若 uniMMR 仍高于安全线（默认 2，爆仓约 1.05），两边按新保证金一起加仓。
5. 回到盯盘。

## 命令

```bash
python -m licai scan
python -m licai web
python -m licai cycle
python -m licai cycle --live --confirm
```

操作台 `python -m licai web` 打开 http://127.0.0.1:8765

- 绑定账号；每个账号可填 HTTP 代理，空则走本机默认 IP
- 资金/理财一天拉一次，点「刷新」才重拉；仓位浮盈按 `live_poll_seconds` 更新
- 收利门槛默认按单边仓位名义本金的 2.5%（再和手续费倍数取较大值）。输入框会填这个建议值，点保存才锁定；没锁定时收利加仓后会按新仓位重算
- 先看净利润能不能覆盖手续费，价格 15 分钟振幅低于 0.25%、冷却满 10 分钟后，再点一键平仓
- 换更高年化时点「分批换更高年化」：每批按 uniMMR 不低于安全线估能赎多少，赎完立刻申购，碰到安全线就停
- 默认不自动下单。页面点确认才会真下单
- 账号密钥存在本地 `data/accounts.sqlite`，不要提交 git

## 仓位

- `hedge_margin_use_pct: 0.85` 低杠杆最多用到 85%；高杠杆按 `40/杠杆` 自动收紧
- `min_uni_mmr: 2` uniMMR 低于这个不加仓（越大越安全，爆仓约 1.05）
- `scale_min_add_pct: 0.03` 目标仓位大 3% 以上才加
- `hedge_qty: 0` 自动按理财仓位算；写成固定数量则不加仓
- 每个账号可单独选套保币对：ETHUSDT / BTCUSDT / SOLUSDT / BNBUSDT，默认 ETHUSDT
- `harvest_pos_pct: 0.025` 每次收利默认建议值 = 单边仓位名义 × 2.5%

## 服务器部署

仓库不含密钥和 sqlite。服务器上克隆后在页面里重新绑账号。

```bash
sudo apt-get update
sudo apt-get install -y python3 python3-venv python3-pip git
git clone git@github.com:wtTuoz2019/licai.git
cd licai
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

需要的话改 `config.yaml`（杠杆、uniMMR、收利比例等）。不要把 API Key 写进仓库。

后台跑操作台（监听所有网卡，方便用服务器 IP 访问）：

```bash
source .venv/bin/activate
mkdir -p logs
nohup python -m licai web --host 0.0.0.0 --port 8765 > logs/web.log 2>&1 &
```

或用 systemd，把 `User`、路径改成你的：

```ini
[Unit]
Description=licai dashboard
After=network.target

[Service]
WorkingDirectory=/opt/licai
Environment=LICAI_DRY_RUN=true
ExecStart=/opt/licai/.venv/bin/python -m licai web --host 0.0.0.0 --port 8765
Restart=on-failure

[Install]
WantedBy=multi-user.target
```

打开 `http://服务器IP:8765`，添加账号后点一键入场/收利。操作台默认仍要你在页面确认才会实盘下单。

安全建议：用防火墙只放行自己的 IP，或前面加 nginx + 基本认证 / VPN。不要把 8765 裸暴露到公网。
