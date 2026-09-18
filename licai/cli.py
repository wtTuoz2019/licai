from __future__ import annotations

import argparse
import json
import time

from .auth import require_dashboard_password
from .config import fmt_amount, load_settings
from .earn import FlexibleProduct
from .pipeline import Pipeline, StepResult, apr_percent


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m licai",
        description="币安活期理财：扫描最高年化并申购；统一账户下仓位直接当保证金，不划转",
    )
    parser.add_argument("command", choices=["scan", "pick", "run", "buy", "to-margin", "cycle", "web"], help="要执行的动作")
    parser.add_argument("--live", action="store_true", help="真实下单（默认是模拟）")
    parser.add_argument("--confirm", action="store_true", help="配合 --live，确认你知道这会动用真实资金")
    parser.add_argument("--interval", type=int, default=0, help="scan 循环间隔秒数，0 表示只跑一次")
    parser.add_argument("--top", type=int, default=15, help="scan 展示前 N 条")
    parser.add_argument("--config", default=None, help="配置文件路径")
    parser.add_argument("--host", default="127.0.0.1", help="web 监听地址")
    parser.add_argument("--port", type=int, default=8765, help="web 端口")
    args = parser.parse_args(argv)

    dry_run = True
    if args.live:
        if args.command == "scan":
            dry_run = True
        elif not args.confirm:
            raise SystemExit("真实下单需要同时加上 --live --confirm")
        else:
            dry_run = False

    settings = load_settings(args.config, dry_run_override=dry_run)
    if args.command == "web":
        require_dashboard_password()
        return _cmd_web(args.host, args.port)
    pipeline = Pipeline(settings)
    if args.command not in {"scan", "pick"}:
        pipeline.require_keys()

    if args.command == "scan":
        return _loop_or_once(lambda: _cmd_scan(pipeline, args.top), args.interval)
    if args.command == "pick":
        return _cmd_pick(pipeline)
    if args.command == "buy":
        return _cmd_buy(pipeline)
    if args.command == "to-margin":
        return _cmd_to_margin(pipeline)
    if args.command == "cycle":
        return _cmd_cycle(pipeline)
    return _cmd_run(pipeline)


def _cmd_web(host: str, port: int) -> int:
    import uvicorn

    from .web import app

    print(f"操作台: http://{host}:{port}")
    print("先输入访问密码才能看数据和操作。默认不自动下单，页面确认后才是实盘。")
    uvicorn.run(app, host=host, port=port, log_level="info")
    return 0


def _loop_or_once(fn, interval: int) -> int:
    if interval <= 0:
        return fn()
    while True:
        fn()
        print(f"\n{interval} 秒后再次扫描...\n")
        time.sleep(interval)


def _cmd_scan(pipeline: Pipeline, top: int) -> int:
    products = pipeline.list_products()
    margin_assets = pipeline.margin_assets()
    ranked = sorted((p for p in products if p.purchasable), key=lambda p: p.apr, reverse=True)
    source = "官方 API" if pipeline.settings.api_key else "公开行情（无需 API Key）"
    print(f"只看能当合约保证金的保本活期：{', '.join(pipeline.settings.margin_earn_assets)}")
    print(f"共 {len(ranked)} 个可申购 | 数据来源: {source}")
    print(f"模式: {'模拟' if pipeline.settings.dry_run else '实盘'} | {'统一账户' if pipeline.settings.unified_account else '经典账户'} | 资金 {pipeline.settings.source_asset}")
    print()
    print(f"{'年化':>8}  {'资产':<10} {'类型':<10} {'最小申购':>12}  productId")
    print("-" * 70)
    for product in ranked[:top]:
        print(_row(product, margin_assets))
    print()
    try:
        chosen = pipeline.pick(products, margin_assets)
        print("当前会选中:")
        print(_row(chosen, margin_assets))
        print(f"提示: 统一账户下申购 {chosen.asset} 后即可作为合约保证金，无需划转")
        if chosen.asset == "BFUSD" and chosen.apr == 0:
            print("提示: 未配置 API Key 时 BFUSD 实时年化拉不到，以平台「保本型」页为准")
    except RuntimeError as exc:
        print(f"没有可选产品: {exc}")
        return 1
    return 0


def _cmd_pick(pipeline: Pipeline) -> int:
    product = pipeline.pick()
    margin_assets = pipeline.margin_assets()
    print(_row(product, margin_assets))
    return 0


def _cmd_buy(pipeline: Pipeline) -> int:
    product = pipeline.pick()
    amount = pipeline.plan_amount(product)
    print(f"准备申购 {amount} {pipeline.settings.source_asset} -> {product.asset} 年化 {apr_percent(product.apr)}%")
    _print_steps(pipeline.buy(product, amount))
    return 0


def _cmd_to_margin(pipeline: Pipeline) -> int:
    product = pipeline.pick()
    margin_assets = pipeline.margin_assets()
    amount = pipeline.position_amount(product) or pipeline.plan_amount(product)
    if pipeline.settings.unified_account:
        print(f"统一账户：核对该仓位是否已计入保证金（不会划转）")
    else:
        print(f"准备把 {fmt_amount(amount)} {product.asset} 兑成保证金并划转合约")
    _print_steps(pipeline.to_margin(product, amount, margin_assets))
    return 0


def _cmd_cycle(pipeline: Pipeline) -> int:
    from .cycle import HedgeCycle

    print(
        f"{'模拟盘' if pipeline.settings.dry_run else '实盘'} 循环: "
        f"现货USDT->理财保证金 -> {pipeline.settings.hedge_symbol} "
        f"{pipeline.settings.hedge_leverage}x 多空GTX对冲 -> 平盈利腿后补仓，闲置USDT再申购理财"
    )
    if pipeline.settings.hedge_leverage >= 50:
        print("警告: 高杠杆。平盈利腿后若补仓失败，剩下的腿会变成单向曝口。")
    steps = HedgeCycle(pipeline).run(watch=not pipeline.settings.dry_run)
    _print_steps(steps)
    failed = [s for s in steps if not s.ok]
    return 1 if failed else 0


def _cmd_run(pipeline: Pipeline) -> int:
    if pipeline.settings.unified_account:
        flow = "选可作保证金的最高活期 -> 申购（仓位直接当保证金，不划转）"
    else:
        flow = "选最高活期 -> 申购 -> 兑保证金 -> 划转合约"
    print(f"{'模拟盘' if pipeline.settings.dry_run else '实盘'} 全流程: {flow}")
    steps = pipeline.run()
    _print_steps(steps)
    failed = [s for s in steps if not s.ok]
    return 1 if failed else 0


def _row(product: FlexibleProduct, margin_assets: set[str] | None = None) -> str:
    kind = "BFUSD专项" if product.kind == "bfusd" else "活期"
    return (
        f"{apr_percent(product.apr):>7.2f}%  "
        f"{product.asset:<10} "
        f"{kind:<10} "
        f"{product.min_purchase:>12}  {product.product_id}"
    )


def _print_steps(steps: list[StepResult]) -> None:
    for step in steps:
        if step.name == "subscribe_amount":
            continue
        flag = "OK" if step.ok else "FAIL"
        mode = "模拟" if step.dry_run else "实盘"
        detail = step.detail
        if not isinstance(detail, str):
            detail = json.dumps(detail, ensure_ascii=False, default=str)
        print(f"[{flag}][{mode}] {step.name}: {detail}")


if __name__ == "__main__":
    raise SystemExit(main())
