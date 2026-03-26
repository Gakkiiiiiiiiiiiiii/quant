-- 日期：2026-03-21
-- 执行者：Codex
-- 说明：本文件依据设计文档附录 A 整理，作为 PostgreSQL 初始 DDL 基线。

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS strategy_versions (
    strategy_version_id    uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    strategy_name          varchar(64) NOT NULL,
    version_label          varchar(64) NOT NULL,
    implementation         varchar(64) NOT NULL,
    parameters             jsonb NOT NULL DEFAULT '{}'::jsonb,
    metrics                jsonb NOT NULL DEFAULT '{}'::jsonb,
    artifact_uri           text NULL,
    created_at             timestamptz NOT NULL DEFAULT now(),
    UNIQUE(strategy_name, version_label)
);

CREATE TABLE IF NOT EXISTS promotion_requests (
    promotion_request_id   uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    strategy_version_id    uuid NOT NULL REFERENCES strategy_versions(strategy_version_id),
    object_type            varchar(32) NOT NULL CHECK (object_type IN ('promotion_request', 'risk_rule_change', 'manual_order')),
    requested_by           varchar(64) NOT NULL,
    status                 varchar(16) NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'approved', 'rejected')),
    reason                 text NULL,
    created_at             timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS order_intents (
    order_intent_id        uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    strategy_version_id    uuid NULL REFERENCES strategy_versions(strategy_version_id),
    account_id             varchar(64) NOT NULL,
    trading_date           date NOT NULL,
    symbol                 varchar(32) NOT NULL,
    side                   varchar(8) NOT NULL CHECK (side IN ('buy', 'sell')),
    qty                    integer NOT NULL,
    limit_price            numeric(18, 6) NULL,
    reference_price        numeric(18, 6) NOT NULL,
    source                 varchar(16) NOT NULL CHECK (source IN ('strategy', 'manual', 'system')),
    metadata               jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at             timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS risk_decisions (
    risk_decision_id       uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    order_intent_id        uuid NOT NULL REFERENCES order_intents(order_intent_id) ON DELETE CASCADE,
    status                 varchar(16) NOT NULL CHECK (status IN ('approved', 'rejected', 'manual_review')),
    rule_results           jsonb NOT NULL DEFAULT '[]'::jsonb,
    decided_at             timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS orders (
    order_id               uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    order_intent_id        uuid NOT NULL REFERENCES order_intents(order_intent_id),
    risk_decision_id       uuid NULL REFERENCES risk_decisions(risk_decision_id),
    broker_order_id        varchar(64) NULL,
    status                 varchar(16) NOT NULL,
    account_id             varchar(64) NOT NULL,
    symbol                 varchar(32) NOT NULL,
    side                   varchar(8) NOT NULL CHECK (side IN ('buy', 'sell')),
    qty                    integer NOT NULL,
    filled_qty             integer NOT NULL DEFAULT 0,
    avg_price              numeric(18, 6) NULL,
    created_at             timestamptz NOT NULL DEFAULT now(),
    updated_at             timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS order_events (
    order_event_id         bigserial PRIMARY KEY,
    order_id               uuid NOT NULL REFERENCES orders(order_id) ON DELETE CASCADE,
    status                 varchar(16) NOT NULL,
    source                 varchar(16) NOT NULL CHECK (source IN ('strategy', 'risk', 'execution', 'qmt', 'manual', 'system')),
    payload                jsonb NOT NULL DEFAULT '{}'::jsonb,
    event_time             timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS trades (
    trade_id               uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    order_id               uuid NOT NULL REFERENCES orders(order_id),
    symbol                 varchar(32) NOT NULL,
    side                   varchar(8) NOT NULL CHECK (side IN ('buy', 'sell')),
    fill_qty               integer NOT NULL,
    fill_price             numeric(18, 6) NOT NULL,
    commission             numeric(18, 6) NOT NULL DEFAULT 0,
    trade_time             timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS position_snapshots (
    snapshot_id            bigserial PRIMARY KEY,
    account_id             varchar(64) NOT NULL,
    symbol                 varchar(32) NOT NULL,
    qty                    integer NOT NULL,
    available_qty          integer NOT NULL,
    cost_price             numeric(18, 6) NOT NULL,
    market_price           numeric(18, 6) NOT NULL,
    snapshot_time          timestamptz NOT NULL,
    UNIQUE(account_id, snapshot_time, symbol)
);

CREATE TABLE IF NOT EXISTS asset_snapshots (
    snapshot_id            bigserial PRIMARY KEY,
    account_id             varchar(64) NOT NULL,
    cash                   numeric(18, 6) NOT NULL,
    frozen_cash            numeric(18, 6) NOT NULL DEFAULT 0,
    total_asset            numeric(18, 6) NOT NULL,
    total_pnl              numeric(18, 6) NOT NULL DEFAULT 0,
    turnover               numeric(18, 6) NOT NULL DEFAULT 0,
    max_drawdown           numeric(18, 6) NOT NULL DEFAULT 0,
    snapshot_time          timestamptz NOT NULL,
    UNIQUE(account_id, snapshot_time)
);

CREATE TABLE IF NOT EXISTS audit_logs (
    audit_log_id           bigserial PRIMARY KEY,
    object_type            varchar(32) NOT NULL,
    object_id              varchar(64) NOT NULL,
    message                text NOT NULL,
    payload                jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at             timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS common_strategies (
    strategy_id            uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    strategy_key           varchar(32) NOT NULL UNIQUE,
    display_name           varchar(64) NOT NULL,
    is_active              integer NOT NULL DEFAULT 1,
    created_at             timestamptz NOT NULL DEFAULT now(),
    updated_at             timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS strategy_backtest_results (
    backtest_result_id     uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    strategy_id            uuid NOT NULL REFERENCES common_strategies(strategy_id) ON DELETE CASCADE,
    run_key                varchar(64) NOT NULL,
    mode                   varchar(16) NOT NULL,
    start_date             varchar(16) NOT NULL,
    end_date               varchar(16) NOT NULL,
    account                numeric(18, 6) NOT NULL DEFAULT 0,
    total_return           numeric(18, 6) NULL,
    annualized_return      numeric(18, 6) NULL,
    max_drawdown           numeric(18, 6) NULL,
    ending_equity          numeric(18, 6) NULL,
    report_path            text NULL,
    risk_path              text NULL,
    daily_action_path      text NULL,
    daily_decision_path    text NULL,
    raw_payload            jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at             timestamptz NOT NULL DEFAULT now()
);
