"""
급등패턴 매매 시뮬레이터 — 핵심 엔진
Surge Pattern Trade Simulator — Core Engine
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
파일경로: app/engine/surge_simulator.py

DTW 패턴 분석 결과(매수추천)를 받아 과거 데이터로
실제 매매를 시뮬레이션하여 수익성을 검증합니다.

전략:
  - 진입: DTW 유사도 ≥ threshold → 다음 거래일 시가 매수
  - 익절: +take_profit% 도달 시 매도
  - 손절: -stop_loss% 도달 시 매도
  - 시간손절: max_hold_days 거래일 경과 시 종가 매도
  - 포지션: 최대 max_positions종목 동시 보유, 균등 배분
  - 비용: 수수료 0.015% (매수+매도) + 매도세 0.23%
"""

import logging
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass, field, asdict
from datetime import datetime

logger = logging.getLogger(__name__)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 설정 상수 / Configuration Constants
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

COMMISSION_RATE = 0.00015      # 수수료 0.015% (매수+매도 각각)
SELL_TAX_RATE = 0.0023         # 매도세 0.23% (한국 주식)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 데이터 클래스 / Data Classes
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@dataclass
class SimConfig:
    """시뮬레이션 설정"""
    initial_capital: float = 10_000_000     # 초기 자금 1천만원
    take_profit_pct: float = 7.0            # 익절 기준 %
    stop_loss_pct: float = 3.0              # 손절 기준 %
    max_hold_days: int = 10                 # 최대 보유 거래일
    max_positions: int = 5                  # 최대 동시 보유 종목 수
    similarity_threshold: float = 65.0      # DTW 유사도 진입 기준
    trailing_stop: bool = False             # 트레일링 스톱 사용 여부
    trailing_pct: float = 3.0              # 트레일링 스톱 비율 %


@dataclass
class Trade:
    """개별 매매 기록"""
    code: str
    name: str
    buy_date: str
    buy_price: float
    sell_date: str = ""
    sell_price: float = 0
    quantity: int = 0
    invested: float = 0            # 투자금
    profit: float = 0              # 순수익 (비용 차감 후)
    profit_pct: float = 0          # 수익률 %
    hold_days: int = 0             # 보유 거래일
    exit_reason: str = ""          # 매도 사유: take_profit / stop_loss / time_exit
    commission: float = 0          # 수수료
    tax: float = 0                 # 매도세
    similarity: float = 0         # 진입 시 DTW 유사도
    cluster_id: int = 0           # 매칭된 클러스터 ID


@dataclass
class Position:
    """현재 보유 포지션"""
    code: str
    name: str
    buy_date: str
    buy_price: float
    quantity: int
    invested: float
    highest_price: float = 0       # 보유 중 최고가 (트레일링용)
    hold_days: int = 0
    similarity: float = 0
    cluster_id: int = 0
    buy_idx: int = 0               # 매수일의 candle index


@dataclass
class DailySnapshot:
    """일별 자산 스냅샷"""
    date: str
    total_asset: float             # 총 자산 (현금 + 평가)
    cash: float                    # 현금
    invested: float                # 투자금 (평가액)
    positions_count: int           # 보유 종목 수
    daily_return_pct: float = 0    # 당일 수익률
    cumulative_return_pct: float = 0  # 누적 수익률


@dataclass
class SimResult:
    """시뮬레이션 결과"""
    # 성과 지표
    total_trades: int = 0
    win_count: int = 0
    lose_count: int = 0
    win_rate: float = 0
    avg_profit_pct: float = 0       # 평균 수익률 (수익 거래)
    avg_loss_pct: float = 0         # 평균 손실률 (손실 거래)
    profit_loss_ratio: float = 0    # 손익비
    total_return_pct: float = 0     # 총 수익률
    max_drawdown_pct: float = 0     # 최대 낙폭
    avg_hold_days: float = 0        # 평균 보유 일수
    total_profit: float = 0         # 총 순수익
    total_commission: float = 0     # 총 수수료
    total_tax: float = 0            # 총 세금
    final_capital: float = 0        # 최종 자산
    # 패턴별 성과
    pattern_performance: List[Dict] = field(default_factory=list)
    # 매매 기록
    trades: List[Dict] = field(default_factory=list)
    # 일별 자산
    daily_snapshots: List[Dict] = field(default_factory=list)
    # 설정
    config: Dict = field(default_factory=dict)
    # 종목별 성과
    stock_performance: List[Dict] = field(default_factory=list)
    # 월별 성과
    monthly_performance: List[Dict] = field(default_factory=list)
    # 매도 사유 통계
    exit_stats: Dict = field(default_factory=dict)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# DTW 유사도 계산 (pattern_analyzer.py에서 가져옴)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def dtw_distance(s1: List[float], s2: List[float], window: int = None) -> float:
    """DTW 거리 계산"""
    n, m = len(s1), len(s2)
    if n == 0 or m == 0:
        return float('inf')
    if window is None:
        window = max(n, m)

    cost = [[float('inf')] * (m + 1) for _ in range(n + 1)]
    cost[0][0] = 0

    for i in range(1, n + 1):
        j_start = max(1, i - window)
        j_end = min(m, i + window)
        for j in range(j_start, j_end + 1):
            d = abs(s1[i - 1] - s2[j - 1])
            cost[i][j] = d + min(cost[i - 1][j], cost[i][j - 1], cost[i - 1][j - 1])

    return cost[n][m]


def dtw_similarity(s1: List[float], s2: List[float]) -> float:
    """DTW 유사도 (0~100, 높을수록 유사)"""
    if not s1 or not s2:
        return 0
    dist = dtw_distance(s1, s2)
    max_len = max(len(s1), len(s2))
    if max_len == 0:
        return 0
    normalized = dist / max_len
    similarity = max(0, 100 - normalized * 20)
    return round(similarity, 1)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 시뮬레이션 핵심 함수 / Core Simulation
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def run_surge_simulation(
    candles_by_code: Dict[str, List],
    names: Dict[str, str],
    clusters: List,
    config: SimConfig = None,
    progress_callback=None,
) -> SimResult:
    """
    급등패턴 매매 시뮬레이션 실행

    Parameters:
    -----------
    candles_by_code : {종목코드: [CandleDay, ...]}
        각 종목의 전체 일봉 데이터
    names : {종목코드: 종목명}
    clusters : List[PatternCluster]
        DTW 분석에서 도출된 공통 패턴 클러스터
    config : SimConfig
        시뮬레이션 설정
    progress_callback : callable(pct, message)
        진행률 콜백

    Returns:
    --------
    SimResult : 시뮬레이션 결과
    """
    if config is None:
        config = SimConfig()

    if progress_callback:
        progress_callback(0, "시뮬레이션 초기화 중...")

    # ── 클러스터에서 평균 패턴 추출 ──
    cluster_patterns = _extract_cluster_patterns(clusters)
    if not cluster_patterns:
        logger.warning("유효한 클러스터 패턴이 없음 — 시뮬레이션 불가")
        return _empty_result(config)

    # ── 모든 종목의 일봉을 날짜순 통합 타임라인 구성 ──
    timeline = _build_timeline(candles_by_code)
    if not timeline:
        return _empty_result(config)

    total_days = len(timeline)
    logger.info(f"시뮬레이션 기간: {timeline[0]['date']} ~ {timeline[-1]['date']} ({total_days}거래일)")

    # ── 시뮬레이션 상태 초기화 ──
    cash = config.initial_capital
    positions: List[Position] = []
    completed_trades: List[Trade] = []
    daily_snapshots: List[DailySnapshot] = []
    peak_asset = config.initial_capital

    pre_days = 10  # 패턴 비교 구간

    # ── 일별 시뮬레이션 루프 ──
    for day_idx, day_info in enumerate(timeline):
        current_date = day_info['date']

        if progress_callback and day_idx % 20 == 0:
            pct = int((day_idx / total_days) * 80)
            progress_callback(pct, f"시뮬레이션 중: {current_date} ({day_idx + 1}/{total_days})")

        # ── Step 1: 기존 포지션 매도 체크 ──
        new_positions = []
        for pos in positions:
            candles = candles_by_code.get(pos.code, [])
            if not candles:
                new_positions.append(pos)
                continue

            # 현재일의 캔들 찾기
            today_candle = _find_candle_by_date(candles, current_date)
            if not today_candle:
                new_positions.append(pos)
                continue

            pos.hold_days += 1
            today_high = today_candle.high if hasattr(today_candle, 'high') else today_candle['high']
            today_low = today_candle.low if hasattr(today_candle, 'low') else today_candle['low']
            today_close = today_candle.close if hasattr(today_candle, 'close') else today_candle['close']

            # 트레일링 스톱: 최고가 갱신
            if today_high > pos.highest_price:
                pos.highest_price = today_high

            # 매도 조건 체크
            sell_price = 0
            exit_reason = ""

            # 1) 익절 체크 (장중 고가 기준)
            tp_price = pos.buy_price * (1 + config.take_profit_pct / 100)
            if today_high >= tp_price:
                sell_price = tp_price
                exit_reason = "take_profit"

            # 2) 손절 체크 (장중 저가 기준)
            sl_price = pos.buy_price * (1 - config.stop_loss_pct / 100)
            if today_low <= sl_price and not exit_reason:
                sell_price = sl_price
                exit_reason = "stop_loss"

            # 3) 트레일링 스톱 체크
            if config.trailing_stop and pos.highest_price > pos.buy_price and not exit_reason:
                trail_price = pos.highest_price * (1 - config.trailing_pct / 100)
                if today_low <= trail_price:
                    sell_price = trail_price
                    exit_reason = "trailing_stop"

            # 4) 시간 손절 (최대 보유일 초과)
            if pos.hold_days >= config.max_hold_days and not exit_reason:
                sell_price = today_close
                exit_reason = "time_exit"

            if exit_reason:
                # 매도 실행
                trade = _execute_sell(pos, sell_price, current_date, exit_reason)
                completed_trades.append(trade)
                cash += (sell_price * pos.quantity) - trade.commission - trade.tax
                logger.debug(f"매도: {pos.name} @ {sell_price:,.0f} ({exit_reason}) "
                           f"수익: {trade.profit_pct:+.1f}%")
            else:
                new_positions.append(pos)

        positions = new_positions

        # ── Step 2: 신규 매수 신호 탐색 ──
        if len(positions) < config.max_positions:
            buy_signals = _scan_buy_signals(
                candles_by_code, names, day_idx, current_date,
                cluster_patterns, pre_days, config.similarity_threshold
            )

            # 이미 보유 중인 종목 제외
            held_codes = {p.code for p in positions}
            buy_signals = [s for s in buy_signals if s['code'] not in held_codes]

            # 유사도 높은 순 정렬
            buy_signals.sort(key=lambda s: s['similarity'], reverse=True)

            # 빈 슬롯만큼 매수
            slots_available = config.max_positions - len(positions)
            for signal in buy_signals[:slots_available]:
                # 슬롯당 자금 배분
                per_slot = config.initial_capital / config.max_positions
                if cash < per_slot * 0.5:  # 최소 50%는 있어야 매수
                    break

                invest_amount = min(per_slot, cash * 0.95)  # 현금의 95%까지만
                buy_price = signal['buy_price']

                if buy_price <= 0:
                    continue

                quantity = int(invest_amount / buy_price)
                if quantity <= 0:
                    continue

                actual_invest = buy_price * quantity
                buy_commission = actual_invest * COMMISSION_RATE
                cash -= (actual_invest + buy_commission)

                pos = Position(
                    code=signal['code'],
                    name=signal['name'],
                    buy_date=current_date,
                    buy_price=buy_price,
                    quantity=quantity,
                    invested=actual_invest,
                    highest_price=buy_price,
                    hold_days=0,
                    similarity=signal['similarity'],
                    cluster_id=signal.get('cluster_id', 0),
                    buy_idx=day_idx,
                )
                positions.append(pos)
                logger.debug(f"매수: {signal['name']} @ {buy_price:,.0f} "
                           f"(유사도: {signal['similarity']:.1f}%)")

        # ── Step 3: 일별 자산 스냅샷 ──
        invested_value = 0
        for pos in positions:
            candles = candles_by_code.get(pos.code, [])
            today_candle = _find_candle_by_date(candles, current_date)
            if today_candle:
                close = today_candle.close if hasattr(today_candle, 'close') else today_candle['close']
                invested_value += close * pos.quantity
            else:
                invested_value += pos.buy_price * pos.quantity

        total_asset = cash + invested_value
        prev_asset = daily_snapshots[-1].total_asset if daily_snapshots else config.initial_capital
        daily_return = ((total_asset - prev_asset) / prev_asset) * 100 if prev_asset > 0 else 0
        cum_return = ((total_asset - config.initial_capital) / config.initial_capital) * 100

        if total_asset > peak_asset:
            peak_asset = total_asset

        daily_snapshots.append(DailySnapshot(
            date=current_date,
            total_asset=round(total_asset),
            cash=round(cash),
            invested=round(invested_value),
            positions_count=len(positions),
            daily_return_pct=round(daily_return, 2),
            cumulative_return_pct=round(cum_return, 2),
        ))

    # ── 시뮬레이션 종료: 잔여 포지션 강제 청산 ──
    if positions and timeline:
        last_date = timeline[-1]['date']
        for pos in positions:
            candles = candles_by_code.get(pos.code, [])
            last_candle = _find_candle_by_date(candles, last_date)
            if last_candle:
                close = last_candle.close if hasattr(last_candle, 'close') else last_candle['close']
            else:
                close = pos.buy_price
            trade = _execute_sell(pos, close, last_date, "simulation_end")
            completed_trades.append(trade)
            cash += (close * pos.quantity) - trade.commission - trade.tax

    if progress_callback:
        progress_callback(90, "성과 분석 중...")

    # ── 결과 집계 ──
    result = _compile_results(completed_trades, daily_snapshots, config, peak_asset, cash)

    if progress_callback:
        progress_callback(100, "시뮬레이션 완료!")

    return result


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 내부 헬퍼 함수들 / Internal Helper Functions
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _extract_cluster_patterns(clusters) -> List[Dict]:
    """클러스터에서 비교용 패턴 벡터 추출"""
    patterns = []
    for cluster in clusters:
        if hasattr(cluster, 'avg_return_flow'):
            avg_returns = cluster.avg_return_flow
            avg_volumes = cluster.avg_volume_flow
            cid = cluster.cluster_id
        elif isinstance(cluster, dict):
            avg_returns = cluster.get('avg_return_flow', [])
            avg_volumes = cluster.get('avg_volume_flow', [])
            cid = cluster.get('cluster_id', 0)
        else:
            continue

        if avg_returns and len(avg_returns) >= 3:
            patterns.append({
                'cluster_id': cid,
                'avg_returns': avg_returns,
                'avg_volumes': avg_volumes,
            })

    return patterns


def _build_timeline(candles_by_code: Dict) -> List[Dict]:
    """모든 종목의 거래일을 통합한 타임라인 구성"""
    all_dates = set()
    for code, candles in candles_by_code.items():
        for c in candles:
            d = c.date if hasattr(c, 'date') else c.get('date', '')
            if d:
                all_dates.add(d)

    sorted_dates = sorted(all_dates)
    return [{'date': d, 'idx': i} for i, d in enumerate(sorted_dates)]


def _find_candle_by_date(candles: List, target_date: str):
    """특정 날짜의 캔들 찾기 (이진 검색)"""
    for c in candles:
        d = c.date if hasattr(c, 'date') else c.get('date', '')
        if d == target_date:
            return c
    return None


def _get_candle_value(candle, field: str, default=0):
    """캔들에서 값 추출 (dataclass 또는 dict 대응)"""
    if hasattr(candle, field):
        return getattr(candle, field)
    elif isinstance(candle, dict):
        return candle.get(field, default)
    return default


def _scan_buy_signals(
    candles_by_code: Dict,
    names: Dict,
    day_idx: int,
    current_date: str,
    cluster_patterns: List[Dict],
    pre_days: int,
    threshold: float,
) -> List[Dict]:
    """
    현재일 기준으로 각 종목의 최근 N일 패턴과 클러스터 패턴을
    DTW로 비교하여 매수 신호 생성
    """
    signals = []

    for code, candles in candles_by_code.items():
        # 현재일 인덱스 찾기
        curr_idx = -1
        for i, c in enumerate(candles):
            d = c.date if hasattr(c, 'date') else c.get('date', '')
            if d == current_date:
                curr_idx = i
                break

        if curr_idx < 0 or curr_idx < pre_days + 5:
            continue

        # 최근 pre_days일 등락률 계산
        recent_returns = []
        recent_volumes = []
        for k in range(pre_days):
            idx = curr_idx - pre_days + k
            if idx < 1:
                continue

            c_now = candles[idx]
            c_prev = candles[idx - 1]

            close_now = _get_candle_value(c_now, 'close')
            close_prev = _get_candle_value(c_prev, 'close')
            vol_now = _get_candle_value(c_now, 'volume', 1)

            if close_prev > 0:
                ret = ((close_now - close_prev) / close_prev) * 100
            else:
                ret = 0
            recent_returns.append(round(ret, 4))

            # 거래량 비율 (20일 평균 대비)
            vol_start = max(0, idx - 20)
            vol_slice = candles[vol_start:idx]
            avg_vol = sum(_get_candle_value(v, 'volume', 1) for v in vol_slice) / max(1, len(vol_slice))
            vol_ratio = round(vol_now / avg_vol, 4) if avg_vol > 0 else 1.0
            recent_volumes.append(vol_ratio)

        if len(recent_returns) < 5:
            continue

        # 각 클러스터와 DTW 유사도 계산
        best_sim = 0
        best_cluster_id = 0

        for cp in cluster_patterns:
            sim_r = dtw_similarity(recent_returns, cp['avg_returns'])
            sim_v = dtw_similarity(recent_volumes, cp['avg_volumes'])
            sim = sim_r * 0.6 + sim_v * 0.4

            if sim > best_sim:
                best_sim = sim
                best_cluster_id = cp['cluster_id']

        if best_sim >= threshold:
            # 다음 거래일 시가로 매수 (현재일 다음 캔들)
            next_idx = curr_idx + 1
            if next_idx < len(candles):
                next_candle = candles[next_idx]
                buy_price = _get_candle_value(next_candle, 'open')
                buy_date = _get_candle_value(next_candle, 'date', current_date)

                if buy_price > 0:
                    signals.append({
                        'code': code,
                        'name': names.get(code, code),
                        'similarity': best_sim,
                        'cluster_id': best_cluster_id,
                        'buy_price': buy_price,
                        'buy_date': buy_date,
                        'signal_date': current_date,
                    })

    return signals


def _execute_sell(pos: Position, sell_price: float, sell_date: str, exit_reason: str) -> Trade:
    """매도 실행 및 Trade 기록 생성"""
    gross_amount = sell_price * pos.quantity
    commission = (pos.invested + gross_amount) * COMMISSION_RATE  # 매수+매도 수수료
    tax = gross_amount * SELL_TAX_RATE                             # 매도세
    net_profit = gross_amount - pos.invested - commission - tax
    profit_pct = (net_profit / pos.invested) * 100 if pos.invested > 0 else 0

    return Trade(
        code=pos.code,
        name=pos.name,
        buy_date=pos.buy_date,
        buy_price=pos.buy_price,
        sell_date=sell_date,
        sell_price=round(sell_price),
        quantity=pos.quantity,
        invested=round(pos.invested),
        profit=round(net_profit),
        profit_pct=round(profit_pct, 2),
        hold_days=pos.hold_days,
        exit_reason=exit_reason,
        commission=round(commission),
        tax=round(tax),
        similarity=pos.similarity,
        cluster_id=pos.cluster_id,
    )


def _compile_results(
    trades: List[Trade],
    snapshots: List[DailySnapshot],
    config: SimConfig,
    peak_asset: float,
    final_cash: float,
) -> SimResult:
    """전체 결과 집계"""

    # 기본 통계
    total = len(trades)
    wins = [t for t in trades if t.profit > 0]
    losses = [t for t in trades if t.profit <= 0]
    win_count = len(wins)
    lose_count = len(losses)

    win_rate = (win_count / total * 100) if total > 0 else 0
    avg_profit = (sum(t.profit_pct for t in wins) / win_count) if win_count > 0 else 0
    avg_loss = (sum(t.profit_pct for t in losses) / lose_count) if lose_count > 0 else 0
    profit_loss_ratio = abs(avg_profit / avg_loss) if avg_loss != 0 else 0

    total_profit = sum(t.profit for t in trades)
    total_commission = sum(t.commission for t in trades)
    total_tax = sum(t.tax for t in trades)
    avg_hold = (sum(t.hold_days for t in trades) / total) if total > 0 else 0

    final_asset = snapshots[-1].total_asset if snapshots else config.initial_capital
    total_return = ((final_asset - config.initial_capital) / config.initial_capital) * 100

    # MDD 계산
    max_drawdown = 0
    running_peak = config.initial_capital
    for snap in snapshots:
        if snap.total_asset > running_peak:
            running_peak = snap.total_asset
        drawdown = ((running_peak - snap.total_asset) / running_peak) * 100
        if drawdown > max_drawdown:
            max_drawdown = drawdown

    # 매도 사유 통계
    exit_stats = {}
    for t in trades:
        exit_stats[t.exit_reason] = exit_stats.get(t.exit_reason, 0) + 1

    # 종목별 성과
    stock_perf = {}
    for t in trades:
        if t.code not in stock_perf:
            stock_perf[t.code] = {
                'code': t.code, 'name': t.name,
                'trades': 0, 'wins': 0, 'total_profit': 0,
                'total_profit_pct': 0,
            }
        sp = stock_perf[t.code]
        sp['trades'] += 1
        if t.profit > 0:
            sp['wins'] += 1
        sp['total_profit'] += t.profit
        sp['total_profit_pct'] += t.profit_pct

    stock_performance = sorted(stock_perf.values(), key=lambda s: s['total_profit'], reverse=True)

    # 월별 성과
    monthly = {}
    for t in trades:
        month_key = t.sell_date[:7] if t.sell_date else t.buy_date[:7]
        if month_key not in monthly:
            monthly[month_key] = {
                'month': month_key, 'trades': 0, 'wins': 0,
                'profit': 0, 'profit_pct': 0,
            }
        mp = monthly[month_key]
        mp['trades'] += 1
        if t.profit > 0:
            mp['wins'] += 1
        mp['profit'] += t.profit
        mp['profit_pct'] += t.profit_pct

    monthly_performance = sorted(monthly.values(), key=lambda m: m['month'])

    # 패턴(클러스터)별 성과
    cluster_perf = {}
    for t in trades:
        cid = t.cluster_id
        if cid not in cluster_perf:
            cluster_perf[cid] = {
                'cluster_id': cid, 'trades': 0, 'wins': 0,
                'total_profit': 0, 'avg_profit_pct': 0,
            }
        cp = cluster_perf[cid]
        cp['trades'] += 1
        if t.profit > 0:
            cp['wins'] += 1
        cp['total_profit'] += t.profit
        cp['avg_profit_pct'] += t.profit_pct

    for cp in cluster_perf.values():
        if cp['trades'] > 0:
            cp['avg_profit_pct'] = round(cp['avg_profit_pct'] / cp['trades'], 2)
            cp['win_rate'] = round(cp['wins'] / cp['trades'] * 100, 1)

    pattern_performance = sorted(cluster_perf.values(), key=lambda p: p['total_profit'], reverse=True)

    return SimResult(
        total_trades=total,
        win_count=win_count,
        lose_count=lose_count,
        win_rate=round(win_rate, 1),
        avg_profit_pct=round(avg_profit, 2),
        avg_loss_pct=round(avg_loss, 2),
        profit_loss_ratio=round(profit_loss_ratio, 2),
        total_return_pct=round(total_return, 2),
        max_drawdown_pct=round(max_drawdown, 2),
        avg_hold_days=round(avg_hold, 1),
        total_profit=round(total_profit),
        total_commission=round(total_commission),
        total_tax=round(total_tax),
        final_capital=round(final_asset),
        pattern_performance=pattern_performance,
        trades=[asdict(t) for t in trades],
        daily_snapshots=[asdict(s) for s in snapshots],
        config=asdict(config),
        stock_performance=stock_performance,
        monthly_performance=monthly_performance,
        exit_stats=exit_stats,
    )


def _empty_result(config: SimConfig) -> SimResult:
    """빈 결과 반환"""
    return SimResult(
        final_capital=config.initial_capital,
        config=asdict(config),
    )
