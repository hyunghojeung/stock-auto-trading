import { useState, useEffect, useRef, useCallback } from "react";

const API = "https://web-production-139e9.up.railway.app";

function fmt(n) {
  if (n == null || isNaN(n)) return "—";
  return Number(n).toLocaleString("ko-KR");
}

// ── 스타일 상수 ──
const S = {
  card: { background: "linear-gradient(135deg,rgba(25,35,65,0.95),rgba(15,22,48,0.98))", border: "1px solid rgba(100,140,200,0.15)", borderRadius: 12, padding: 16, marginBottom: 16 },
  cardTitle: { color: "#e0e6f0", fontWeight: 600, fontSize: 15, marginBottom: 12, display: "flex", justifyContent: "space-between", alignItems: "center" },
  badge: (ok) => ({ padding: "3px 10px", borderRadius: 10, fontSize: 11, fontWeight: 600, background: ok ? "rgba(76,255,139,0.15)" : "rgba(255,100,100,0.15)", color: ok ? "#4cff8b" : "#ff6464" }),
  btn: (color = "#1a3a6e") => ({ padding: "8px 16px", background: `linear-gradient(135deg,${color},${color}cc)`, color: "#e0e6f0", border: "none", borderRadius: 8, fontSize: 13, fontWeight: 600, cursor: "pointer" }),
  btnSm: (color = "#1a3a6e") => ({ padding: "5px 12px", background: `linear-gradient(135deg,${color},${color}cc)`, color: "#e0e6f0", border: "none", borderRadius: 6, fontSize: 12, fontWeight: 500, cursor: "pointer" }),
  input: { background: "rgba(10,18,40,0.8)", border: "1px solid rgba(100,140,200,0.2)", borderRadius: 8, color: "#e0e6f0", padding: "8px 12px", fontSize: 13, outline: "none", width: "100%" },
  select: { background: "rgba(10,18,40,0.8)", border: "1px solid rgba(100,140,200,0.2)", borderRadius: 8, color: "#e0e6f0", padding: "8px 12px", fontSize: 13, outline: "none" },
  th: { padding: "8px 6px", color: "#6688aa", textAlign: "left", fontSize: 12, borderBottom: "1px solid rgba(100,140,200,0.15)" },
  td: { padding: "8px 6px", fontSize: 12, borderBottom: "1px solid rgba(100,140,200,0.08)" },
  num: { textAlign: "right", fontFamily: "'JetBrains Mono',monospace" },
  row: { display: "flex", gap: 10, marginBottom: 10, alignItems: "center" },
  grid2: { display: "grid", gridTemplateColumns: "1fr 1fr", gap: 16 },
  summaryCard: { background: "rgba(10,18,40,0.6)", borderRadius: 8, padding: 12, textAlign: "center" },
  tab: (active) => ({ flex: 1, padding: "8px 12px", textAlign: "center", borderRadius: 6, cursor: "pointer", fontSize: 13, fontWeight: active ? 600 : 400, background: active ? "rgba(26,58,110,0.6)" : "transparent", color: active ? "#64b5f6" : "#6688aa", border: active ? "1px solid rgba(100,140,200,0.3)" : "1px solid transparent", transition: "all 0.2s" }),
  profit: (v) => ({ color: v > 0 ? "#ff6b6b" : v < 0 ? "#64b5f6" : "#6688aa" }),
};

function Toast({ msg, type }) {
  if (!msg) return null;
  const colors = { success: "#4cff8b", error: "#ff6464", info: "#64b5f6" };
  return <div style={{ position: "fixed", top: 20, right: 20, padding: "12px 20px", borderRadius: 8, fontSize: 13, fontWeight: 600, zIndex: 200, background: `${colors[type] || colors.info}22`, color: colors[type] || colors.info, border: `1px solid ${colors[type] || colors.info}44` }}>{msg}</div>;
}

// ── 메인 컴포넌트 ──
export default function TradingDashboard() {
  const [toast, setToast] = useState({ msg: "", type: "info" });
  const [pw, setPw] = useState("");

  const showToast = useCallback((msg, type = "info") => {
    setToast({ msg, type });
    setTimeout(() => setToast({ msg: "", type: "info" }), 3000);
  }, []);

  const promptPw = useCallback(() => {
    if (pw) return pw;
    const p = prompt("비밀번호를 입력하세요") || "";
    if (p) setPw(p);
    return p;
  }, [pw]);

  return (
    <div>
      <Toast msg={toast.msg} type={toast.type} />
      <KisStatusCard />
      <div style={S.grid2}>
        <StrategyCard promptPw={promptPw} showToast={showToast} />
        <BalanceCard />
      </div>
      <SseMonitorCard />
      <div style={S.grid2}>
        <OrderCard promptPw={promptPw} showToast={showToast} />
        <OrderHistoryCard />
      </div>
      <LiveKeyCard promptPw={promptPw} showToast={showToast} />
      <TradeHistoryCard />
    </div>
  );
}

// ── KIS 연결 상태 ──
function KisStatusCard() {
  const [paper, setPaper] = useState({ connected: false, msg: "확인중..." });
  const [live, setLive] = useState({ connected: false, msg: "확인중..." });
  const [market, setMarket] = useState("확인중");

  const check = useCallback(async () => {
    for (const isLive of [false, true]) {
      try {
        const r = await fetch(`${API}/api/kis/status?is_live=${isLive}`);
        const d = await r.json();
        const s = { connected: d.connected, msg: d.connected ? `연결됨 | 계좌: ${d.account}` : (d.error || "연결 실패") };
        isLive ? setLive(s) : setPaper(s);
      } catch { (isLive ? setLive : setPaper)({ connected: false, msg: "서버 연결 실패" }); }
    }
    try {
      const r = await fetch(`${API}/api/system/status`);
      const d = await r.json();
      setMarket(d.is_market_open ? "장중" : "장외");
    } catch {}
  }, []);

  useEffect(() => { check(); const t = setInterval(check, 30000); return () => clearInterval(t); }, [check]);

  const Dot = ({ on }) => <div style={{ width: 10, height: 10, borderRadius: "50%", background: on ? "#4cff8b" : "#ff6464", boxShadow: on ? "0 0 6px #4cff8b" : "none", flexShrink: 0 }} />;

  return (
    <div style={S.card}>
      <div style={S.cardTitle}>
        <span>🔗 KIS 증권 연결 상태</span>
        <div style={{ display: "flex", gap: 8 }}>
          <span style={S.badge(market === "장중")}>{market}</span>
          <button style={S.btnSm()} onClick={check}>새로고침</button>
        </div>
      </div>
      <div style={S.grid2}>
        {[{ label: "모의투자", s: paper }, { label: "실전투자", s: live }].map(({ label, s }) => (
          <div key={label} style={{ display: "flex", gap: 12, alignItems: "center", padding: 12, background: "rgba(10,18,40,0.5)", borderRadius: 8 }}>
            <Dot on={s.connected} />
            <div>
              <div style={{ fontWeight: 600, fontSize: 14, color: "#e0e6f0" }}>{label}</div>
              <div style={{ fontSize: 12, color: "#6688aa" }}>{s.msg}</div>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}

// ── 전략 관리 ──
function StrategyCard({ promptPw, showToast }) {
  const [list, setList] = useState([]);
  const [showModal, setShowModal] = useState(false);
  const [form, setForm] = useState({ name: "", is_live: false, initial_capital: 1000000, stop_loss_pct: -3, atr_multiplier: 2 });

  const load = useCallback(async () => {
    try { const r = await fetch(`${API}/api/strategy/`); setList(await r.json()); } catch {}
  }, []);

  useEffect(() => { load(); const t = setInterval(load, 30000); return () => clearInterval(t); }, [load]);

  const act = async (url, method = "POST") => {
    const p = promptPw(); if (!p) return;
    try {
      const r = await fetch(`${url}${url.includes("?") ? "&" : "?"}password=${encodeURIComponent(p)}`, { method });
      const d = await r.json();
      if (r.ok) { showToast(d.message || "완료", "success"); load(); }
      else showToast(d.detail || "실패", "error");
    } catch { showToast("서버 오류", "error"); }
  };

  const create = async () => {
    const p = promptPw(); if (!p) return;
    if (!form.name) { showToast("전략 이름을 입력하세요", "error"); return; }
    try {
      const r = await fetch(`${API}/api/strategy/?password=${encodeURIComponent(p)}`, {
        method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(form)
      });
      const d = await r.json();
      if (r.ok) { showToast(`전략 '${d.name}' 생성`, "success"); setShowModal(false); load(); }
      else showToast(d.detail || "실패", "error");
    } catch { showToast("서버 오류", "error"); }
  };

  return (
    <div style={S.card}>
      <div style={S.cardTitle}>
        <span>🎯 매매 전략 관리</span>
        <button style={S.btnSm("#2a5098")} onClick={() => setShowModal(true)}>+ 전략 생성</button>
      </div>
      {list.length === 0 ? <div style={{ color: "#556677", textAlign: "center", padding: 20 }}>등록된 전략이 없습니다</div>
        : list.map(s => (
          <div key={s.id} style={{ display: "flex", justifyContent: "space-between", alignItems: "center", padding: 12, background: "rgba(10,18,40,0.4)", borderRadius: 8, marginBottom: 8 }}>
            <div style={{ flex: 1 }}>
              <div style={{ fontWeight: 600, fontSize: 14, color: "#e0e6f0" }}>
                {s.name}{" "}
                <span style={{ ...S.badge(false), background: s.is_live ? "rgba(255,100,100,0.15)" : "rgba(100,140,200,0.15)", color: s.is_live ? "#ff6464" : "#64b5f6" }}>{s.is_live ? "실전" : "모의"}</span>{" "}
                <span style={S.badge(s.is_active)}>{s.is_active ? "매매중" : "중지"}</span>
              </div>
              <div style={{ fontSize: 12, color: "#556677", marginTop: 2 }}>자금 {fmt(s.initial_capital)}원 | 손절 {s.stop_loss_pct}% | ATR x{s.atr_multiplier}</div>
            </div>
            <div style={{ display: "flex", gap: 6 }}>
              {s.is_active
                ? <button style={S.btnSm("#8b0000")} onClick={() => { if (confirm("매매를 중지하시겠습니까?")) act(`${API}/api/strategy/${s.id}/stop`); }}>중지</button>
                : <button style={S.btnSm("#0a6b35")} onClick={() => act(`${API}/api/strategy/${s.id}/start`)}>시작</button>}
              <button style={S.btnSm("#4a2080")} disabled={s.is_active} onClick={() => { if (confirm("모드를 전환하시겠습니까?")) act(`${API}/api/strategy/${s.id}/toggle-live`); }}>{s.is_live ? "모의전환" : "실전전환"}</button>
              <button style={{ ...S.btnSm("#3a0000"), opacity: s.is_active ? 0.4 : 1 }} disabled={s.is_active} onClick={() => { if (confirm("정말 삭제?")) act(`${API}/api/strategy/${s.id}`, "DELETE"); }}>삭제</button>
            </div>
          </div>
        ))}
      {showModal && (
        <div style={{ position: "fixed", top: 0, left: 0, right: 0, bottom: 0, background: "rgba(0,0,0,0.6)", zIndex: 100, display: "flex", alignItems: "center", justifyContent: "center" }} onClick={() => setShowModal(false)}>
          <div style={{ ...S.card, width: 400, maxWidth: "90vw" }} onClick={e => e.stopPropagation()}>
            <div style={{ color: "#e0e6f0", fontWeight: 600, fontSize: 16, marginBottom: 16 }}>새 전략 생성</div>
            {[["이름", "name", "text"], ["모드", "is_live", "select"], ["초기자금", "initial_capital", "number"], ["손절률(%)", "stop_loss_pct", "number"], ["ATR배수", "atr_multiplier", "number"]].map(([label, key, type]) => (
              <div key={key} style={S.row}>
                <label style={{ minWidth: 80, fontSize: 13, color: "#6688aa" }}>{label}</label>
                {type === "select" ? <select style={S.select} value={form[key]} onChange={e => setForm({ ...form, [key]: e.target.value === "true" })}><option value="false">모의투자</option><option value="true">실전투자</option></select>
                  : <input style={S.input} type={type} value={form[key]} onChange={e => setForm({ ...form, [key]: type === "number" ? Number(e.target.value) : e.target.value })} />}
              </div>
            ))}
            <div style={{ display: "flex", gap: 10, justifyContent: "flex-end", marginTop: 16 }}>
              <button style={S.btnSm("#333")} onClick={() => setShowModal(false)}>취소</button>
              <button style={S.btn("#2a5098")} onClick={create}>생성</button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

// ── 계좌 잔고 ──
function BalanceCard() {
  const [tab, setTab] = useState(false);
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(false);

  const load = useCallback(async () => {
    setLoading(true);
    try {
      const r = await fetch(`${API}/api/kis/balance?is_live=${tab}`);
      setData(await r.json());
    } catch { setData(null); }
    setLoading(false);
  }, [tab]);

  useEffect(() => { load(); }, [load]);

  const s = data?.summary || {};
  return (
    <div style={S.card}>
      <div style={S.cardTitle}>
        <span>💰 KIS 계좌 잔고</span>
        <button style={S.btnSm()} onClick={load}>새로고침</button>
      </div>
      <div style={{ display: "flex", gap: 4, marginBottom: 12, background: "rgba(10,18,40,0.5)", borderRadius: 8, padding: 4 }}>
        {[false, true].map(v => <div key={String(v)} style={S.tab(tab === v)} onClick={() => setTab(v)}>{v ? "실전투자" : "모의투자"}</div>)}
      </div>
      {loading ? <div style={{ textAlign: "center", padding: 20, color: "#556677" }}>로딩중...</div> : !data?.success ?
        <div style={{ textAlign: "center", padding: 20, color: "#ff6464" }}>{data?.error || "조회 실패"}</div> : <>
          <div style={{ display: "grid", gridTemplateColumns: "repeat(4,1fr)", gap: 8, marginBottom: 12 }}>
            {[["총평가", fmt(s.total_eval), "#e0e6f0"], ["예수금", fmt(s.deposit), "#64b5f6"], ["총손익", fmt(s.total_profit), s.total_profit >= 0 ? "#ff6b6b" : "#64b5f6"], ["수익률", `${s.total_profit_pct || 0}%`, s.total_profit_pct >= 0 ? "#ff6b6b" : "#64b5f6"]].map(([l, v, c]) => (
              <div key={l} style={S.summaryCard}><div style={{ fontSize: 11, color: "#556677" }}>{l}</div><div style={{ fontSize: 16, fontWeight: 700, color: c, marginTop: 4, fontFamily: "'JetBrains Mono',monospace" }}>{v}</div></div>
            ))}
          </div>
          <div style={{ maxHeight: 200, overflowY: "auto" }}>
            <table style={{ width: "100%", borderCollapse: "collapse" }}>
              <thead><tr>{["종목", "현재가", "수량", "손익률"].map(h => <th key={h} style={S.th}>{h}</th>)}</tr></thead>
              <tbody>{(data?.holdings || []).length === 0 ? <tr><td colSpan={4} style={{ ...S.td, color: "#556677", textAlign: "center" }}>보유종목 없음</td></tr>
                : (data.holdings || []).map((h, i) => (
                  <tr key={i}>
                    <td style={S.td}>{h.name}<br /><span style={{ color: "#556677" }}>{h.code}</span></td>
                    <td style={{ ...S.td, ...S.num }}>{fmt(h.current_price)}</td>
                    <td style={{ ...S.td, ...S.num }}>{fmt(h.quantity)}</td>
                    <td style={{ ...S.td, ...S.num, ...S.profit(h.profit_pct) }}>{h.profit_pct}%<br /><span style={{ fontSize: 11 }}>{fmt(h.profit_amt)}원</span></td>
                  </tr>
                ))}</tbody>
            </table>
          </div>
        </>}
    </div>
  );
}

// ── 실시간 시세 모니터 (SSE) ──
function SseMonitorCard() {
  const [codes, setCodes] = useState("");
  const [isLive, setIsLive] = useState(false);
  const [prices, setPrices] = useState([]);
  const [connected, setConnected] = useState(false);
  const [ts, setTs] = useState("");
  const sseRef = useRef(null);

  const start = useCallback(() => {
    if (!codes.trim()) return;
    stop();
    const url = `${API}/api/kis/sse/price?codes=${encodeURIComponent(codes)}&is_live=${isLive}&interval=3`;
    const es = new EventSource(url);
    es.addEventListener("connected", () => setConnected(true));
    es.addEventListener("price", (e) => {
      const d = JSON.parse(e.data);
      setTs(d.timestamp);
      setPrices(d.prices || []);
    });
    es.onerror = () => setConnected(false);
    sseRef.current = es;
  }, [codes, isLive]);

  const stop = useCallback(() => {
    if (sseRef.current) { sseRef.current.close(); sseRef.current = null; }
    setConnected(false);
  }, []);

  useEffect(() => () => stop(), [stop]);

  return (
    <div style={S.card}>
      <div style={S.cardTitle}>
        <span>📡 실시간 시세 모니터</span>
        <span style={S.badge(connected)}>{connected ? "연결됨" : "미연결"}</span>
      </div>
      <div style={{ ...S.row }}>
        <input style={{ ...S.input, flex: 3 }} placeholder="종목코드 (쉼표 구분: 005930,000660)" value={codes} onChange={e => setCodes(e.target.value)} />
        <select style={S.select} value={isLive} onChange={e => setIsLive(e.target.value === "true")}><option value="false">모의</option><option value="true">실전</option></select>
        <button style={S.btnSm("#0a6b35")} onClick={start}>시작</button>
        <button style={S.btnSm("#333")} onClick={stop}>중지</button>
      </div>
      {prices.length > 0 && (
        <div style={{ maxHeight: 200, overflowY: "auto" }}>
          <table style={{ width: "100%", borderCollapse: "collapse" }}>
            <thead><tr>{["종목", "현재가", "전일비", "등락률", "거래량", "고가", "저가"].map(h => <th key={h} style={S.th}>{h}</th>)}</tr></thead>
            <tbody>{prices.map(p => {
              const sign = p.change > 0 ? "+" : "";
              return (
                <tr key={p.code}>
                  <td style={S.td}>{p.name || p.code}<br /><span style={{ color: "#556677" }}>{p.code}</span></td>
                  <td style={{ ...S.td, ...S.num, fontWeight: 700 }}>{fmt(p.price)}</td>
                  <td style={{ ...S.td, ...S.num, ...S.profit(p.change) }}>{sign}{fmt(p.change)}</td>
                  <td style={{ ...S.td, ...S.num, ...S.profit(p.change) }}>{sign}{p.change_pct}%</td>
                  <td style={{ ...S.td, ...S.num }}>{fmt(p.volume)}</td>
                  <td style={{ ...S.td, ...S.num, color: "#ff6b6b" }}>{fmt(p.high)}</td>
                  <td style={{ ...S.td, ...S.num, color: "#64b5f6" }}>{fmt(p.low)}</td>
                </tr>
              );
            })}</tbody>
          </table>
        </div>
      )}
      {ts && <div style={{ fontSize: 11, color: "#556677", marginTop: 6 }}>마지막 갱신: {ts}</div>}
    </div>
  );
}

// ── 수동 주문 ──
function OrderCard({ promptPw, showToast }) {
  const [mode, setMode] = useState(false);
  const [code, setCode] = useState("");
  const [qty, setQty] = useState("");
  const [price, setPrice] = useState("");
  const [priceInfo, setPriceInfo] = useState("");

  const lookup = async () => {
    if (code.length !== 6) { showToast("종목코드 6자리", "error"); return; }
    try {
      const r = await fetch(`${API}/api/kis/price/${code}?is_live=${mode}`);
      if (!r.ok) { setPriceInfo("시세 조회 실패"); return; }
      const d = await r.json();
      const sign = d.change > 0 ? "+" : "";
      setPriceInfo(`${d.name} | 현재가: ${fmt(d.price)}원 (${sign}${d.change_pct}%) | 거래량: ${fmt(d.volume)} | PER: ${d.per} | PBR: ${d.pbr}`);
    } catch { setPriceInfo("서버 오류"); }
  };

  const order = async (type) => {
    const p = promptPw(); if (!p) return;
    if (code.length !== 6) { showToast("종목코드 6자리", "error"); return; }
    const q = parseInt(qty) || 0;
    if (q <= 0) { showToast("수량을 입력하세요", "error"); return; }
    const pr = parseInt(price) || 0;
    const action = type === "buy" ? "매수" : "매도";
    const modeStr = mode ? "실전" : "모의";
    if (!confirm(`[${modeStr}] ${code} ${q}주 ${action} 주문하시겠습니까?`)) return;
    try {
      const r = await fetch(`${API}/api/kis/order/${type}`, {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ code, quantity: q, price: pr, is_live: mode, password: p })
      });
      const d = await r.json();
      if (d.success) showToast(`${action} 주문 성공!`, "success");
      else showToast(`${action} 실패: ${d.error || d.message || "알 수 없는 오류"}`, "error");
    } catch { showToast("서버 오류", "error"); }
  };

  return (
    <div style={S.card}>
      <div style={S.cardTitle}><span>📝 수동 주문</span></div>
      <div style={S.row}><label style={{ minWidth: 70, fontSize: 13, color: "#6688aa" }}>모드</label><select style={S.select} value={mode} onChange={e => setMode(e.target.value === "true")}><option value="false">모의투자</option><option value="true">실전투자</option></select></div>
      <div style={S.row}>
        <label style={{ minWidth: 70, fontSize: 13, color: "#6688aa" }}>종목코드</label>
        <input style={{ ...S.input, flex: 1 }} placeholder="예: 005930" maxLength={6} value={code} onChange={e => setCode(e.target.value)} />
        <button style={S.btnSm()} onClick={lookup}>시세조회</button>
      </div>
      {priceInfo && <div style={{ fontSize: 12, color: "#6688aa", marginLeft: 80, marginBottom: 8 }}>{priceInfo}</div>}
      <div style={S.row}><label style={{ minWidth: 70, fontSize: 13, color: "#6688aa" }}>수량</label><input style={{ ...S.input, flex: 1 }} type="number" placeholder="수량" min="1" value={qty} onChange={e => setQty(e.target.value)} /></div>
      <div style={S.row}><label style={{ minWidth: 70, fontSize: 13, color: "#6688aa" }}>가격</label><input style={{ ...S.input, flex: 1 }} type="number" placeholder="0 = 시장가" min="0" value={price} onChange={e => setPrice(e.target.value)} /></div>
      <div style={{ display: "flex", gap: 10, marginTop: 12 }}>
        <button style={{ ...S.btn("#8b0000"), flex: 1 }} onClick={() => order("buy")}>매수 주문</button>
        <button style={{ ...S.btn("#1a3a6e"), flex: 1 }} onClick={() => order("sell")}>매도 주문</button>
      </div>
    </div>
  );
}

// ── 당일 체결내역 ──
function OrderHistoryCard() {
  const [tab, setTab] = useState(false);
  const [orders, setOrders] = useState([]);

  const load = useCallback(async () => {
    try {
      const r = await fetch(`${API}/api/kis/orders/today?is_live=${tab}`);
      const d = await r.json();
      setOrders(d.success ? d.orders : []);
    } catch { setOrders([]); }
  }, [tab]);

  useEffect(() => { load(); }, [load]);

  return (
    <div style={S.card}>
      <div style={S.cardTitle}><span>📋 당일 체결내역</span><button style={S.btnSm()} onClick={load}>새로고침</button></div>
      <div style={{ display: "flex", gap: 4, marginBottom: 12, background: "rgba(10,18,40,0.5)", borderRadius: 8, padding: 4 }}>
        {[false, true].map(v => <div key={String(v)} style={S.tab(tab === v)} onClick={() => setTab(v)}>{v ? "실전투자" : "모의투자"}</div>)}
      </div>
      <div style={{ maxHeight: 280, overflowY: "auto" }}>
        <table style={{ width: "100%", borderCollapse: "collapse" }}>
          <thead><tr>{["시간", "종목", "구분", "체결가", "수량", "상태"].map(h => <th key={h} style={S.th}>{h}</th>)}</tr></thead>
          <tbody>{orders.length === 0 ? <tr><td colSpan={6} style={{ ...S.td, color: "#556677", textAlign: "center" }}>체결내역 없음</td></tr>
            : orders.map((o, i) => (
              <tr key={i}>
                <td style={S.td}>{o.order_time ? `${o.order_time.substring(0, 2)}:${o.order_time.substring(2, 4)}:${o.order_time.substring(4, 6)}` : "—"}</td>
                <td style={S.td}>{o.name}<br /><span style={{ color: "#556677" }}>{o.code}</span></td>
                <td style={{ ...S.td, color: o.order_type === "매수" ? "#ff6b6b" : "#64b5f6" }}>{o.order_type}</td>
                <td style={{ ...S.td, ...S.num }}>{fmt(o.filled_price)}</td>
                <td style={{ ...S.td, ...S.num }}>{o.filled_qty}/{o.order_qty}</td>
                <td style={S.td}>{o.status}</td>
              </tr>
            ))}</tbody>
        </table>
      </div>
    </div>
  );
}

// ── 실전 키 설정 ──
function LiveKeyCard({ promptPw, showToast }) {
  const [keySet, setKeySet] = useState(false);
  const [appKey, setAppKey] = useState("");
  const [appSecret, setAppSecret] = useState("");
  const [cano, setCano] = useState("");

  const checkKeys = useCallback(async () => {
    try {
      const r = await fetch(`${API}/api/kis/config`);
      const d = await r.json();
      setKeySet(d.live?.app_key_set || false);
    } catch {}
  }, []);

  useEffect(() => { checkKeys(); }, [checkKeys]);

  const apply = async () => {
    const p = promptPw(); if (!p) return;
    if (!appKey || !appSecret) { showToast("App Key/Secret 입력 필요", "error"); return; }
    try {
      const r = await fetch(`${API}/api/kis/config/live`, {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ app_key: appKey, app_secret: appSecret, cano, acnt_prdt_cd: "01", password: p })
      });
      if (r.ok) { showToast("실전 키 적용 완료!", "success"); checkKeys(); }
      else { const d = await r.json(); showToast(d.detail || "실패", "error"); }
    } catch { showToast("서버 오류", "error"); }
  };

  const revoke = async (isLive) => {
    const p = promptPw(); if (!p) return;
    try {
      const r = await fetch(`${API}/api/kis/token/revoke?is_live=${isLive}&password=${encodeURIComponent(p)}`, { method: "POST" });
      const d = await r.json();
      showToast(d.message || "완료", d.success ? "success" : "error");
    } catch { showToast("서버 오류", "error"); }
  };

  return (
    <div style={S.card}>
      <div style={S.cardTitle}>
        <span>⚙️ 실전투자 API 설정</span>
        <span style={S.badge(keySet)}>{keySet ? "설정됨" : "미설정"}</span>
      </div>
      <div style={{ fontSize: 12, color: "#556677", marginBottom: 12 }}>실전투자 키를 입력하면 서버 재시작 없이 즉시 적용됩니다.</div>
      <div style={S.grid2}>
        <div>
          <div style={S.row}><label style={{ minWidth: 80, fontSize: 13, color: "#6688aa" }}>App Key</label><input style={S.input} type="password" placeholder="실전 App Key" value={appKey} onChange={e => setAppKey(e.target.value)} /></div>
          <div style={S.row}><label style={{ minWidth: 80, fontSize: 13, color: "#6688aa" }}>App Secret</label><input style={S.input} type="password" placeholder="실전 App Secret" value={appSecret} onChange={e => setAppSecret(e.target.value)} /></div>
        </div>
        <div>
          <div style={S.row}><label style={{ minWidth: 80, fontSize: 13, color: "#6688aa" }}>계좌번호</label><input style={S.input} placeholder="8자리" maxLength={8} value={cano} onChange={e => setCano(e.target.value)} /></div>
        </div>
      </div>
      <div style={{ display: "flex", gap: 10, marginTop: 8 }}>
        <button style={S.btn("#2a5098")} onClick={apply}>실전 키 적용</button>
        <button style={S.btnSm("#8b0000")} onClick={() => revoke(true)}>실전 토큰 폐기</button>
        <button style={S.btnSm("#333")} onClick={() => revoke(false)}>모의 토큰 폐기</button>
      </div>
    </div>
  );
}

// ── DB 매매기록 ──
function TradeHistoryCard() {
  const [trades, setTrades] = useState([]);

  const load = useCallback(async () => {
    try {
      const r = await fetch(`${API}/api/trades/?limit=30`);
      setTrades(await r.json());
    } catch {}
  }, []);

  useEffect(() => { load(); }, [load]);

  return (
    <div style={S.card}>
      <div style={S.cardTitle}><span>📊 시스템 매매 기록</span><button style={S.btnSm()} onClick={load}>새로고침</button></div>
      <div style={{ maxHeight: 300, overflowY: "auto" }}>
        <table style={{ width: "100%", borderCollapse: "collapse" }}>
          <thead><tr>{["시간", "전략", "종목", "구분", "매수가", "매도가", "수량", "순수익", "사유"].map(h => <th key={h} style={S.th}>{h}</th>)}</tr></thead>
          <tbody>{trades.length === 0 ? <tr><td colSpan={9} style={{ ...S.td, color: "#556677", textAlign: "center" }}>매매 기록 없음</td></tr>
            : trades.map((t, i) => {
              const time = t.traded_at ? new Date(t.traded_at).toLocaleString("ko-KR", { month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit" }) : "—";
              const isSell = t.trade_type === "sell";
              return (
                <tr key={i}>
                  <td style={S.td}>{time}</td>
                  <td style={S.td}>{t.strategy_id || "—"}</td>
                  <td style={S.td}>{t.stock_name || t.stock_code}</td>
                  <td style={{ ...S.td, color: isSell ? "#4cff8b" : "#ff6b6b" }}>{isSell ? "매도" : "매수"}</td>
                  <td style={{ ...S.td, ...S.num }}>{fmt(t.buy_price)}</td>
                  <td style={{ ...S.td, ...S.num }}>{isSell ? fmt(t.sell_price) : "—"}</td>
                  <td style={{ ...S.td, ...S.num }}>{fmt(t.quantity)}</td>
                  <td style={{ ...S.td, ...S.num, ...S.profit(t.net_profit || 0) }}>{isSell ? fmt(t.net_profit) : "—"}</td>
                  <td style={{ ...S.td, fontSize: 11, maxWidth: 100, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{t.trade_reason || "—"}</td>
                </tr>
              );
            })}</tbody>
        </table>
      </div>
    </div>
  );
}
