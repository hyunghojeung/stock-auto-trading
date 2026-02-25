/**
 * 급상승 패턴 탐지기 — DTW 기반
 * Pattern Surge Detector — DTW-based
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 * 파일경로: src/pages/PatternDetector.jsx
 *
 * [v3.1] 브라우저 이탈 후 재진입 시 스캔 자동 재개
 * - 페이지 이탈/재진입 시 진행 중인 스캔 자동 감지 & 폴링 재개
 * - scanIntervalRef로 인터벌 관리 (메모리 누수 방지)
 */

import React, { useState, useEffect, useRef, useCallback } from 'react';
import VirtualInvestTab from "./VirtualInvestTab";

const API_BASE = "https://web-production-139e9.up.railway.app";

const PRESETS = {
  bluechip: {
    key: 'bluechip', label: '🏢 우량주 모드',
    desc: '대형주 실적·눌림목 기반 급상승 탐지', color: '#3b82f6',
    params: { periodDays: 365, preRiseDays: 10, risePct: 30, riseWindow: 5,
              weightReturns: 0.5, weightCandle: 0.2, weightVolume: 0.3 },
  },
  manipulation: {
    key: 'manipulation', label: '⚡ 작전주 모드',
    desc: '세력 매집기 패턴 탐지 (거래량 중심)', color: '#ef4444',
    params: { periodDays: 730, preRiseDays: 30, risePct: 50, riseWindow: 10,
              weightReturns: 0.3, weightCandle: 0.2, weightVolume: 0.5 },
  },
  custom: {
    key: 'custom', label: '🔧 사용자 정의',
    desc: '직접 파라미터를 설정합니다', color: '#f59e0b', params: null,
  },
};

const COLORS = {
  bg: '#0a0e1a', card: '#111827', cardBorder: '#1e293b',
  accent: '#3b82f6', accentDim: 'rgba(59,130,246,0.15)',
  green: '#10b981', greenDim: 'rgba(16,185,129,0.15)',
  red: '#ef4444', redDim: 'rgba(239,68,68,0.15)',
  yellow: '#f59e0b', yellowDim: 'rgba(245,158,11,0.15)',
  purple: '#8b5cf6', purpleDim: 'rgba(139,92,246,0.15)',
  gray: '#6b7280', grayLight: '#9ca3af',
  text: '#e5e7eb', textDim: '#9ca3af', white: '#f9fafb',
};

const fmt = (n) => n?.toLocaleString() ?? '-';

export default function PatternDetector() {
  const [pageMode, setPageMode] = useState('scanner');

  // ━━━ 스캐너 상태 ━━━
  const [scanMarket, setScanMarket] = useState('ALL');
  const [scanPeriod, setScanPeriod] = useState(365);
  const [scanRisePct, setScanRisePct] = useState(30);
  const [scanRiseWindow, setScanRiseWindow] = useState(5);
  const [scanVolRatio, setScanVolRatio] = useState(2.0);
  const [scanning, setScanning] = useState(false);
  const [scanProgress, setScanProgress] = useState(0);
  const [scanMsg, setScanMsg] = useState('');
  const [scanResult, setScanResult] = useState(null);

  // ━━━ localStorage 캐시 헬퍼 (스캔 결과 보존) ━━━
  const saveScanCache = (data) => {
    try { localStorage.setItem('scanResultCache', JSON.stringify(data)); }
    catch (e) { console.log('스캔 캐시 저장 실패:', e); }
  };
  const loadScanCache = () => {
    try {
      const cached = localStorage.getItem('scanResultCache');
      if (cached) return JSON.parse(cached);
    } catch (e) { console.log('스캔 캐시 로드 실패:', e); }
    return null;
  };
  // setScanResult를 래핑하여 자동 캐시
  const setScanResultWithCache = (data) => {
    setScanResult(data);
    if (data && data.stocks) saveScanCache(data);
  };
  const [scanError, setScanError] = useState('');
  const [scanSortKey, setScanSortKey] = useState('manip_score');
  const [scanFilterLevel, setScanFilterLevel] = useState('all');
  const [selectedScanStocks, setSelectedScanStocks] = useState(new Set());
  const [scanDate, setScanDate] = useState('');
  const [scanSource, setScanSource] = useState('');
  const [loadingPrev, setLoadingPrev] = useState(false);

  // ━━━ [v3.1] 폴링 인터벌 ref — 언마운트 시 정리용 ━━━
  const scanIntervalRef = useRef(null);

  // ━━━ [v3.1] 페이지 진입 시: 진행 중인 스캔 확인 → 자동 재개 ━━━
  useEffect(() => {
    let cancelled = false;

    (async () => {
      // 0) localStorage에서 캐시된 스캔 결과 즉시 복원
      const cached = loadScanCache();
      if (cached && cached.stocks && !cancelled) {
        setScanResult(cached);
        setScanDate(cached.scan_date || '');
        setScanSource('cache');
      }

      // 1) 먼저 서버에 진행 중인 스캔이 있는지 확인
      try {
        const progResp = await fetch(`${API_BASE}/api/scanner/progress`);
        const progData = await progResp.json();

        if (cancelled) return;

        if (progData.running) {
          // ✅ 스캔이 아직 진행 중 → 상태 복원 & 폴링 재개
          setScanning(true);
          setScanProgress(progData.progress || 0);
          setScanMsg(progData.message || '스캔 진행 중...');
          pollScanProgress();
          return;
        }

        // 스캔이 끝났는데 결과가 메모리에 있는 경우
        if (!progData.running && progData.has_result) {
          try {
            const resResp = await fetch(`${API_BASE}/api/scanner/result`);
            const resData = await resResp.json();
            if (!cancelled && resData.status === 'done') {
              setScanResultWithCache(resData);
              setScanDate(resData.scan_date || new Date().toISOString());
              setScanSource('memory');
              return;
            }
          } catch (e) { console.log('메모리 결과 로드 실패:', e); }
        }
      } catch (e) {
        console.log('진행 상태 확인 실패:', e);
      }

      if (cancelled) return;

      // 2) 진행 중이 아니면 → DB에서 이전 결과 로드 (캐시가 없을 때만)
      if (!cached) {
        setLoadingPrev(true);
        try {
          const resp = await fetch(`${API_BASE}/api/scanner/latest`);
          const data = await resp.json();
          if (!cancelled && data.status === 'done') {
            setScanResultWithCache(data);
            setScanDate(data.scan_date || '');
            setScanSource(data.source || 'db');
          }
        } catch (e) { console.log('이전 스캔 로드 실패:', e); }
        if (!cancelled) setLoadingPrev(false);
      }
    })();

    return () => {
      cancelled = true;
      if (scanIntervalRef.current) {
        clearInterval(scanIntervalRef.current);
        scanIntervalRef.current = null;
      }
    };
  }, []);

  // ━━━ 분석기 상태 ━━━
  const [stocks, setStocks] = useState([]);
  const [searchKeyword, setSearchKeyword] = useState('');
  const [searchResults, setSearchResults] = useState([]);
  const [showDropdown, setShowDropdown] = useState(false);
  const searchRef = useRef(null);
  const searchTimerRef = useRef(null);
  const [activePreset, setActivePreset] = useState('bluechip');
  const [periodDays, setPeriodDays] = useState(365);
  const [preRiseDays, setPreRiseDays] = useState(10);
  const [risePct, setRisePct] = useState(30);
  const [riseWindow, setRiseWindow] = useState(5);
  const [weightReturns, setWeightReturns] = useState(0.5);
  const [weightCandle, setWeightCandle] = useState(0.2);
  const [weightVolume, setWeightVolume] = useState(0.3);
  const [showSettings, setShowSettings] = useState(false);
  const [analyzing, setAnalyzing] = useState(false);
  const [progress, setProgress] = useState(0);
  const [progressMsg, setProgressMsg] = useState('');
  const [result, setResult] = useState(null);
  const [error, setError] = useState('');
  const [activeTab, setActiveTab] = useState(0);

  const applyPreset = (presetKey) => {
    setActivePreset(presetKey);
    const preset = PRESETS[presetKey];
    if (preset.params) {
      setPeriodDays(preset.params.periodDays); setPreRiseDays(preset.params.preRiseDays);
      setRisePct(preset.params.risePct); setRiseWindow(preset.params.riseWindow);
      setWeightReturns(preset.params.weightReturns); setWeightCandle(preset.params.weightCandle);
      setWeightVolume(preset.params.weightVolume);
    }
    if (presetKey === 'custom') setShowSettings(true);
  };

  // ━━━ 스캐너 기능 ━━━
  const reloadScanFromDB = async () => {
    try {
      setScanSource('loading');
      const res = await fetch(`${API_BASE}/api/scanner/latest`);
      const data = await res.json();
      if (data.status === 'done' && data.stocks?.length > 0) {
        setScanResultWithCache(data);
        setScanDate(data.scan_date || '');
        setScanSource(data.source || 'db');
      } else {
        setScanSource('');
        alert('저장된 스캔 결과가 없습니다. 스캔을 먼저 실행해주세요.');
      }
    } catch (e) {
      console.error('DB 리로드 실패:', e);
      setScanSource('');
    }
  };

  const startScan = async () => {
    setScanError(''); setScanResult(null); setScanning(true);
    setScanProgress(0); setScanMsg('스캔 요청 중...'); setSelectedScanStocks(new Set());
    try {
      const resp = await fetch(`${API_BASE}/api/scanner/start`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          market: scanMarket, period_days: scanPeriod,
          rise_pct: scanRisePct, rise_window: scanRiseWindow, min_volume_ratio: scanVolRatio,
        }),
      });
      if (!resp.ok) { const err = await resp.json(); throw new Error(err.detail || '스캔 요청 실패'); }
      pollScanProgress();
    } catch (e) { setScanError(e.message); setScanning(false); }
  };

  const stopScan = async () => {
    try { await fetch(`${API_BASE}/api/scanner/stop`, { method: 'POST' }); setScanMsg('중지 요청됨...'); }
    catch (e) { console.error('중지 실패:', e); }
  };

  // ━━━ [v3.1] 폴링 — ref 기반 (이탈 후 재개 가능) ━━━
  const pollScanProgress = useCallback(() => {
    if (scanIntervalRef.current) {
      clearInterval(scanIntervalRef.current);
      scanIntervalRef.current = null;
    }

    scanIntervalRef.current = setInterval(async () => {
      try {
        const resp = await fetch(`${API_BASE}/api/scanner/progress`);
        const data = await resp.json();
        setScanProgress(data.progress || 0); setScanMsg(data.message || '');
        if (!data.running) {
          clearInterval(scanIntervalRef.current);
          scanIntervalRef.current = null;
          if (data.error) { setScanError(data.error); setScanning(false); }
          else if (data.has_result) {
            const resResp = await fetch(`${API_BASE}/api/scanner/result`);
            const resData = await resResp.json();
            if (resData.status === 'done') { setScanResultWithCache(resData); setScanDate(resData.scan_date || new Date().toISOString()); setScanSource('memory'); }
            else if (resData.status === 'error') setScanError(resData.error);
            setScanning(false);
          } else {
            setScanning(false);
          }
        }
      } catch (e) {
        clearInterval(scanIntervalRef.current);
        scanIntervalRef.current = null;
        setScanError('진행률 조회 실패');
        setScanning(false);
      }
    }, 1500);
  }, []);

  const sendToAnalyzer = () => {
    if (selectedScanStocks.size === 0) return;
    const scanStocks = scanResult?.stocks || [];
    const selected = scanStocks.filter(s => selectedScanStocks.has(s.code));
    setStocks(prev => {
      const merged = [...prev];
      for (const ns of selected) {
        if (!merged.find(s => s.code === ns.code) && merged.length < 20)
          merged.push({ code: ns.code, name: ns.name });
      }
      return merged;
    });
    applyPreset('manipulation');
    setPageMode('analyzer');
  };

  const getFilteredScanResults = () => {
    if (!scanResult?.stocks) return [];
    let list = [...scanResult.stocks];
    if (scanFilterLevel === 'high') list = list.filter(s => s.top_manip_level === 'high');
    else if (scanFilterLevel === 'medium') list = list.filter(s => s.top_manip_level === 'medium' || s.top_manip_level === 'high');
    if (scanSortKey === 'manip_score') list.sort((a, b) => b.top_manip_score - a.top_manip_score);
    else if (scanSortKey === 'rise_pct') list.sort((a, b) => b.latest_rise_pct - a.latest_rise_pct);
    else if (scanSortKey === 'date') list.sort((a, b) => (b.latest_surge_date||'').localeCompare(a.latest_surge_date||''));
    else if (scanSortKey === 'from_peak') list.sort((a, b) => a.latest_from_peak - b.latest_from_peak);
    return list;
  };

  const toggleScanStock = (code) => {
    setSelectedScanStocks(prev => {
      const next = new Set(prev);
      if (next.has(code)) next.delete(code); else if (next.size < 20) next.add(code);
      return next;
    });
  };

  const selectAllVisible = () => {
    setSelectedScanStocks(new Set(getFilteredScanResults().slice(0, 20).map(s => s.code)));
  };

  // ━━━ 분석기 기능 ━━━
  useEffect(() => {
    if (!searchKeyword.trim()) { setSearchResults([]); setShowDropdown(false); return; }
    if (searchTimerRef.current) clearTimeout(searchTimerRef.current);
    searchTimerRef.current = setTimeout(async () => {
      try {
        const resp = await fetch(`${API_BASE}/api/pattern/search`, {
          method: 'POST', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ keyword: searchKeyword.trim() }),
        });
        const data = await resp.json(); setSearchResults(data.results || []); setShowDropdown(true);
      } catch (e) { console.error('검색 실패:', e); }
    }, 300);
    return () => clearTimeout(searchTimerRef.current);
  }, [searchKeyword]);

  useEffect(() => {
    const handler = (e) => { if (searchRef.current && !searchRef.current.contains(e.target)) setShowDropdown(false); };
    document.addEventListener('mousedown', handler);
    return () => document.removeEventListener('mousedown', handler);
  }, []);

  const addStock = (code, name) => {
    if (stocks.find(s => s.code === code)) return;
    if (stocks.length >= 20) { alert('최대 20개까지 추가 가능합니다.'); return; }
    setStocks(prev => [...prev, { code, name }]); setSearchKeyword(''); setShowDropdown(false);
  };
  const removeStock = (code) => setStocks(prev => prev.filter(s => s.code !== code));

  const startAnalysis = async () => {
    if (stocks.length === 0) { setError('종목을 1개 이상 추가하세요.'); return; }
    setError(''); setResult(null); setAnalyzing(true); setProgress(0); setProgressMsg('분석 요청 중...');
    try {
      const names = {}; stocks.forEach(s => { names[s.code] = s.name; });
      const resp = await fetch(`${API_BASE}/api/pattern/analyze`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          codes: stocks.map(s => s.code), names, period_days: periodDays,
          pre_rise_days: preRiseDays, rise_pct: risePct, rise_window: riseWindow,
          weight_returns: weightReturns, weight_candle: weightCandle, weight_volume: weightVolume,
        }),
      });
      if (!resp.ok) { const err = await resp.json(); throw new Error(err.detail || '분석 요청 실패'); }
      pollProgress();
    } catch (e) { setError(e.message); setAnalyzing(false); }
  };

  const pollProgress = useCallback(() => {
    const interval = setInterval(async () => {
      try {
        const resp = await fetch(`${API_BASE}/api/pattern/progress`);
        const data = await resp.json(); setProgress(data.progress || 0); setProgressMsg(data.message || '');
        if (!data.running) {
          clearInterval(interval);
          if (data.error) { setError(data.error); setAnalyzing(false); }
          else if (data.has_result) {
            const resResp = await fetch(`${API_BASE}/api/pattern/result`);
            const resData = await resResp.json();
            if (resData.status === 'done') { setResult(resData); setActiveTab(0); }
            else if (resData.status === 'error') setError(resData.error);
            setAnalyzing(false);
          }
        }
      } catch (e) { clearInterval(interval); setError('진행률 조회 실패'); setAnalyzing(false); }
    }, 1000);
  }, []);

  const currentPreset = PRESETS[activePreset];

  // ━━━ 렌더링 ━━━
  return (
    <div style={{ minHeight:'100vh', background:COLORS.bg, color:COLORS.text,
      fontFamily:"'Pretendard',-apple-system,sans-serif", padding:'20px', maxWidth:1200, margin:'0 auto' }}>

      {/* 헤더 */}
      <div style={{ marginBottom:20 }}>
        <h1 style={{ fontSize:22, fontWeight:700, margin:0, display:'flex', alignItems:'center', gap:10 }}>
          <span style={{ fontSize:26 }}>🔍</span> 급상승 패턴 탐지기
          <span style={{ fontSize:11, background:COLORS.accentDim, color:COLORS.accent,
            padding:'3px 10px', borderRadius:20, fontWeight:500 }}>DTW Engine v3.1</span>
        </h1>
        <p style={{ color:COLORS.textDim, fontSize:13, marginTop:6 }}>
          전종목 급상승 스캔 → 작전주/세력주 발굴 → DTW 패턴 분석
        </p>
      </div>

      {/* 페이지 모드 탭 */}
      <div style={{ display:'flex', gap:4, marginBottom:20, background:COLORS.card, borderRadius:12,
        padding:4, border:`1px solid ${COLORS.cardBorder}` }}>
        {[
          { k:'scanner', l:'🚀 급상승 종목 발굴', c:COLORS.red, cd:COLORS.redDim },
          { k:'analyzer', l:'🔬 패턴 분석기', c:COLORS.accent, cd:COLORS.accentDim },
        ].map(m => (
          <button key={m.k} onClick={() => setPageMode(m.k)} style={{
            flex:1, padding:'12px 0', fontSize:14, fontWeight:700, border:'none', borderRadius:10,
            cursor:'pointer', background:pageMode===m.k?m.cd:'transparent',
            color:pageMode===m.k?m.c:COLORS.textDim, transition:'all 0.2s' }}>{m.l}</button>
        ))}
      </div>

      {/* ━━━ 페이지 1: 스캐너 ━━━ */}
      {pageMode === 'scanner' && (<div>
        <div style={{ background:COLORS.card, border:`1px solid ${COLORS.cardBorder}`,
          borderRadius:12, padding:20, marginBottom:16 }}>
          <div style={{ fontSize:15, fontWeight:700, marginBottom:16, color:COLORS.red }}>
            🚀 전종목 급상승 스캐너
          </div>
          <div style={{ display:'grid', gridTemplateColumns:'repeat(auto-fit, minmax(140px, 1fr))',
            gap:14, marginBottom:16 }}>
            <FilterGroup label="시장 선택" options={[{v:'ALL',l:'전체'},{v:'KOSPI',l:'KOSPI'},{v:'KOSDAQ',l:'KOSDAQ'}]}
              value={scanMarket} setter={setScanMarket} color={COLORS.red} disabled={scanning} />
            <FilterGroup label="조회 기간" options={[{v:180,l:'6개월'},{v:365,l:'1년'},{v:600,l:'2년'}]}
              value={scanPeriod} setter={setScanPeriod} color={COLORS.red} disabled={scanning} />
            <FilterGroup label="급상승 기준" options={[{v:20,l:'+20%'},{v:30,l:'+30%'},{v:50,l:'+50%'},{v:100,l:'+100%'}]}
              value={scanRisePct} setter={setScanRisePct} color={COLORS.red} disabled={scanning} />
            <FilterGroup label="상승 기간" options={[{v:3,l:'3일'},{v:5,l:'5일'},{v:10,l:'10일'}]}
              value={scanRiseWindow} setter={setScanRiseWindow} color={COLORS.red} disabled={scanning} />
            <FilterGroup label="최소 거래량 배율" options={[{v:1.5,l:'1.5배'},{v:2.0,l:'2배'},{v:3.0,l:'3배'},{v:5.0,l:'5배'}]}
              value={scanVolRatio} setter={setScanVolRatio} color={COLORS.red} disabled={scanning} />
          </div>
          <div style={{ display:'flex', gap:10 }}>
            {!scanning ? (
              <button onClick={startScan} style={{ padding:'12px 32px', fontSize:15, fontWeight:700,
                borderRadius:10, border:'none', cursor:'pointer',
                background:`linear-gradient(135deg, ${COLORS.red}, #dc2626)`, color:COLORS.white }}>
                🚀 전종목 스캔 시작</button>
            ) : (
              <button onClick={stopScan} style={{ padding:'12px 32px', fontSize:15, fontWeight:700,
                borderRadius:10, border:`2px solid ${COLORS.red}`, cursor:'pointer',
                background:'transparent', color:COLORS.red }}>⏹ 스캔 중지</button>
            )}
            <div style={{ fontSize:12, color:COLORS.textDim, alignSelf:'center' }}>
              {scanMarket==='ALL'?'전체':scanMarket} 종목 | {scanRiseWindow}일 내 +{scanRisePct}% 이상 | 거래량 {scanVolRatio}배↑
            </div>
          </div>
        </div>

        {scanning && <ProgressBar progress={scanProgress} msg={scanMsg} color={COLORS.red} />}
        {scanError && <ErrorBox msg={scanError} onClose={() => setScanError('')} />}

        {loadingPrev && !scanning && !scanResult && (
          <div style={{ background:COLORS.card, border:`1px solid ${COLORS.cardBorder}`,
            borderRadius:12, padding:20, marginBottom:16, textAlign:'center' }}>
            <div style={{ fontSize:14, color:COLORS.textDim }}>⏳ 이전 스캔 결과를 불러오는 중...</div>
          </div>
        )}

        {scanResult && <ScanResultView scanResult={scanResult} scanSortKey={scanSortKey}
          setScanSortKey={setScanSortKey} scanFilterLevel={scanFilterLevel}
          setScanFilterLevel={setScanFilterLevel} selectedScanStocks={selectedScanStocks}
          toggleScanStock={toggleScanStock} selectAllVisible={selectAllVisible}
          setSelectedScanStocks={setSelectedScanStocks} sendToAnalyzer={sendToAnalyzer}
          getFilteredScanResults={getFilteredScanResults}
          scanDate={scanDate} scanSource={scanSource} onReload={reloadScanFromDB} />}

        {!scanResult && !scanning && !loadingPrev && (
          <div style={{ background:COLORS.card, border:`1px solid ${COLORS.cardBorder}`,
            borderRadius:12, padding:40, textAlign:'center' }}>
            <div style={{ fontSize:48, marginBottom:16 }}>🚀</div>
            <h3 style={{ fontSize:16, fontWeight:600, marginBottom:8 }}>전종목 급상승 종목을 자동으로 찾아드립니다</h3>
            <p style={{ fontSize:13, color:COLORS.textDim, lineHeight:1.8 }}>
              DB에 등록된 전종목(~2,500개)을 스캔하여<br/>
              급상승 이력이 있는 종목을 발굴하고, 작전 세력 의심 종목을 표시합니다
            </p>
            <div style={{ marginTop:20, display:'inline-flex', gap:16, fontSize:12, color:COLORS.textDim }}>
              <span>① 조건 설정</span><span>→</span><span>② 전종목 스캔</span>
              <span>→</span><span>③ 세력주 발굴</span><span>→</span><span>④ 선택 후 패턴 분석</span>
            </div>
          </div>
        )}
      </div>)}

      {/* ━━━ 페이지 2: 패턴 분석기 ━━━ */}
      {pageMode === 'analyzer' && (<div>
        <div style={{ display:'flex', gap:10, marginBottom:16, flexWrap:'wrap' }}>
          {Object.values(PRESETS).map(preset => {
            const isActive = activePreset === preset.key;
            return (
              <button key={preset.key} onClick={() => applyPreset(preset.key)} disabled={analyzing}
                style={{ flex:'1 1 auto', minWidth:180, padding:'14px 18px', borderRadius:12, cursor:'pointer',
                  background:isActive?`${preset.color}15`:COLORS.card,
                  border:`2px solid ${isActive?preset.color:COLORS.cardBorder}`,
                  textAlign:'left', opacity:analyzing?0.5:1 }}>
                <div style={{ fontSize:14, fontWeight:700, color:isActive?preset.color:COLORS.text, marginBottom:4 }}>{preset.label}</div>
                <div style={{ fontSize:11, color:COLORS.textDim, lineHeight:1.4 }}>{preset.desc}</div>
                {isActive && preset.params && (
                  <div style={{ marginTop:8, display:'flex', gap:6, flexWrap:'wrap' }}>
                    <span style={tagStyle(preset.color)}>분석 {preset.params.preRiseDays}일</span>
                    <span style={tagStyle(preset.color)}>급상승 +{preset.params.risePct}%</span>
                    <span style={tagStyle(preset.color)}>기간 {preset.params.riseWindow}일</span>
                    <span style={tagStyle(preset.color)}>거래량 {(preset.params.weightVolume*100).toFixed(0)}%</span>
                  </div>
                )}
              </button>
            );
          })}
        </div>

        <div style={{ background:COLORS.card, border:`1px solid ${COLORS.cardBorder}`, borderRadius:12, padding:18, marginBottom:16 }}>
          <div style={{ display:'flex', gap:12, alignItems:'flex-start', flexWrap:'wrap' }}>
            <div ref={searchRef} style={{ position:'relative', flex:1, minWidth:220 }}>
              <input value={searchKeyword} onChange={e => setSearchKeyword(e.target.value)}
                placeholder="종목명 또는 코드 검색..." disabled={analyzing}
                style={{ width:'100%', padding:'10px 14px', fontSize:14, background:'#1a2234',
                  border:`1px solid ${COLORS.cardBorder}`, borderRadius:8, color:COLORS.text, outline:'none' }}
                onFocus={() => searchResults.length > 0 && setShowDropdown(true)} />
              {showDropdown && searchResults.length > 0 && (
                <div style={{ position:'absolute', top:'100%', left:0, right:0, background:'#1a2234',
                  border:`1px solid ${COLORS.cardBorder}`, borderRadius:8, marginTop:4, maxHeight:240,
                  overflowY:'auto', zIndex:100, boxShadow:'0 8px 24px rgba(0,0,0,0.4)' }}>
                  {searchResults.map((r, i) => (
                    <div key={r.code} onClick={() => addStock(r.code, r.name)}
                      style={{ padding:'10px 14px', cursor:'pointer', fontSize:13,
                        borderBottom:i<searchResults.length-1?`1px solid ${COLORS.cardBorder}`:'none',
                        display:'flex', justifyContent:'space-between', alignItems:'center' }}
                      onMouseEnter={e => e.currentTarget.style.background='rgba(59,130,246,0.1)'}
                      onMouseLeave={e => e.currentTarget.style.background='transparent'}>
                      <span style={{ fontWeight:600 }}>{r.name}</span>
                      <span style={{ color:COLORS.textDim, fontSize:12 }}>{r.code}</span>
                    </div>
                  ))}
                </div>
              )}
            </div>
            <button onClick={() => setShowSettings(!showSettings)} style={{ padding:'10px 16px', fontSize:13,
              borderRadius:8, cursor:'pointer', background:showSettings?COLORS.accentDim:'transparent',
              color:showSettings?COLORS.accent:COLORS.textDim,
              border:`1px solid ${showSettings?COLORS.accent:COLORS.cardBorder}` }}>⚙️ 상세설정</button>
            <button onClick={startAnalysis} disabled={analyzing||stocks.length===0}
              style={{ padding:'10px 24px', fontSize:14, fontWeight:600, borderRadius:8, border:'none',
                cursor:analyzing?'wait':'pointer', background:analyzing?COLORS.gray:stocks.length===0?'#374151':currentPreset.color,
                color:stocks.length===0?COLORS.textDim:COLORS.white, opacity:analyzing?0.7:1 }}>
              {analyzing?'분석 중...':'🔍 분석 시작'}</button>
          </div>

          {showSettings && <SettingsPanel {...{periodDays,setPeriodDays,preRiseDays,setPreRiseDays,
            risePct,setRisePct,riseWindow,setRiseWindow,weightReturns,setWeightReturns,
            weightCandle,setWeightCandle,weightVolume,setWeightVolume,setActivePreset}} />}

          {stocks.length > 0 && (
            <div style={{ marginTop:12, display:'flex', flexWrap:'wrap', gap:8 }}>
              {stocks.map(s => (
                <span key={s.code} style={{ display:'inline-flex', alignItems:'center', gap:6,
                  padding:'5px 12px', background:'#1a2234', borderRadius:20, fontSize:13,
                  border:`1px solid ${COLORS.cardBorder}` }}>
                  <span style={{ fontWeight:600 }}>{s.name}</span>
                  <span style={{ color:COLORS.textDim, fontSize:11 }}>{s.code}</span>
                  <span onClick={() => !analyzing && removeStock(s.code)}
                    style={{ cursor:analyzing?'default':'pointer', color:COLORS.red, fontSize:14, marginLeft:2,
                      opacity:analyzing?0.3:0.7 }}>✕</span>
                </span>
              ))}
              <span style={{ fontSize:12, color:COLORS.textDim, alignSelf:'center' }}>{stocks.length}개 종목</span>
            </div>
          )}
        </div>

        {analyzing && <ProgressBar progress={progress} msg={progressMsg} color={currentPreset.color} />}
        {error && <ErrorBox msg={error} onClose={() => setError('')} />}

        {result && (<>
          <div style={{ display:'grid', gridTemplateColumns:'repeat(auto-fit, minmax(150px, 1fr))', gap:12, marginBottom:16 }}>
            {[
              { label:'분석 종목', value:result.total_stocks, unit:'개', color:COLORS.accent },
              { label:'급상승 발견', value:result.total_surges, unit:'건', color:COLORS.green },
              { label:'패턴 추출', value:result.total_patterns, unit:'개', color:COLORS.yellow },
              { label:'패턴 그룹', value:result.clusters?.length||0, unit:'개', color:COLORS.accent },
              { label:'평균 상승률', value:result.summary?.avg_rise_pct, unit:'%', color:COLORS.red },
              { label:'평균 상승일', value:result.summary?.avg_rise_days, unit:'일', color:COLORS.grayLight },
            ].map((item, i) => (
              <div key={i} style={{ background:COLORS.card, border:`1px solid ${COLORS.cardBorder}`,
                borderRadius:10, padding:14, textAlign:'center' }}>
                <div style={{ fontSize:11, color:COLORS.textDim, marginBottom:4 }}>{item.label}</div>
                <div style={{ fontSize:22, fontWeight:700, color:item.color }}>
                  {typeof item.value==='number'?item.value.toLocaleString():'-'}</div>
                <div style={{ fontSize:11, color:COLORS.textDim }}>{item.unit}</div>
              </div>
            ))}
          </div>
          <div style={{ display:'grid', gridTemplateColumns:'repeat(4, 1fr)', gap:10, marginBottom:20 }}>
            {[
              {label:'공통 패턴', icon:'📊', color:'#4fc3f7'},
              {label:'차트 오버레이', icon:'📈', color:'#ffd54f'},
              {label:'매수 추천', icon:'🎯', color:'#4cff8b'},
              {label:'가상투자', icon:'💰', color:'#ff9800'},
            ].map((tab, i) => (
              <button key={i} onClick={() => setActiveTab(i)} style={{
                padding:'22px 10px', cursor:'pointer', transition:'all 0.2s',
                borderRadius:14, textAlign:'center', display:'flex', flexDirection:'column', alignItems:'center', gap:6,
                border: activeTab===i ? `3px solid ${tab.color}` : `2px solid ${tab.color}55`,
                background: activeTab===i ? `${tab.color}25` : `${tab.color}08`,
                color: activeTab===i ? tab.color : `${tab.color}aa`,
                boxShadow: activeTab===i ? `0 0 20px ${tab.color}40, inset 0 0 20px ${tab.color}10` : 'none',
              }}
              onMouseEnter={e => { if(activeTab!==i) { e.currentTarget.style.background=`${tab.color}18`; e.currentTarget.style.borderColor=`${tab.color}99`; e.currentTarget.style.boxShadow=`0 0 12px ${tab.color}25`; }}}
              onMouseLeave={e => { if(activeTab!==i) { e.currentTarget.style.background=`${tab.color}08`; e.currentTarget.style.borderColor=`${tab.color}55`; e.currentTarget.style.boxShadow='none'; }}}
              >
                <span style={{ fontSize:28 }}>{tab.icon}</span>
                <span style={{ fontSize:16, fontWeight:800, letterSpacing:'0.5px' }}>{tab.label}</span>
                {activeTab===i && <span style={{ fontSize:10, opacity:0.7, marginTop:2 }}>● 선택됨</span>}
              </button>
            ))}
          </div>
          {activeTab===0 && <TabSummary result={result} />}
          {activeTab===1 && <TabChart result={result} />}
          {activeTab===2 && <TabRecommend result={result} />}
          {activeTab===3 && <VirtualInvestTab recommendations={result.recommendations || []} />}
        </>)}

        {!result && !analyzing && (
          <div style={{ background:COLORS.card, border:`1px solid ${COLORS.cardBorder}`,
            borderRadius:12, padding:40, textAlign:'center' }}>
            <div style={{ fontSize:48, marginBottom:16 }}>🔬</div>
            <h3 style={{ fontSize:16, fontWeight:600, marginBottom:8 }}>급상승 직전 패턴을 찾아보세요</h3>
            <p style={{ fontSize:13, color:COLORS.textDim, lineHeight:1.8 }}>
              종목을 추가하고 분석을 시작하면<br/>DTW 알고리즘이 과거 급상승 직전 일봉 패턴을 자동으로 비교합니다
            </p>
            <div style={{ marginTop:16, padding:'10px 20px', borderRadius:8, background:COLORS.purpleDim,
              display:'inline-block', fontSize:12, color:COLORS.purple }}>
              💡 팁: 먼저 "🚀 급상승 종목 발굴"에서 종목을 찾은 뒤 여기로 보내면 편리합니다
            </div>
          </div>
        )}
      </div>)}

      <WorkflowGuide />
    </div>
  );
}

function tagStyle(c) { return { fontSize:10, padding:'2px 8px', borderRadius:10, background:`${c}20`, color:c, fontWeight:500 }; }

function WorkflowGuide() {
  const [open, setOpen] = useState(true);
  const sectionStyle = { background: COLORS.card, border: `1px solid ${COLORS.cardBorder}`, borderRadius: 12, padding: 20, marginBottom: 12 };
  const stepNumStyle = (color) => ({ width: 28, height: 28, borderRadius: '50%', display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: 13, fontWeight: 700, color: COLORS.white, background: color, flexShrink: 0 });
  const stepTitleStyle = { fontSize: 14, fontWeight: 700, color: COLORS.text };
  const stepDescStyle = { fontSize: 12, color: COLORS.textDim, lineHeight: 1.7, marginTop: 4 };
  const subStepStyle = { fontSize: 12, color: COLORS.text, padding: '8px 12px', marginTop: 6, background: '#0d1321', borderRadius: 8, lineHeight: 1.8 };
  const arrowDown = (<div style={{ textAlign: 'center', padding: '4px 0', fontSize: 16, color: COLORS.gray }}>↓</div>);
  const labelBadge = (text, color) => (<span style={{ fontSize: 10, padding: '2px 8px', borderRadius: 10, background: `${color}20`, color: color, fontWeight: 600, marginLeft: 6 }}>{text}</span>);

  return (
    <div style={{ marginTop: 32 }}>
      <div onClick={() => setOpen(!open)} style={{ display: 'flex', alignItems: 'center', gap: 10, cursor: 'pointer', padding: '14px 20px', background: COLORS.card, border: `1px solid ${COLORS.cardBorder}`, borderRadius: open ? '12px 12px 0 0' : 12, transition: 'border-radius 0.2s' }}>
        <span style={{ fontSize: 20 }}>📖</span>
        <span style={{ fontSize: 15, fontWeight: 700, color: COLORS.text, flex: 1 }}>전체 사용 흐름 / Complete Workflow</span>
        <span style={{ fontSize: 18, color: COLORS.textDim, transition: 'transform 0.2s', transform: open ? 'rotate(180deg)' : 'rotate(0deg)' }}>▾</span>
      </div>
      {open && (
        <div style={{ background: 'rgba(17,24,39,0.5)', border: `1px solid ${COLORS.cardBorder}`, borderTop: 'none', borderRadius: '0 0 12px 12px', padding: 20 }}>
          <div style={sectionStyle}>
            <div style={{ display: 'flex', alignItems: 'center', gap: 12, marginBottom: 14 }}>
              <div style={stepNumStyle(COLORS.red)}>1</div>
              <div>
                <div style={stepTitleStyle}>🚀 급상승 종목 발굴{labelBadge('전종목 스캔', COLORS.red)}</div>
                <div style={stepDescStyle}>DB에 등록된 ~2,500개 전종목을 자동 스캔하여 급상승 이력 종목을 찾습니다</div>
              </div>
            </div>
            <div style={subStepStyle}>
              조건 설정 (시장 / 기간 / 상승률 / 거래량 배율)<br/>
              {arrowDown}
              <b style={{ color: COLORS.red }}>[전종목 스캔 시작]</b> 클릭<br/>
              {arrowDown}
              ~2,500개 종목 자동 스캔 (약 10~15분 소요)<br/>
              <span style={{ fontSize: 11, color: COLORS.green }}>💡 스캔 중 다른 페이지로 이동해도 서버에서 계속 실행됩니다</span><br/>
              {arrowDown}
              결과 테이블 표시:<br/>
              <span style={{ marginLeft: 16 }}>🔴 세력 의심 (점수 70↑) · 🟡 주의 필요 (45↑) · 🟢 일반 급등</span><br/>
              {arrowDown}
              관심 종목 체크 (최대 20개)<br/>
              {arrowDown}
              <b style={{ color: COLORS.accent }}>[🔬 선택 종목 패턴분석]</b> 클릭 → 자동으로 패턴 분석기로 이동
            </div>
          </div>
          <div style={sectionStyle}>
            <div style={{ display: 'flex', alignItems: 'center', gap: 12, marginBottom: 14 }}>
              <div style={stepNumStyle(COLORS.accent)}>2</div>
              <div>
                <div style={stepTitleStyle}>🔬 패턴 분석기{labelBadge('DTW 엔진', COLORS.accent)}</div>
                <div style={stepDescStyle}>선택한 종목들이 자동 입력되어 있으며, 프리셋 선택 후 분석을 시작합니다</div>
              </div>
            </div>
            <div style={subStepStyle}>
              선택한 종목들이 자동으로 입력되어 있음<br/>
              {arrowDown}
              프리셋 선택: 🏢 우량주 / ⚡ 작전주 / 🔧 사용자정의<br/>
              {arrowDown}
              <b style={{ color: COLORS.accent }}>[🔍 분석 시작]</b> 클릭
            </div>
          </div>
          <div style={sectionStyle}>
            <div style={{ display: 'flex', alignItems: 'center', gap: 12, marginBottom: 14 }}>
              <div style={stepNumStyle(COLORS.yellow)}>3</div>
              <div>
                <div style={stepTitleStyle}>⚙️ 분석 시작 후 자동 처리 과정{labelBadge('자동', COLORS.yellow)}</div>
                <div style={stepDescStyle}>분석 시작 버튼을 누르면 아래 과정이 자동으로 진행됩니다</div>
              </div>
            </div>
            {[
              { pct: '0~35%', icon: '📥', title: '데이터 수집', desc: '선택한 종목마다 네이버 금융에서 일봉 데이터를 자동 수집\n(시가 / 고가 / 저가 / 종가 / 거래량, 최대 600거래일)' },
              { pct: '35~45%', icon: '📈', title: '급상승 구간 탐지', desc: '각 종목의 과거 일봉에서 "N일 내 +X% 이상 상승한 구간"을 자동 탐색\n예) 삼성전자: 3건, SK하이닉스: 2건 발견' },
              { pct: '45~50%', icon: '🧬', title: '패턴 벡터 추출', desc: '각 급상승 직전 N일의 일봉을 3차원 벡터로 변환:\n· 매일 등락률 흐름\n· 봉 모양 (양봉/음봉, 꼬리 비율)\n· 거래량 변화 (평소 대비 몇 배)' },
              { pct: '50~75%', icon: '🔄', title: 'DTW 유사도 비교 & 클러스터링', desc: '추출한 모든 패턴을 서로 DTW로 비교\n→ 공통 패턴 찾기 → 그룹으로 묶음' },
              { pct: '75~100%', icon: '🎯', title: '현재 매수 추천 계산', desc: '각 종목의 현재 최근 N일 흐름이\n공통 패턴과 얼마나 비슷한지 계산\n→ 유사도 점수 → 매수 시그널 판정' },
            ].map((step, i) => (
              <div key={i}>
                {i > 0 && <div style={{ textAlign: 'center', padding: '2px 0', fontSize: 14, color: COLORS.gray }}>↓</div>}
                <div style={{ display: 'flex', gap: 12, padding: '10px 14px', background: '#0d1321', borderRadius: 8, alignItems: 'flex-start' }}>
                  <div style={{ fontSize: 10, fontWeight: 700, color: COLORS.yellow, whiteSpace: 'nowrap', padding: '3px 8px', background: COLORS.yellowDim, borderRadius: 6, marginTop: 2 }}>{step.pct}</div>
                  <div style={{ fontSize: 18, flexShrink: 0 }}>{step.icon}</div>
                  <div>
                    <div style={{ fontSize: 13, fontWeight: 700, color: COLORS.text }}>{step.title}</div>
                    <div style={{ fontSize: 11, color: COLORS.textDim, lineHeight: 1.7, marginTop: 4, whiteSpace: 'pre-line' }}>{step.desc}</div>
                  </div>
                </div>
              </div>
            ))}
          </div>
          <div style={sectionStyle}>
            <div style={{ display: 'flex', alignItems: 'center', gap: 12, marginBottom: 14 }}>
              <div style={stepNumStyle(COLORS.green)}>4</div>
              <div>
                <div style={stepTitleStyle}>📊 결과 확인 (3개 탭)</div>
                <div style={stepDescStyle}>분석이 완료되면 3개 탭에서 결과를 확인할 수 있습니다</div>
              </div>
            </div>
            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(280px, 1fr))', gap: 12 }}>
              <div style={{ padding: 14, background: '#0d1321', borderRadius: 10, border: `1px solid ${COLORS.cardBorder}` }}>
                <div style={{ fontSize: 13, fontWeight: 700, color: COLORS.accent, marginBottom: 8 }}>📊 공통 패턴</div>
                <div style={{ fontSize: 11, color: COLORS.textDim, lineHeight: 1.7 }}>급상승 전 반복 패턴 · 소속 종목 · 평균 상승률</div>
              </div>
              <div style={{ padding: 14, background: '#0d1321', borderRadius: 10, border: `1px solid ${COLORS.cardBorder}` }}>
                <div style={{ fontSize: 13, fontWeight: 700, color: COLORS.yellow, marginBottom: 8 }}>📈 차트 오버레이</div>
                <div style={{ fontSize: 11, color: COLORS.textDim, lineHeight: 1.7 }}>등락률/거래량 오버레이 · 개별 미니 캔들차트</div>
              </div>
              <div style={{ padding: 14, background: '#0d1321', borderRadius: 10, border: `1px solid ${COLORS.cardBorder}` }}>
                <div style={{ fontSize: 13, fontWeight: 700, color: COLORS.green, marginBottom: 8 }}>🎯 매수 추천</div>
                <div style={{ fontSize: 11, color: COLORS.textDim, lineHeight: 1.7 }}>🟢 강력매수 65%↑ · 🟡 관심 50%↑ · ⚠️ 대기 40%↑</div>
              </div>
            </div>
          </div>
          <div style={{ background: 'linear-gradient(135deg, rgba(59,130,246,0.1), rgba(16,185,129,0.1))', border: `1px solid rgba(59,130,246,0.2)`, borderRadius: 12, padding: 20, textAlign: 'center' }}>
            <div style={{ fontSize: 14, fontWeight: 700, color: COLORS.accent, marginBottom: 10 }}>💡 핵심 요약</div>
            <div style={{ fontSize: 13, color: COLORS.text, lineHeight: 2 }}>
              <b style={{ color: COLORS.red }}>스캐너</b> — 2,500개 중 "과거에 급상승한 종목" 발굴<br/>
              <b style={{ color: COLORS.accent }}>패턴분석기</b> — 급상승 직전 패턴을 학습하여 "현재 같은 패턴인 종목" 추천
            </div>
            <div style={{ marginTop: 14, display: 'inline-flex', gap: 12, alignItems: 'center', fontSize: 13, color: COLORS.text, fontWeight: 600 }}>
              <span style={{ padding: '4px 12px', borderRadius: 8, background: COLORS.redDim, color: COLORS.red }}>과거 급상승</span>
              <span style={{ color: COLORS.gray }}>→</span>
              <span style={{ padding: '4px 12px', borderRadius: 8, background: COLORS.yellowDim, color: COLORS.yellow }}>직전 패턴 학습</span>
              <span style={{ color: COLORS.gray }}>→</span>
              <span style={{ padding: '4px 12px', borderRadius: 8, background: COLORS.greenDim, color: COLORS.green }}>현재 같은 패턴 추천</span>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

// ━━━ 공통 컴포넌트 ━━━
function FilterGroup({ label, options, value, setter, color, disabled }) {
  return (<div>
    <div style={{ fontSize:11, color:COLORS.textDim, marginBottom:6 }}>{label}</div>
    <div style={{ display:'flex', gap:4 }}>
      {options.map(opt => (<button key={opt.v} onClick={() => setter(opt.v)} disabled={disabled} style={{ flex:1, padding:'6px 4px', fontSize:11, borderRadius:6, cursor:disabled?'default':'pointer', border:`1px solid ${value===opt.v?color:COLORS.cardBorder}`, background:value===opt.v?`${color}20`:'transparent', color:value===opt.v?color:COLORS.textDim }}>{opt.l}</button>))}
    </div>
  </div>);
}

function ProgressBar({ progress, msg, color }) {
  return (<div style={{ background:COLORS.card, border:`1px solid ${COLORS.cardBorder}`, borderRadius:12, padding:18, marginBottom:16 }}>
    <div style={{ display:'flex', justifyContent:'space-between', marginBottom:8 }}>
      <span style={{ fontSize:13, fontWeight:600 }}>⏳ 진행 중...</span>
      <span style={{ fontSize:13, color, fontWeight:700 }}>{progress}%</span>
    </div>
    <div style={{ height:8, background:'#1a2234', borderRadius:4, overflow:'hidden' }}>
      <div style={{ height:'100%', width:`${progress}%`, background:`linear-gradient(90deg, ${color}, ${COLORS.green})`, borderRadius:4, transition:'width 0.5s ease' }} />
    </div>
    <div style={{ fontSize:12, color:COLORS.textDim, marginTop:6 }}>{msg}</div>
  </div>);
}

function ErrorBox({ msg, onClose }) {
  return (<div style={{ background:COLORS.redDim, border:`1px solid ${COLORS.red}`, borderRadius:8, padding:14, marginBottom:16, fontSize:13, color:COLORS.red }}>
    ❌ {msg}<span onClick={onClose} style={{ float:'right', cursor:'pointer', fontWeight:700 }}>✕</span>
  </div>);
}

function SettingsPanel(p) {
  return (<div style={{ marginTop:14, padding:14, background:'#0d1321', borderRadius:8 }}>
    <div style={{ display:'grid', gridTemplateColumns:'repeat(auto-fit, minmax(150px, 1fr))', gap:12, marginBottom:14 }}>
      {[
        { label:'조회 기간', value:p.periodDays, setter:p.setPeriodDays, opts:[{v:180,l:'6개월'},{v:365,l:'1년'},{v:730,l:'2년'},{v:1095,l:'3년'}] },
        { label:'사전 분석일', value:p.preRiseDays, setter:p.setPreRiseDays, opts:[{v:5,l:'5일'},{v:10,l:'10일'},{v:20,l:'20일'},{v:30,l:'30일'},{v:40,l:'40일'}] },
        { label:'급상승 기준', value:p.risePct, setter:p.setRisePct, opts:[{v:15,l:'+15%'},{v:20,l:'+20%'},{v:30,l:'+30%'},{v:50,l:'+50%'},{v:100,l:'+100%'}] },
        { label:'상승 기간', value:p.riseWindow, setter:p.setRiseWindow, opts:[{v:3,l:'3일'},{v:5,l:'5일'},{v:10,l:'10일'},{v:15,l:'15일'}] },
      ].map(cfg => (<div key={cfg.label}>
        <div style={{ fontSize:11, color:COLORS.textDim, marginBottom:6 }}>{cfg.label}</div>
        <div style={{ display:'flex', gap:4, flexWrap:'wrap' }}>
          {cfg.opts.map(opt => (<button key={opt.v} onClick={() => { cfg.setter(opt.v); p.setActivePreset('custom'); }}
            style={{ flex:'1 1 auto', padding:'5px 4px', fontSize:11, borderRadius:6, border:`1px solid ${cfg.value===opt.v?COLORS.accent:COLORS.cardBorder}`, background:cfg.value===opt.v?COLORS.accentDim:'transparent', color:cfg.value===opt.v?COLORS.accent:COLORS.textDim, cursor:'pointer' }}>{opt.l}</button>))}
        </div>
      </div>))}
    </div>
    <div style={{ borderTop:`1px solid ${COLORS.cardBorder}`, paddingTop:14 }}>
      <div style={{ fontSize:12, fontWeight:600, color:COLORS.text, marginBottom:10 }}>📊 DTW 비교 가중치 (합계 100%)</div>
      <div style={{ display:'grid', gridTemplateColumns:'repeat(3, 1fr)', gap:16 }}>
        {[
          { label:'📈 등락률', value:p.weightReturns, setter:p.setWeightReturns, color:'#3b82f6' },
          { label:'🕯️ 봉모양', value:p.weightCandle, setter:p.setWeightCandle, color:'#f59e0b' },
          { label:'📊 거래량', value:p.weightVolume, setter:p.setWeightVolume, color:'#10b981' },
        ].map(w => (<div key={w.label}>
          <div style={{ display:'flex', justifyContent:'space-between', fontSize:11, color:COLORS.textDim, marginBottom:6 }}>
            <span>{w.label}</span><span style={{ color:w.color, fontWeight:700 }}>{(w.value*100).toFixed(0)}%</span>
          </div>
          <input type="range" min={0} max={80} step={5} value={w.value*100}
            onChange={e => { const nv = parseInt(e.target.value)/100; w.setter(nv); p.setActivePreset('custom'); const allW = [p.weightReturns,p.weightCandle,p.weightVolume]; const idx = allW.indexOf(w.value); const o1 = idx===0?1:0, o2 = idx===2?1:2; const rem = 1-nv; const os = allW[o1]+allW[o2]; if(os>0){ [p.setWeightReturns,p.setWeightCandle,p.setWeightVolume][o1](Math.round(allW[o1]/os*rem*100)/100); [p.setWeightReturns,p.setWeightCandle,p.setWeightVolume][o2](Math.round(allW[o2]/os*rem*100)/100); } else { [p.setWeightReturns,p.setWeightCandle,p.setWeightVolume][o1](rem/2); [p.setWeightReturns,p.setWeightCandle,p.setWeightVolume][o2](rem/2); } }}
            style={{ width:'100%', height:6, borderRadius:3, accentColor:w.color, cursor:'pointer' }} />
        </div>))}
      </div>
      <div style={{ display:'flex', height:8, borderRadius:4, overflow:'hidden', marginTop:10 }}>
        <div style={{ width:`${p.weightReturns*100}%`, background:'#3b82f6' }} />
        <div style={{ width:`${p.weightCandle*100}%`, background:'#f59e0b' }} />
        <div style={{ width:`${p.weightVolume*100}%`, background:'#10b981' }} />
      </div>
    </div>
  </div>);
}

function ScanResultView({ scanResult, scanSortKey, setScanSortKey, scanFilterLevel, setScanFilterLevel, selectedScanStocks, toggleScanStock, selectAllVisible, setSelectedScanStocks, sendToAnalyzer, getFilteredScanResults, scanDate, scanSource, onReload }) {
  const stats = scanResult.stats || {};
  const filtered = getFilteredScanResults();
  const fmtDate = (iso) => { if (!iso) return ''; try { const d = new Date(iso); return `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}-${String(d.getDate()).padStart(2,'0')} ${String(d.getHours()).padStart(2,'0')}:${String(d.getMinutes()).padStart(2,'0')}`; } catch { return iso; } };

  return (<div>
    {scanDate && (<div onClick={onReload} style={{ display:'flex', justifyContent:'space-between', alignItems:'center', padding:'10px 16px', marginBottom:12, borderRadius:8, background: scanSource==='db' ? 'rgba(139,92,246,0.1)' : scanSource==='loading' ? 'rgba(100,100,100,0.1)' : 'rgba(16,185,129,0.1)', border: `1px solid ${scanSource==='db' ? 'rgba(139,92,246,0.2)' : scanSource==='loading' ? 'rgba(100,100,100,0.2)' : 'rgba(16,185,129,0.2)'}`, cursor:'pointer', transition:'all 0.2s' }}
      onMouseEnter={e => e.currentTarget.style.opacity='0.8'}
      onMouseLeave={e => e.currentTarget.style.opacity='1'}>
      <span style={{ fontSize:12, color: scanSource==='db' ? COLORS.purple : scanSource==='loading' ? '#888' : COLORS.green }}>
        {scanSource==='loading' ? '⏳ DB에서 불러오는 중...' : scanSource==='db' ? '💾 DB에서 복원된 결과' : '✅ 방금 스캔한 결과'}
        <span style={{ marginLeft:8, fontSize:10, color:COLORS.textDim }}>클릭하면 DB에서 다시 불러옵니다</span>
      </span>
      <span style={{ fontSize:12, color:COLORS.textDim }}>마지막 스캔: <b style={{ color:COLORS.text }}>{fmtDate(scanDate)}</b>{scanResult.market && <span> · {scanResult.market==='ALL'?'전체':scanResult.market}</span>}</span>
    </div>)}
    <div style={{ display:'grid', gridTemplateColumns:'repeat(auto-fit, minmax(130px, 1fr))', gap:12, marginBottom:16 }}>
      {[
        { label:'스캔 종목', value:stats.total_scanned, unit:'개', color:COLORS.accent },
        { label:'급상승 발견', value:stats.total_found, unit:'종목', color:COLORS.green },
        { label:'급상승 건수', value:stats.total_surges, unit:'건', color:COLORS.yellow },
        { label:'🔴 세력 의심', value:stats.high_manip_count, unit:'종목', color:COLORS.red },
        { label:'🟡 주의 필요', value:stats.medium_manip_count, unit:'종목', color:COLORS.yellow },
      ].map((item,i) => (<div key={i} style={{ background:COLORS.card, border:`1px solid ${COLORS.cardBorder}`, borderRadius:10, padding:14, textAlign:'center' }}>
        <div style={{ fontSize:11, color:COLORS.textDim, marginBottom:4 }}>{item.label}</div>
        <div style={{ fontSize:22, fontWeight:700, color:item.color }}>{item.value?.toLocaleString()??'-'}</div>
        <div style={{ fontSize:11, color:COLORS.textDim }}>{item.unit}</div>
      </div>))}
    </div>
    <div style={{ background:COLORS.card, border:`1px solid ${COLORS.cardBorder}`, borderRadius:12, padding:14, marginBottom:12, display:'flex', flexWrap:'wrap', gap:12, alignItems:'center' }}>
      <div style={{ display:'flex', gap:4 }}>
        <span style={{ fontSize:12, color:COLORS.textDim, alignSelf:'center', marginRight:4 }}>필터:</span>
        {[{v:'all',l:'전체',c:COLORS.accent},{v:'high',l:'🔴 세력의심',c:COLORS.red},{v:'medium',l:'🟡 주의이상',c:COLORS.yellow}].map(f => (<button key={f.v} onClick={() => setScanFilterLevel(f.v)} style={{ padding:'5px 12px', fontSize:11, borderRadius:6, cursor:'pointer', border:`1px solid ${scanFilterLevel===f.v?f.c:COLORS.cardBorder}`, background:scanFilterLevel===f.v?`${f.c}20`:'transparent', color:scanFilterLevel===f.v?f.c:COLORS.textDim }}>{f.l}</button>))}
      </div>
      <div style={{ display:'flex', gap:4 }}>
        <span style={{ fontSize:12, color:COLORS.textDim, alignSelf:'center', marginRight:4 }}>정렬:</span>
        {[{v:'manip_score',l:'세력점수↓'},{v:'rise_pct',l:'상승률↓'},{v:'date',l:'최근순'},{v:'from_peak',l:'고점대비↓'}].map(s => (<button key={s.v} onClick={() => setScanSortKey(s.v)} style={{ padding:'5px 10px', fontSize:11, borderRadius:6, cursor:'pointer', border:`1px solid ${scanSortKey===s.v?COLORS.accent:COLORS.cardBorder}`, background:scanSortKey===s.v?COLORS.accentDim:'transparent', color:scanSortKey===s.v?COLORS.accent:COLORS.textDim }}>{s.l}</button>))}
      </div>
      <div style={{ marginLeft:'auto', display:'flex', gap:8 }}>
        <button onClick={selectAllVisible} style={{ padding:'5px 12px', fontSize:11, borderRadius:6, cursor:'pointer', border:`1px solid ${COLORS.cardBorder}`, background:'transparent', color:COLORS.textDim }}>전체선택</button>
        <button onClick={() => setSelectedScanStocks(new Set())} style={{ padding:'5px 12px', fontSize:11, borderRadius:6, cursor:'pointer', border:`1px solid ${COLORS.cardBorder}`, background:'transparent', color:COLORS.textDim }}>선택해제</button>
        <button onClick={sendToAnalyzer} disabled={selectedScanStocks.size===0} style={{ padding:'6px 16px', fontSize:12, fontWeight:700, borderRadius:8, border:'none', cursor:selectedScanStocks.size>0?'pointer':'default', background:selectedScanStocks.size>0?COLORS.accent:'#374151', color:selectedScanStocks.size>0?COLORS.white:COLORS.textDim }}>🔬 선택 종목 패턴분석 ({selectedScanStocks.size})</button>
      </div>
    </div>
    <div style={{ background:COLORS.card, border:`1px solid ${COLORS.cardBorder}`, borderRadius:12, overflow:'hidden' }}>
      <div style={{ display:'grid', gridTemplateColumns:'40px 1fr 80px 80px 60px 70px 80px 80px', padding:'10px 14px', fontSize:11, color:COLORS.textDim, fontWeight:600, borderBottom:`1px solid ${COLORS.cardBorder}`, background:'#0d1321' }}>
        <span></span><span>종목</span><span style={{textAlign:'right'}}>현재가</span><span style={{textAlign:'center'}}>최대상승</span><span style={{textAlign:'center'}}>횟수</span><span style={{textAlign:'center'}}>고점대비</span><span style={{textAlign:'center'}}>세력점수</span><span style={{textAlign:'center'}}>판정</span>
      </div>
      {filtered.length===0 ? (<div style={{ textAlign:'center', padding:30, color:COLORS.textDim, fontSize:13 }}>조건에 해당하는 급상승 종목이 없습니다.</div>) : filtered.map((stock, i) => {
        const sel = selectedScanStocks.has(stock.code);
        const mc = stock.top_manip_level==='high'?COLORS.red:stock.top_manip_level==='medium'?COLORS.yellow:COLORS.green;
        return (<div key={stock.code} onClick={() => toggleScanStock(stock.code)} style={{ display:'grid', gridTemplateColumns:'40px 1fr 80px 80px 60px 70px 80px 80px', padding:'10px 14px', alignItems:'center', borderBottom:`1px solid ${COLORS.cardBorder}`, background:sel?'rgba(59,130,246,0.08)':i%2===0?'transparent':'rgba(255,255,255,0.015)', cursor:'pointer' }}>
          <div style={{textAlign:'center'}}><div style={{ width:18, height:18, borderRadius:4, border:`2px solid ${sel?COLORS.accent:COLORS.cardBorder}`, background:sel?COLORS.accent:'transparent', display:'flex', alignItems:'center', justifyContent:'center', fontSize:12, color:COLORS.white, fontWeight:700 }}>{sel&&'✓'}</div></div>
          <div><div style={{ fontSize:13, fontWeight:600 }}>{stock.name}<span style={{ fontSize:10, color:COLORS.textDim, marginLeft:6 }}>{stock.code} · {stock.market}</span></div><div style={{ fontSize:10, color:COLORS.textDim }}>최근: {stock.latest_surge_date||'-'}</div></div>
          <div style={{ textAlign:'right', fontSize:12, fontWeight:600 }}>{fmt(stock.current_price)}</div>
          <div style={{ textAlign:'center', fontSize:12, fontWeight:700, color:COLORS.red }}>+{stock.latest_rise_pct}%</div>
          <div style={{ textAlign:'center', fontSize:12, fontWeight:600 }}>{stock.surge_count}회</div>
          <div style={{ textAlign:'center', fontSize:11, fontWeight:600, color:stock.latest_from_peak<-30?COLORS.accent:COLORS.textDim }}>{stock.latest_from_peak}%</div>
          <div style={{textAlign:'center'}}><div style={{ display:'flex', alignItems:'center', gap:4, justifyContent:'center' }}><div style={{ width:36, height:6, background:'#1a2234', borderRadius:3, overflow:'hidden' }}><div style={{ width:`${stock.top_manip_score}%`, height:'100%', background:mc, borderRadius:3 }} /></div><span style={{ fontSize:11, fontWeight:700, color:mc }}>{stock.top_manip_score}</span></div></div>
          <div style={{ textAlign:'center', fontSize:10, fontWeight:600, padding:'3px 4px', borderRadius:6, background:`${mc}20`, color:mc }}>{stock.top_manip_label}</div>
        </div>);
      })}
    </div>
    <div style={{ fontSize:12, color:COLORS.textDim, marginTop:8, textAlign:'right' }}>총 {filtered.length}개 종목</div>
    <div style={{ marginTop:12, padding:12, borderRadius:8, background:'rgba(245,158,11,0.08)', border:'1px solid rgba(245,158,11,0.2)', fontSize:11, color:COLORS.yellow, lineHeight:1.6 }}>⚠️ 세력 의심 점수는 거래량 폭증, 급등 후 급락, 매집 흔적 등을 종합한 통계적 지표이며, 실제 작전 여부를 확정하지 않습니다. 반드시 추가 확인 후 투자 판단하세요.</div>
  </div>);
}

function TabSummary({ result }) {
  const clusters = result.clusters||[], summary = result.summary||{};
  return (<div>
    {summary.common_features?.length>0 && (<div style={{ background:COLORS.accentDim, border:'1px solid rgba(59,130,246,0.3)', borderRadius:10, padding:16, marginBottom:16 }}>
      <div style={{ fontSize:13, fontWeight:600, marginBottom:8, color:COLORS.accent }}>💡 공통 패턴 특징</div>
      {summary.common_features.map((f,i) => (<div key={i} style={{ fontSize:13, color:COLORS.text, marginBottom:4, paddingLeft:12 }}>• {f}</div>))}
    </div>)}
    {clusters.length===0 ? <div style={{textAlign:'center',padding:40,color:COLORS.textDim}}>유의미한 공통 패턴이 발견되지 않았습니다.</div> :
      clusters.map((c,ci) => (<div key={ci} style={{ background:COLORS.card, border:`1px solid ${COLORS.cardBorder}`, borderRadius:12, padding:18, marginBottom:12 }}>
        <div style={{ display:'flex', justifyContent:'space-between', alignItems:'center', marginBottom:12 }}>
          <div><span style={{ fontSize:11, background:COLORS.accentDim, color:COLORS.accent, padding:'2px 10px', borderRadius:12, fontWeight:600 }}>패턴 #{ci+1}</span><span style={{ fontSize:13, fontWeight:600, marginLeft:10 }}>{c.pattern_count}건 발견</span></div>
          <div style={{ fontSize:12, color:COLORS.textDim }}>유사도 {c.avg_similarity?.toFixed(1)}%</div>
        </div>
        <div style={{ fontSize:13, color:COLORS.text, marginBottom:14, padding:10, background:'#0d1321', borderRadius:6, lineHeight:1.6 }}>{c.description||'패턴 분석 중'}</div>
        {c.avg_return_flow?.length>0 && <MiniReturnChart returns={c.avg_return_flow} label="평균 등락률 흐름" />}
        <div style={{ marginTop:12 }}>
          <div style={{ fontSize:11, color:COLORS.textDim, marginBottom:6 }}>소속 종목</div>
          <div style={{ display:'flex', flexWrap:'wrap', gap:6 }}>
            {c.members?.map((m,mi) => (<span key={mi} style={{ fontSize:11, padding:'3px 10px', borderRadius:12, background:'#1a2234', border:`1px solid ${COLORS.cardBorder}` }}>{m.name}<span style={{color:COLORS.green,marginLeft:4}}>+{m.rise_pct}%</span><span style={{color:COLORS.textDim,marginLeft:4}}>{m.surge_date}</span></span>))}
          </div>
        </div>
        <div style={{ display:'flex', gap:20, marginTop:12, paddingTop:12, borderTop:`1px solid ${COLORS.cardBorder}`, fontSize:12 }}>
          <span>평균 상승: <b style={{color:COLORS.red}}>+{c.avg_rise_pct}%</b></span><span>평균 기간: <b>{c.avg_rise_days}일</b></span>
        </div>
      </div>))}
  </div>);
}

function TabChart({ result }) {
  const patterns = result.all_patterns||[];
  if(patterns.length===0) return <div style={{textAlign:'center',padding:40,color:COLORS.textDim}}>차트를 표시할 패턴이 없습니다.</div>;
  return (<div>
    <div style={{ background:COLORS.card, border:`1px solid ${COLORS.cardBorder}`, borderRadius:12, padding:18, marginBottom:16 }}>
      <div style={{ fontSize:14, fontWeight:600, marginBottom:14 }}>📈 등락률 흐름 오버레이</div>
      <OverlayChart patterns={patterns} dataKey="returns" yLabel="등락률 (%)" />
    </div>
    <div style={{ background:COLORS.card, border:`1px solid ${COLORS.cardBorder}`, borderRadius:12, padding:18, marginBottom:16 }}>
      <div style={{ fontSize:14, fontWeight:600, marginBottom:14 }}>📊 거래량 비율 오버레이</div>
      <OverlayChart patterns={patterns} dataKey="volume_ratios" yLabel="거래량 배율" />
    </div>
    <div style={{ fontSize:14, fontWeight:600, marginBottom:12 }}>🕯️ 개별 패턴 일봉</div>
    <div style={{ display:'grid', gridTemplateColumns:'repeat(auto-fit, minmax(340px, 1fr))', gap:12 }}>
      {patterns.slice(0,12).map((p,i) => (<div key={i} style={{ background:COLORS.card, border:`1px solid ${COLORS.cardBorder}`, borderRadius:10, padding:14 }}>
        <div style={{ display:'flex', justifyContent:'space-between', marginBottom:8 }}>
          <span style={{ fontSize:12, fontWeight:600 }}>{p.name} <span style={{color:COLORS.textDim}}>({p.code})</span></span>
          <span style={{ fontSize:11, color:COLORS.green }}>+{p.surge?.rise_pct}% ({p.surge?.start_date})</span>
        </div>
        <MiniCandleChart candles={p.candles||[]} />
      </div>))}
    </div>
  </div>);
}

function TabRecommend({ result }) {
  const recs = result.recommendations||[];
  const scannedCount = result.scanned_candidates || recs.length;
  const analyzedCodes = result.analyzed_codes || [];
  return (<div>
    {/* 스캔 정보 헤더 */}
    <div style={{ marginBottom:12, padding:'10px 14px', borderRadius:8, background:'rgba(79,195,247,0.08)', border:'1px solid rgba(79,195,247,0.2)', fontSize:12, color:COLORS.accent, lineHeight:1.6 }}>
      🔍 전종목 DB에서 <span style={{fontWeight:700,color:'#4fc3f7'}}>{scannedCount}개</span> 종목을 스캔하여,
      분석 대상({analyzedCodes.length}개)을 <span style={{fontWeight:700,color:'#ff6b6b'}}>제외</span>한 유사 패턴 종목입니다.
    </div>
    {recs.length===0 ? <div style={{textAlign:'center',padding:40,color:COLORS.textDim}}>유사 패턴 종목이 발견되지 않았습니다.<br/><span style={{fontSize:11}}>클러스터가 없거나 DB에 종목이 부족합니다.</span></div> : (
      <div style={{ background:COLORS.card, border:`1px solid ${COLORS.cardBorder}`, borderRadius:12, overflow:'hidden' }}>
        <div style={{ display:'grid', gridTemplateColumns:'1fr 90px 100px 80px', padding:'12px 18px', fontSize:11, color:COLORS.textDim, fontWeight:600, borderBottom:`1px solid ${COLORS.cardBorder}`, background:'#0d1321' }}>
          <span>종목</span><span style={{textAlign:'right'}}>현재가</span><span style={{textAlign:'center'}}>유사도</span><span style={{textAlign:'center'}}>시그널</span>
        </div>
        {recs.map((rec,i) => {
          const sc = rec.similarity>=65?COLORS.green:rec.similarity>=50?COLORS.yellow:rec.similarity>=40?COLORS.grayLight:COLORS.gray;
          const sb = rec.signal_code==='strong_buy'?COLORS.greenDim:rec.signal_code==='watch'?COLORS.yellowDim:'transparent';
          return (<div key={i} style={{ display:'grid', gridTemplateColumns:'1fr 90px 100px 80px', padding:'12px 18px', alignItems:'center', borderBottom:`1px solid ${COLORS.cardBorder}`, background:i%2===0?'transparent':'rgba(255,255,255,0.02)' }}>
            <div><div style={{fontSize:13,fontWeight:600}}>{rec.name}</div><div style={{fontSize:11,color:COLORS.textDim}}>{rec.code}</div></div>
            <div style={{textAlign:'right',fontSize:13,fontWeight:600}}>{fmt(rec.current_price)}</div>
            <div style={{textAlign:'center'}}><div style={{display:'flex',alignItems:'center',gap:6,justifyContent:'center'}}><div style={{width:50,height:6,background:'#1a2234',borderRadius:3,overflow:'hidden'}}><div style={{width:`${Math.min(rec.similarity,100)}%`,height:'100%',background:sc,borderRadius:3}} /></div><span style={{fontSize:12,fontWeight:700,color:sc}}>{rec.similarity}%</span></div></div>
            <div style={{textAlign:'center',fontSize:11,fontWeight:600,padding:'3px 6px',borderRadius:6,background:sb}}>{rec.signal}</div>
          </div>);
        })}
      </div>
    )}
    <div style={{ marginTop:16, padding:12, borderRadius:8, background:'rgba(245,158,11,0.08)', border:'1px solid rgba(245,158,11,0.2)', fontSize:11, color:COLORS.yellow, lineHeight:1.6 }}>⚠️ 패턴 유사도는 과거 데이터 기반 통계이며, 미래 수익을 보장하지 않습니다.</div>
  </div>);
}

function MiniReturnChart({ returns, label }) {
  if(!returns||returns.length===0) return null;
  const maxAbs=Math.max(...returns.map(Math.abs),1), W=300, H=60;
  const barW=Math.max(4,(W-20)/returns.length-2);
  return (<div>
    {label && <div style={{fontSize:11,color:COLORS.textDim,marginBottom:4}}>{label}</div>}
    <svg width={W} height={H} style={{display:'block'}}>
      <line x1={0} y1={H/2} x2={W} y2={H/2} stroke={COLORS.cardBorder} strokeWidth={1}/>
      {returns.map((r,i) => { const x=10+i*(barW+2), bH=(Math.abs(r)/maxAbs)*(H/2-4), y=r>=0?H/2-bH:H/2; return <rect key={i} x={x} y={y} width={barW} height={Math.max(bH,1)} fill={r>=0?COLORS.red:COLORS.accent} rx={1} opacity={0.8}/>; })}
      <text x={10} y={H-1} fontSize={8} fill={COLORS.textDim}>D-{returns.length}</text>
      <text x={W-20} y={H-1} fontSize={8} fill={COLORS.textDim}>D-1</text>
    </svg>
  </div>);
}

function OverlayChart({ patterns, dataKey, yLabel }) {
  if(!patterns||patterns.length===0) return null;
  const W=700, H=220, PAD=40, plotW=W-PAD*2, plotH=H-PAD*2;
  const palette=['#3b82f6','#ef4444','#10b981','#f59e0b','#8b5cf6','#ec4899','#14b8a6','#f97316','#6366f1','#06b6d4','#84cc16','#e11d48'];
  const allSeries=patterns.map(p=>p[dataKey]||[]);
  const allVals=allSeries.flat(); if(allVals.length===0) return null;
  const minVal=Math.min(...allVals), maxVal=Math.max(...allVals), range=maxVal-minVal||1;
  const toX=(i,len)=>PAD+(i/Math.max(len-1,1))*plotW, toY=v=>PAD+(1-(v-minVal)/range)*plotH;
  return (
    <svg width={W} height={H} style={{display:'block',maxWidth:'100%'}}>
      <rect x={PAD} y={PAD} width={plotW} height={plotH} fill="rgba(13,19,33,0.5)" rx={4}/>
      {[0,0.25,0.5,0.75,1].map((pct,i) => { const y=PAD+pct*plotH, val=maxVal-pct*range; return (<g key={i}><line x1={PAD} y1={y} x2={PAD+plotW} y2={y} stroke={COLORS.cardBorder} strokeDasharray="3,3"/><text x={PAD-4} y={y+4} fontSize={9} fill={COLORS.textDim} textAnchor="end">{val.toFixed(1)}</text></g>); })}
      {dataKey==='returns'&&minVal<0&&maxVal>0 && <line x1={PAD} y1={toY(0)} x2={PAD+plotW} y2={toY(0)} stroke={COLORS.grayLight} strokeWidth={1} opacity={0.5}/>}
      {allSeries.slice(0,12).map((s,si) => { if(s.length<2) return null; const path=s.map((v,i)=>`${i===0?'M':'L'}${toX(i,s.length).toFixed(1)},${toY(v).toFixed(1)}`).join(' '); return <path key={si} d={path} fill="none" stroke={palette[si%palette.length]} strokeWidth={1.5} opacity={0.7}/>; })}
      <text x={12} y={PAD+plotH/2} fontSize={10} fill={COLORS.textDim} textAnchor="middle" transform={`rotate(-90, 12, ${PAD+plotH/2})`}>{yLabel}</text>
      {patterns.slice(0,8).map((p,i) => (<g key={i} transform={`translate(${PAD+8+(i%4)*160}, ${PAD+8+Math.floor(i/4)*14})`}><rect width={10} height={3} fill={palette[i%palette.length]} rx={1}/><text x={14} y={4} fontSize={9} fill={COLORS.textDim}>{p.name}</text></g>))}
    </svg>);
}

function MiniCandleChart({ candles }) {
  if(!candles||candles.length===0) return null;
  const W=300, H=80;
  const allP=candles.flatMap(c=>[c.high,c.low]).filter(p=>p>0); if(allP.length===0) return null;
  const minP=Math.min(...allP), maxP=Math.max(...allP), rP=maxP-minP||1;
  const cw=(W-10)/candles.length, toY=p=>5+(1-(p-minP)/rP)*(H-10);
  return (<svg width={W} height={H} style={{display:'block'}}>
    {candles.map((c,i) => { const x=5+i*cw, isUp=c.close>=c.open, color=isUp?COLORS.red:COLORS.accent; const bT=toY(Math.max(c.open,c.close)), bB=toY(Math.min(c.open,c.close)), bH=Math.max(bB-bT,1); return (<g key={i}><line x1={x+cw/2} y1={toY(c.high)} x2={x+cw/2} y2={toY(c.low)} stroke={color} strokeWidth={0.8}/><rect x={x+1} y={bT} width={Math.max(cw-2,2)} height={bH} fill={color} rx={0.5}/></g>); })}
  </svg>);
}
