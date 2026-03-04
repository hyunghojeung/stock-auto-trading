"""
DB 백업 API 라우트 / Database Backup API Routes
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
파일경로: app/api/backup_routes.py

엔드포인트:
  GET  /api/backup/tables          — 전체 테이블 목록 + 행 수
  GET  /api/backup/table/{name}    — 특정 테이블 데이터 (JSON)
  GET  /api/backup/csv/{name}      — 특정 테이블 CSV 다운로드
  POST /api/backup/full            — 전체 DB 백업 (ZIP)
"""

from fastapi import APIRouter, Response
from fastapi.responses import StreamingResponse
from datetime import datetime, timedelta, timezone
import logging
import json
import csv
import io
import zipfile

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/backup", tags=["backup"])

KST = timezone(timedelta(hours=9))

try:
    from app.core.database import db
    logger.info("[백업] DB 연결 성공")
except Exception:
    db = None
    logger.warning("[백업] DB 연결 실패")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 테이블 목록 (수동 정의 — Supabase REST API 제한 우회)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
KNOWN_TABLES = [
    "stock_list",
    "pattern_definitions",
    "saved_patterns",
    "virtual_portfolios",
    "virtual_positions",
    "virtual_compare_result",
    "compound_groups",
    "surge_scan_sessions",
    "surge_scan_stocks",
    "watchlist",
]


@router.get("/tables")
async def list_tables():
    """
    전체 테이블 목록 + 행 수 조회
    List all tables with row counts
    """
    if not db:
        return {"error": "DB 미연결", "tables": []}

    results = []
    for table_name in KNOWN_TABLES:
        try:
            # 행 수 조회 (헤더만 가져와서 count)
            res = db.table(table_name).select("*", count="exact").limit(0).execute()
            row_count = res.count if res.count is not None else 0

            results.append({
                "name": table_name,
                "rows": row_count,
                "status": "ok",
            })
        except Exception as e:
            err_str = str(e)
            if "does not exist" in err_str or "404" in err_str or "relation" in err_str:
                results.append({
                    "name": table_name,
                    "rows": 0,
                    "status": "not_found",
                })
            else:
                results.append({
                    "name": table_name,
                    "rows": 0,
                    "status": f"error: {err_str[:80]}",
                })

    total_rows = sum(t["rows"] for t in results if t["status"] == "ok")
    ok_count = sum(1 for t in results if t["status"] == "ok")

    return {
        "tables": results,
        "total_tables": ok_count,
        "total_rows": total_rows,
        "timestamp": datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S KST"),
    }


@router.get("/table/{name}")
async def get_table_data(name: str, limit: int = 10000, offset: int = 0):
    """
    특정 테이블 데이터 JSON 조회
    Get table data as JSON
    """
    if not db:
        return {"error": "DB 미연결"}

    if name not in KNOWN_TABLES:
        return {"error": f"허용되지 않은 테이블: {name}"}

    try:
        res = db.table(name).select("*", count="exact").range(offset, offset + limit - 1).execute()
        return {
            "table": name,
            "rows": res.data or [],
            "count": res.count or len(res.data or []),
            "offset": offset,
            "limit": limit,
        }
    except Exception as e:
        return {"error": str(e), "table": name}


@router.get("/csv/{name}")
async def download_csv(name: str):
    """
    특정 테이블 CSV 다운로드
    Download table as CSV file
    """
    if not db:
        return Response(content="DB 미연결", status_code=500)

    if name not in KNOWN_TABLES:
        return Response(content=f"허용되지 않은 테이블: {name}", status_code=400)

    try:
        # 전체 데이터 조회 (최대 50000행)
        all_data = []
        offset = 0
        batch = 1000
        while True:
            res = db.table(name).select("*").range(offset, offset + batch - 1).execute()
            rows = res.data or []
            if not rows:
                break
            all_data.extend(rows)
            offset += batch
            if len(rows) < batch or offset >= 50000:
                break

        if not all_data:
            return Response(content="데이터 없음", status_code=404)

        # CSV 생성
        output = io.StringIO()
        columns = list(all_data[0].keys())
        writer = csv.DictWriter(output, fieldnames=columns)
        writer.writeheader()

        for row in all_data:
            # JSON 필드 문자열 변환
            clean = {}
            for k, v in row.items():
                if isinstance(v, (dict, list)):
                    clean[k] = json.dumps(v, ensure_ascii=False)
                else:
                    clean[k] = v
            writer.writerow(clean)

        csv_content = output.getvalue().encode('utf-8-sig')  # BOM 포함 (엑셀 한글 호환)
        today = datetime.now(KST).strftime("%Y%m%d_%H%M")
        filename = f"{name}_{today}.csv"

        return Response(
            content=csv_content,
            media_type="text/csv; charset=utf-8",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )

    except Exception as e:
        logger.error(f"[백업] CSV 다운로드 실패 ({name}): {e}")
        return Response(content=str(e), status_code=500)


@router.post("/full")
async def full_backup():
    """
    전체 DB 백업 (ZIP 파일 — 각 테이블 CSV)
    Full database backup as ZIP file containing CSVs
    """
    if not db:
        return Response(content="DB 미연결", status_code=500)

    try:
        zip_buffer = io.BytesIO()
        today = datetime.now(KST).strftime("%Y%m%d_%H%M")
        backup_info = {
            "backup_date": datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S KST"),
            "tables": [],
        }

        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zf:
            for table_name in KNOWN_TABLES:
                try:
                    # 데이터 조회
                    all_data = []
                    offset = 0
                    batch = 1000
                    while True:
                        res = db.table(table_name).select("*").range(offset, offset + batch - 1).execute()
                        rows = res.data or []
                        if not rows:
                            break
                        all_data.extend(rows)
                        offset += batch
                        if len(rows) < batch or offset >= 50000:
                            break

                    if not all_data:
                        backup_info["tables"].append({
                            "name": table_name, "rows": 0, "status": "empty"
                        })
                        continue

                    # CSV 생성
                    output = io.StringIO()
                    columns = list(all_data[0].keys())
                    writer = csv.DictWriter(output, fieldnames=columns)
                    writer.writeheader()
                    for row in all_data:
                        clean = {}
                        for k, v in row.items():
                            if isinstance(v, (dict, list)):
                                clean[k] = json.dumps(v, ensure_ascii=False)
                            else:
                                clean[k] = v
                        writer.writerow(clean)

                    csv_bytes = output.getvalue().encode('utf-8-sig')
                    zf.writestr(f"{table_name}.csv", csv_bytes)

                    backup_info["tables"].append({
                        "name": table_name,
                        "rows": len(all_data),
                        "size_kb": round(len(csv_bytes) / 1024, 1),
                        "status": "ok",
                    })
                    logger.info(f"[백업] {table_name}: {len(all_data)}행 백업 완료")

                except Exception as e:
                    backup_info["tables"].append({
                        "name": table_name, "rows": 0,
                        "status": f"error: {str(e)[:60]}"
                    })
                    logger.warning(f"[백업] {table_name} 실패: {e}")

            # 백업 정보 파일 추가
            zf.writestr("_backup_info.json", json.dumps(backup_info, ensure_ascii=False, indent=2))

        zip_buffer.seek(0)
        filename = f"db_backup_{today}.zip"

        return Response(
            content=zip_buffer.read(),
            media_type="application/zip",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )

    except Exception as e:
        logger.error(f"[백업] 전체 백업 실패: {e}")
        return Response(content=str(e), status_code=500)
