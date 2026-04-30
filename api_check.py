"""
api_check.py — Meta Graph API ↔ depart_data DB 비교 및 보완/업데이트

규칙:
  - force_update 컬럼 (status, effective_status): API 값이 있으면 항상 UPDATE
  - DB NULL + API 값 있음   → UPDATE (보완)
  - DB 값 있고 API 값 다름  → 로그만 (force 대상 - status, effective_status, ad_performance_daily)
  - API NULL + DB 값 있음   → 로그 출력, UPDATE 안 함
  - 신규 INSERT: created_at = updated_at = now()
  - UPDATE:      updated_at = now()

대상 테이블:
  business_portfolios / ig_accounts / ad_accounts /
  campaigns / ad_sets / ads / ad_performance_daily /
  ig_contents / ig_insights_total / ig_content_insights
"""

import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone

import psycopg2
import psycopg2.extras
import requests
from dotenv import load_dotenv

load_dotenv()

# ─────────────────────────────────────────────────────────────
# 설정
# ─────────────────────────────────────────────────────────────

API_VER      = "v22.0"
API_BASE     = f"https://graph.facebook.com/{API_VER}"
ACCESS_TOKEN = os.getenv("META_ACCESS_TOKEN")
DB_URL       = os.getenv("depart_data_URL")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────
# DB 헬퍼
# ─────────────────────────────────────────────────────────────

def get_conn():
    conn = psycopg2.connect(DB_URL)
    conn.autocommit = False
    return conn


def fetch(conn, sql, params=None):
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, params)
        return cur.fetchall()


def db_execute(conn, sql, params=None):
    with conn.cursor() as cur:
        cur.execute(sql, params)
    conn.commit()

# ─────────────────────────────────────────────────────────────
# Meta API 헬퍼
# ─────────────────────────────────────────────────────────────

def api_get(path, params=None, retries=3):
    url = path if path.startswith("https://") else f"{API_BASE}/{path.lstrip('/')}"
    p = {"access_token": ACCESS_TOKEN, **(params or {})}
    for attempt in range(retries):
        try:
            resp = requests.get(url, params=p, timeout=30)
            data = resp.json()
            if "error" in data:
                log.warning(f"API 오류 [{path}]: {data['error'].get('message')}")
                return None
            return data
        except Exception as e:
            if attempt == retries - 1:
                log.error(f"API 호출 실패 [{url}]: {e}")
                return None
            time.sleep(2 ** attempt)
    return None


def paginate(path, params=None):
    """커서 기반 페이지네이션으로 전체 결과 yield."""
    url = path if path.startswith("https://") else f"{API_BASE}/{path.lstrip('/')}"
    p = {"access_token": ACCESS_TOKEN, **(params or {})}
    while url:
        try:
            resp = requests.get(url, params=p, timeout=60)
            data = resp.json()
            if "error" in data:
                log.warning(f"API 오류 (paginate) [{url}]: {data['error'].get('message')}")
                break
            yield from data.get("data", [])
            url = data.get("paging", {}).get("next")
            p = {}
        except Exception as e:
            log.error(f"페이지네이션 오류 [{url}]: {e}")
            break


def ad_account_id(fb_id: str) -> str:
    return fb_id if fb_id.startswith("act_") else f"act_{fb_id}"

# ─────────────────────────────────────────────────────────────
# NULL 보완 / 강제 업데이트 공통 헬퍼
# ─────────────────────────────────────────────────────────────

def fill_and_log(conn, table, pk_col, pk_val, db_row, field_map,
                 jsonb_cols=None, force_update_cols=None):
    """
    field_map        : {db_column: api_value}
    jsonb_cols       : JSON 비교를 건너뛸 컬럼 (NULL 보완만)
    force_update_cols: API 값이 있으면 DB 값과 무관하게 항상 UPDATE할 컬럼
    """
    jsonb_cols        = set(jsonb_cols or [])
    force_update_cols = set(force_update_cols or [])
    updates = {}

    for col, api_val in field_map.items():
        db_val = db_row.get(col)

        if api_val is None:
            if db_val is not None:
                log.warning(
                    f"[API_NULL] {table}.{col} pk={pk_val} "
                    f"DB={db_val!r} API=None → UPDATE 건너뜀"
                )
            continue  # API NULL → 무시

        if col in force_update_cols:
            # API 값이 있고 DB와 다르면 무조건 업데이트
            if db_val is None or str(db_val).strip() != str(api_val).strip():
                updates[col] = api_val
        else:
            if db_val is None:
                updates[col] = api_val
            elif col not in jsonb_cols:
                if str(db_val).strip() != str(api_val).strip():
                    log.warning(
                        f"[DIFF] {table}.{col} "
                        f"pk={pk_val} DB={db_val!r} API={api_val!r}"
                    )

    if updates:
        updates["updated_at"] = datetime.now(timezone.utc)
        set_clause = ", ".join(f"{c} = %s" for c in updates)
        vals = list(updates.values()) + [pk_val]
        db_execute(conn, f"UPDATE {table} SET {set_clause} WHERE {pk_col} = %s", vals)
        log.info(f"[UPDATE] {table} pk={pk_val} → {list(updates.keys())}")


# ─────────────────────────────────────────────────────────────
# 1. business_portfolios
# ─────────────────────────────────────────────────────────────

def check_business_portfolios(conn):
    """접근 불가 business_portfolio id 집합 반환."""
    log.info("=== [1] business_portfolios ===")
    rows = fetch(conn, "SELECT * FROM business_portfolios")
    no_access = set()

    for row in rows:
        data = api_get(row['fb_business_id'], {"fields": "id,name"})
        if not data:
            log.warning(
                f"[NO_ACCESS] business_portfolios id={row['id']} "
                f"fb_business_id={row['fb_business_id']} — 하위 데이터 스킵"
            )
            no_access.add(row['id'])
            continue
        fill_and_log(conn, "business_portfolios", "id", row['id'], row, {
            "business_name": data.get("name"),
        })

    log.info(f"  → {len(rows)}건 확인, {len(no_access)}건 접근 불가")
    return no_access


# ─────────────────────────────────────────────────────────────
# 2. ig_accounts
# ─────────────────────────────────────────────────────────────

def check_ig_accounts(conn):
    """접근 불가 ig_account DB id 집합 반환."""
    log.info("=== [2] ig_accounts ===")
    rows = fetch(conn, "SELECT * FROM ig_accounts")
    no_access_ids = set()

    for row in rows:
        data = api_get(row['fb_ig_id'], {"fields": "id,username"})
        if not data:
            log.warning(
                f"[NO_ACCESS] ig_accounts id={row['id']} fb_ig_id={row['fb_ig_id']}"
            )
            no_access_ids.add(row['id'])
            continue
        fill_and_log(conn, "ig_accounts", "id", row['id'], row, {
            "username": data.get("username"),
        })

    log.info(f"  → {len(rows)}건 확인, {len(no_access_ids)}건 접근 불가")
    return no_access_ids


# ─────────────────────────────────────────────────────────────
# 3. ad_accounts
# ─────────────────────────────────────────────────────────────

def check_ad_accounts(conn):
    """접근 불가 ad_account fb_ad_account_id 집합 반환."""
    log.info("=== [3] ad_accounts ===")
    rows = fetch(conn, "SELECT * FROM ad_accounts")
    no_access_fb_ids = set()

    for row in rows:
        acc_id = ad_account_id(row['fb_ad_account_id'])
        data = api_get(acc_id, {"fields": "id,name,currency,account_status"})
        if not data:
            log.warning(
                f"[NO_ACCESS] ad_accounts id={row['id']} "
                f"fb_ad_account_id={acc_id} — 하위 데이터 스킵"
            )
            no_access_fb_ids.add(row['fb_ad_account_id'])
            continue
        fill_and_log(conn, "ad_accounts", "id", row['id'], row, {
            "name":           data.get("name"),
            "currency":       data.get("currency"),
            "account_status": data.get("account_status"),
        })

    log.info(f"  → {len(rows)}건 확인, {len(no_access_fb_ids)}건 접근 불가")
    return no_access_fb_ids


# ─────────────────────────────────────────────────────────────
# 4. campaigns  — 누락 INSERT + status/effective_status 강제 UPDATE
# ─────────────────────────────────────────────────────────────

def check_campaigns(conn, no_access_fb_acc_ids):
    log.info("=== [4] campaigns ===")

    db_map   = {r['fb_campaign_id']: r for r in fetch(conn, "SELECT * FROM campaigns")}
    acc_rows = fetch(conn, "SELECT id, fb_ad_account_id FROM ad_accounts")

    api_count = inserted = skipped_acc = 0
    now = datetime.now(timezone.utc)

    for acc in acc_rows:
        if acc['fb_ad_account_id'] in no_access_fb_acc_ids:
            skipped_acc += 1
            continue

        acc_db_id = acc['id']

        for camp in paginate(
            f"{ad_account_id(acc['fb_ad_account_id'])}/campaigns",
            {
                "fields":      "id,name,objective,status,effective_status,created_time",
                "date_preset": "maximum",
                "limit":       500,
            },
        ):
            api_count += 1
            fb_cid = camp['id']
            db_row = db_map.get(fb_cid)

            if db_row is None:
                # DB에 없는 campaign → INSERT
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO campaigns
                            (ad_account_id, fb_campaign_id, name, objective,
                             status, effective_status, fb_created_time,
                             created_at, updated_at)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        RETURNING id
                        """,
                        (
                            acc_db_id, fb_cid,
                            camp.get("name"), camp.get("objective"),
                            camp.get("status"), camp.get("effective_status"),
                            camp.get("created_time"), now, now,
                        ),
                    )
                    result = cur.fetchone()
                conn.commit()
                if result:
                    log.info(f"[INSERT] campaigns fb_campaign_id={fb_cid} id={result[0]}")
                    db_map[fb_cid] = {
                        "id": result[0], "fb_campaign_id": fb_cid,
                        "status": camp.get("status"),
                        "effective_status": camp.get("effective_status"),
                    }
                    inserted += 1
                continue

            fill_and_log(
                conn, "campaigns", "id", db_row['id'], db_row,
                {
                    "name":             camp.get("name"),
                    "objective":        camp.get("objective"),
                    "status":           camp.get("status"),
                    "effective_status": camp.get("effective_status"),
                },
                force_update_cols={"status", "effective_status"},
            )

    log.info(
        f"  → API {api_count}건 확인, {inserted}건 신규 삽입, "
        f"{skipped_acc}개 ad_account 스킵"
    )


# ─────────────────────────────────────────────────────────────
# 5. ad_sets  — 누락 INSERT + status/effective_status 강제 UPDATE
# ─────────────────────────────────────────────────────────────

def check_ad_sets(conn, no_access_fb_acc_ids):
    log.info("=== [5] ad_sets ===")

    db_map        = {r['fb_ad_set_id']: r for r in fetch(conn, "SELECT * FROM ad_sets")}
    campaign_map  = {r['fb_campaign_id']: r['id']
                     for r in fetch(conn, "SELECT id, fb_campaign_id FROM campaigns")}
    acc_rows      = fetch(conn, "SELECT fb_ad_account_id FROM ad_accounts")

    api_count = inserted = skipped_acc = no_camp = 0
    now = datetime.now(timezone.utc)

    for acc in acc_rows:
        if acc['fb_ad_account_id'] in no_access_fb_acc_ids:
            skipped_acc += 1
            continue

        for ad_set in paginate(
            f"{ad_account_id(acc['fb_ad_account_id'])}/adsets",
            {
                "fields": (
                    "id,name,campaign_id,optimization_goal,billing_event,"
                    "status,effective_status,targeting,created_time"
                ),
                "date_preset": "maximum",
                "limit":       500,
            },
        ):
            api_count += 1
            fb_asid = ad_set['id']
            db_row  = db_map.get(fb_asid)

            if db_row is None:
                # campaign_id 조회
                fb_cid      = ad_set.get("campaign_id")
                campaign_db_id = campaign_map.get(fb_cid)
                if campaign_db_id is None:
                    log.warning(
                        f"[SKIP] ad_sets fb_ad_set_id={fb_asid} "
                        f"campaign fb_campaign_id={fb_cid} DB에 없음"
                    )
                    no_camp += 1
                    continue

                targeting = ad_set.get("targeting")
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO ad_sets
                            (campaign_id, fb_ad_set_id, ad_set_name,
                             optimization_goal, billing_event,
                             status, effective_status, targeting_spec,
                             fb_created_time, created_at, updated_at)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        RETURNING id
                        """,
                        (
                            campaign_db_id, fb_asid, ad_set.get("name"),
                            ad_set.get("optimization_goal"),
                            ad_set.get("billing_event"),
                            ad_set.get("status"), ad_set.get("effective_status"),
                            json.dumps(targeting, ensure_ascii=False) if targeting else None,
                            ad_set.get("created_time"), now, now,
                        ),
                    )
                    result = cur.fetchone()
                conn.commit()
                if result:
                    log.info(f"[INSERT] ad_sets fb_ad_set_id={fb_asid} id={result[0]}")
                    db_map[fb_asid] = {
                        "id": result[0], "fb_ad_set_id": fb_asid,
                        "status": ad_set.get("status"),
                        "effective_status": ad_set.get("effective_status"),
                    }
                    inserted += 1
                continue

            targeting = ad_set.get("targeting")
            fill_and_log(
                conn, "ad_sets", "id", db_row['id'], db_row,
                {
                    "ad_set_name":       ad_set.get("name"),
                    "optimization_goal": ad_set.get("optimization_goal"),
                    "billing_event":     ad_set.get("billing_event"),
                    "status":            ad_set.get("status"),
                    "effective_status":  ad_set.get("effective_status"),
                    "targeting_spec":    json.dumps(targeting, ensure_ascii=False) if targeting else None,
                },
                jsonb_cols={"targeting_spec"},
                force_update_cols={"status", "effective_status"},
            )

    log.info(
        f"  → API {api_count}건 확인, {inserted}건 신규 삽입, "
        f"{no_camp}건 campaign 미매핑 스킵, {skipped_acc}개 ad_account 스킵"
    )


# ─────────────────────────────────────────────────────────────
# 6. ads  — 누락 INSERT + status/effective_status 강제 UPDATE
# ─────────────────────────────────────────────────────────────

def check_ads(conn, no_access_fb_acc_ids):
    log.info("=== [6] ads ===")

    db_map      = {r['fb_ad_id']: r for r in fetch(conn, "SELECT * FROM ads")}
    adset_map   = {r['fb_ad_set_id']: r['id']
                   for r in fetch(conn, "SELECT id, fb_ad_set_id FROM ad_sets")}
    account_map = {r['fb_ad_account_id']: r['id']
                   for r in fetch(conn, "SELECT id, fb_ad_account_id FROM ad_accounts")}
    acc_rows    = fetch(conn, "SELECT fb_ad_account_id FROM ad_accounts")

    api_count = inserted = skipped_acc = no_adset = 0
    now = datetime.now(timezone.utc)

    for acc in acc_rows:
        if acc['fb_ad_account_id'] in no_access_fb_acc_ids:
            skipped_acc += 1
            continue

        for ad in paginate(
            f"{ad_account_id(acc['fb_ad_account_id'])}/ads",
            {
                "fields": (
                    "id,name,adset_id,account_id,"
                    "status,effective_status,created_time,"
                    "creative{body,source_instagram_media_id,object_story_spec}"
                ),
                "date_preset": "maximum",
                "limit":       100,  # 응답 크기 제한으로 인한 에러 방지
            },
        ):
            api_count += 1
            fb_adid = ad['id']
            db_row  = db_map.get(fb_adid)

            creative   = ad.get("creative") or {}
            story_spec = creative.get("object_story_spec") or {}
            link_data  = story_spec.get("link_data") or {}

            if db_row is None:
                # ad_set_id / account_id 조회
                fb_asid    = ad.get("adset_id")
                adset_db_id = adset_map.get(fb_asid)
                if adset_db_id is None:
                    log.warning(
                        f"[SKIP] ads fb_ad_id={fb_adid} "
                        f"ad_set fb_ad_set_id={fb_asid} DB에 없음"
                    )
                    no_adset += 1
                    continue

                # API account_id: "act_XXXXXX" → strip prefix
                fb_acc_raw = ad.get("account_id", "")
                fb_acc_id  = fb_acc_raw.replace("act_", "") if fb_acc_raw else None
                acc_db_id  = account_map.get(fb_acc_id) if fb_acc_id else None
                if acc_db_id is None:
                    # fallback: current acc
                    acc_db_id = account_map.get(acc['fb_ad_account_id'])

                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO ads
                            (ad_set_id, account_id, fb_ad_id, ad_name, body,
                             status, effective_status,
                             source_ig_media_id, landing_page_url,
                             fb_created_time, created_at, updated_at)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        RETURNING id
                        """,
                        (
                            adset_db_id, acc_db_id, fb_adid, ad.get("name"),
                            creative.get("body"),
                            ad.get("status"), ad.get("effective_status"),
                            creative.get("source_instagram_media_id"),
                            link_data.get("link"),
                            ad.get("created_time"), now, now,
                        ),
                    )
                    result = cur.fetchone()
                conn.commit()
                if result:
                    log.info(f"[INSERT] ads fb_ad_id={fb_adid} id={result[0]}")
                    db_map[fb_adid] = {
                        "id": result[0], "fb_ad_id": fb_adid,
                        "status": ad.get("status"),
                        "effective_status": ad.get("effective_status"),
                    }
                    inserted += 1
                continue

            fill_and_log(
                conn, "ads", "id", db_row['id'], db_row,
                {
                    "ad_name":            ad.get("name"),
                    "status":             ad.get("status"),
                    "effective_status":   ad.get("effective_status"),
                    "body":               creative.get("body"),
                    "source_ig_media_id": creative.get("source_instagram_media_id"),
                    "landing_page_url":   link_data.get("link"),
                },
                force_update_cols={"status", "effective_status"},
            )

    log.info(
        f"  → API {api_count}건 확인, {inserted}건 신규 삽입, "
        f"{no_adset}건 ad_set 미매핑 스킵, {skipped_acc}개 ad_account 스킵"
    )


# ─────────────────────────────────────────────────────────────
# 7. ad_performance_daily  — INSERT 누락 / UPDATE 차이
# ─────────────────────────────────────────────────────────────

INSIGHT_FIELDS = ",".join([
    "ad_id", "adset_id",
    "reach", "impressions", "clicks", "ctr", "frequency", "spend",
    "actions", "action_values", "purchase_roas",
    "cpc", "cpm",
    "video_p25_watched_actions",
    "video_p50_watched_actions",
    "video_p75_watched_actions",
    "video_p100_watched_actions",
    "video_thruplay_watched_actions",
])

# 표준 픽셀 이벤트 → Meta API action_type 매핑 (우선순위 순)
_STD_EVENT_TO_ACTIONS: dict[str, list[str]] = {
    "PURCHASE":              ["purchase", "offsite_conversion.fb_pixel_purchase"],
    "LEAD":                  ["lead", "offsite_conversion.fb_pixel_lead"],
    "COMPLETE_REGISTRATION": ["complete_registration"],
    "ADD_TO_CART":           ["add_to_cart"],
    "INITIATE_CHECKOUT":     ["initiate_checkout"],
    "VIEW_CONTENT":          ["view_content"],
    "SEARCH":                ["search"],
    "SUBSCRIBE":             ["subscribe"],
    "CONTACT":               ["contact"],
    "FIND_LOCATION":         ["find_location"],
    "SCHEDULE":              ["schedule"],
    "START_TRIAL":           ["start_trial"],
    "SUBMIT_APPLICATION":    ["submit_application"],
    "DONATE":                ["donate"],
}

# 표준 픽셀 이벤트 → 한국어 결과명
_STD_EVENT_NAMES: dict[str, str] = {
    "PURCHASE":              "구매",
    "LEAD":                  "리드",
    "COMPLETE_REGISTRATION": "등록 완료",
    "ADD_TO_CART":           "장바구니 담기",
    "INITIATE_CHECKOUT":     "결제 시작",
    "VIEW_CONTENT":          "콘텐츠 조회",
    "SEARCH":                "검색",
    "SUBSCRIBE":             "구독",
    "CONTACT":               "연락",
    "FIND_LOCATION":         "위치 찾기",
    "SCHEDULE":              "일정 예약",
    "START_TRIAL":           "무료 체험 시작",
    "SUBMIT_APPLICATION":    "신청서 제출",
    "DONATE":                "기부",
}

# optimization_goal → (결과명, action_type 목록)
# action_type 목록이 None  → reach / impressions 필드 직접 참조
# action_type 목록이 []    → 해당 없음(count 산출 불가)
_OPT_GOAL_TO_RESULT: dict[str, tuple[str, list[str] | None]] = {
    # ── 트래픽 ──
    "LINK_CLICKS":           ("링크 클릭",              ["link_click"]),
    "LANDING_PAGE_VIEWS":    ("랜딩 페이지 조회",        ["landing_page_view"]),
    # ── 참여 ──
    "POST_ENGAGEMENT":       ("게시물 참여",             ["post_engagement"]),
    "PAGE_LIKES":            ("페이지 좋아요",           ["like"]),
    "EVENT_RESPONSES":       ("이벤트 응답",             ["event_responses"]),
    # Instagram 프로필 방문 (optimization_goal 값이 API 버전마다 다를 수 있어 두 가지 등록)
    "VISIT_INSTAGRAM_PROFILE": ("Instagram 프로필 방문", ["instagram_profile_visit", "ig_business_profile_view"]),
    "PROFILE_VISIT":           ("Instagram 프로필 방문", ["instagram_profile_visit", "ig_business_profile_view"]),
    # ── 동영상 조회 ──
    "VIDEO_VIEWS":           ("동영상 조회",             ["video_view"]),
    "THRUPLAY":              ("ThruPlay 조회",           None),  # video_thruplay_watched_actions 별도 처리
    # ── 리드 ──
    "LEAD_GENERATION":       ("리드",                   ["leadgen.other", "onsite_conversion.lead_grouped", "lead"]),
    "QUALITY_LEAD":          ("잠재 고객",              ["leadgen.other", "lead"]),
    # ── 메시지 ──
    "CONVERSATIONS":         ("메시지 대화 시작",        ["onsite_conversion.messaging_conversation_started_7d",
                                                         "onsite_conversion.messaging_first_reply"]),
    # ── 앱 설치 ──
    "APP_INSTALLS":          ("앱 설치",                ["mobile_app_install", "app_install"]),
    "APP_INSTALLS_AND_OFFSITE_CONVERSIONS": ("앱 설치 및 전환", ["mobile_app_install"]),
    # ── 전환(구체 이벤트는 promoted_object.custom_event_type 으로 처리됨) ──
    "OFFSITE_CONVERSIONS":   ("전환",                   []),
    "VALUE":                 ("구매 전환 가치",          ["purchase", "offsite_conversion.fb_pixel_purchase", "omni_purchase"]),
    # ── 도달 / 인지도 (actions 배열 아닌 기본 지표 참조) ──
    "REACH":                 ("도달",                   None),        # reach 필드 직접 참조
    "IMPRESSIONS":           ("노출",                   None),        # impressions 필드 직접 참조
    "BRAND_AWARENESS":       ("예상 광고 회상",          []),
    # ── 기타 ──
    "STORE_VISITS":          ("매장 방문",              ["store_visit"]),
    "QUALITY_CALL":          ("전화 통화",              ["phone_call"]),
    "REPLIES":               ("답글",                   []),
}


def _act(actions, t):
    if not actions:
        return None
    for a in actions:
        if a.get("action_type") == t:
            return int(float(a["value"]))
    return None


def _actv(action_values, t):
    if not action_values:
        return None
    for a in action_values:
        if a.get("action_type") == t:
            return float(a["value"])
    return None


def _vid(arr):
    if not arr:
        return None
    total = sum(int(float(a.get("value", 0))) for a in arr)
    return total if total > 0 else None


def _roas(purchase_roas):
    if not purchase_roas:
        return None
    for r in purchase_roas:
        if r.get("action_type") in ("omni_purchase", "offsite_conversion.fb_pixel_purchase"):
            return float(r["value"])
    return float(purchase_roas[0]["value"]) if purchase_roas else None


def _act_first(actions, *types):
    """여러 action_type 중 첫 번째로 값이 있는 것을 반환 (int)."""
    for t in types:
        v = _act(actions, t)
        if v is not None:
            return v
    return None


def _actv_first(action_values, *types):
    """여러 action_type 중 첫 번째로 값이 있는 것을 반환 (float)."""
    for t in types:
        v = _actv(action_values, t)
        if v is not None:
            return v
    return None


def parse_insight(ins, goal_conv_id=None, goal_conv_name=None, goal_std_event=None, goal_opt_goal=None):
    actions       = ins.get("actions") or []
    action_values = ins.get("action_values") or []

    goal_conv_count = goal_conv_value = goal_conv_cpa = None
    if goal_conv_id:
        # 1순위: 커스텀 전환
        action_type     = f"offsite_conversion.custom.{goal_conv_id}"
        goal_conv_count = _act(actions, action_type)
        goal_conv_value = _actv(action_values, action_type)
    elif goal_std_event:
        # 2순위: 표준 픽셀 이벤트 — 매핑된 action_type 전체를 _act_first 로 한 번에 시도
        candidates = _STD_EVENT_TO_ACTIONS.get(goal_std_event, [goal_std_event.lower()])
        goal_conv_count = _act_first(actions, *candidates)
        if goal_conv_count is not None:
            goal_conv_value = _actv_first(action_values, *candidates)
    elif goal_opt_goal:
        # 3순위: optimization_goal 기반 결과 추출
        result_name, action_types = _OPT_GOAL_TO_RESULT.get(goal_opt_goal, (None, []))
        if action_types is None:
            # REACH / IMPRESSIONS → 해당 집계 필드 직접 사용
            if goal_opt_goal == "REACH":
                goal_conv_count = int(ins["reach"]) if ins.get("reach") else None
            elif goal_opt_goal == "IMPRESSIONS":
                goal_conv_count = int(ins["impressions"]) if ins.get("impressions") else None
            elif goal_opt_goal == "THRUPLAY":
                goal_conv_count = _vid(ins.get("video_thruplay_watched_actions"))
        elif action_types:
            goal_conv_count = _act_first(actions, *action_types)
            if goal_conv_count is not None:
                goal_conv_value = _actv_first(action_values, *action_types)
        # action_types == [] → 카운트 산출 불가, goal_conv_count 은 None 유지

    spend_v = float(ins["spend"]) if ins.get("spend") else None
    if goal_conv_count and goal_conv_count > 0 and spend_v:
        goal_conv_cpa = round(spend_v / goal_conv_count, 4)

    reach       = int(ins["reach"])       if ins.get("reach")       else None
    impressions = int(ins["impressions"]) if ins.get("impressions") else None
    clicks      = int(ins["clicks"])      if ins.get("clicks")      else None

    # API에서 직접 받아오되, 없으면 계산
    ctr       = float(ins["ctr"])       if ins.get("ctr")       else None
    frequency = float(ins["frequency"]) if ins.get("frequency") else None
    cpc       = float(ins["cpc"])       if ins.get("cpc")       else None
    cpm       = float(ins["cpm"])       if ins.get("cpm")       else None

    # clicks=0 포함 모든 케이스 처리 (impressions and clicks 패턴은 clicks=0을 놓침)
    if ctr is None and impressions is not None and impressions > 0 and clicks is not None:
        ctr = round(clicks / impressions, 6)
    if frequency is None and impressions is not None and reach is not None and reach > 0:
        frequency = round(impressions / reach, 4)
    if cpc is None and spend_v and clicks:
        cpc = round(spend_v / clicks, 4) if clicks > 0 else None
    if cpm is None and spend_v and impressions:
        cpm = round(spend_v / impressions * 1000, 4) if impressions > 0 else None

    # ── purchase: 여러 action_type 변형 순차 시도 ──
    # Meta API v22 에서 픽셀 구매는 "purchase" / "offsite_conversion.fb_pixel_purchase" /
    # "omni_purchase" 중 하나로 반환될 수 있음
    purchase_count = _act_first(
        actions,
        "purchase",
        "offsite_conversion.fb_pixel_purchase",
        "omni_purchase",
    )
    purchase_value = _actv_first(
        action_values,
        "purchase",
        "offsite_conversion.fb_pixel_purchase",
        "omni_purchase",
    )

    # instagram_profile_visit → 없으면 ig_business_profile_view 폴백
    ig_profile_visits = _act_first(
        actions,
        "instagram_profile_visit",
        "ig_business_profile_view",
    )

    return {
        "reach":                      reach,
        "impressions":                impressions,
        "clicks":                     clicks,
        "ctr":                        ctr,
        "frequency":                  frequency,
        "spend":                      spend_v,
        "purchase_count":             purchase_count,
        "purchase_value":             purchase_value,
        "purchase_roas":              _roas(ins.get("purchase_roas")),
        "cpc":                        cpc,
        "cpm":                        cpm,
        "link_clicks":                _act(actions, "link_click"),
        "view_content":               _act(actions, "view_content"),
        "add_to_cart":                _act(actions, "add_to_cart"),
        "initiate_checkout":          _act(actions, "initiate_checkout"),
        "complete_registration":      _act(actions, "complete_registration"),
        "instagram_profile_visits":   ig_profile_visits,
        "website_landing_page_views": _act(actions, "landing_page_view"),
        "inline_post_engagement":     _act(actions, "post_engagement"),
        "post_reactions":             _act(actions, "post_reaction"),
        "comments":                   _act(actions, "comment"),
        "post_saves":                 _act(actions, "onsite_conversion.post_save"),
        "video_views":                _act(actions, "video_view"),
        "video_p25_watched":          _vid(ins.get("video_p25_watched_actions")),
        "video_p50_watched":          _vid(ins.get("video_p50_watched_actions")),
        "video_p75_watched":          _vid(ins.get("video_p75_watched_actions")),
        "video_p100_watched":         _vid(ins.get("video_p100_watched_actions")),
        "video_thruplay_watched":     _vid(ins.get("video_thruplay_watched_actions")),
        "goal_conv_id":               goal_conv_id,
        "goal_conv_name":             goal_conv_name,
        "goal_conv_count":            goal_conv_count,
        "goal_conv_value":            goal_conv_value,
        "goal_conv_cpa":              goal_conv_cpa,
    }


def _insert_missing_ad(conn, fb_ad_id, fb_adset_id, fb_acc_id, adset_map, account_map):
    """
    /ads 엔드포인트에 나타나지 않는 완전 삭제된 광고를 직접 ID로 조회 후 INSERT.
    성공 시 새 DB id 반환, 실패 시 None.
    """
    data = api_get(fb_ad_id, {
        "fields": "id,name,adset_id,account_id,status,effective_status,created_time"
    })
    if not data or "id" not in data:
        return None

    adset_db_id = adset_map.get(fb_adset_id or data.get("adset_id"))
    if adset_db_id is None:
        return None

    raw_acc = (data.get("account_id") or "").replace("act_", "")
    acc_db_id = account_map.get(raw_acc) or account_map.get(fb_acc_id)

    now = datetime.now(timezone.utc)
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO ads
                (ad_set_id, account_id, fb_ad_id, ad_name,
                 status, effective_status, fb_created_time, created_at, updated_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            RETURNING id
            """,
            (
                adset_db_id, acc_db_id, fb_ad_id, data.get("name"),
                data.get("status"), data.get("effective_status"),
                data.get("created_time"), now, now,
            ),
        )
        result = cur.fetchone()
    conn.commit()
    if result:
        log.info(f"[INSERT] ads (삭제된 광고 폴백) fb_ad_id={fb_ad_id} id={result[0]}")
        adset_map[data.get("adset_id", "")] = adset_db_id  # 혹시 모를 캐시 갱신
        return result[0]
    return None


def check_ad_performance_daily(conn, no_access_fb_acc_ids):
    log.info("=== [7] ad_performance_daily ===")

    fb_to_ad_id     = {r['fb_ad_id']: r['id']
                       for r in fetch(conn, "SELECT id, fb_ad_id FROM ads")}
    adset_map_cache = {r['fb_ad_set_id']: r['id']
                       for r in fetch(conn, "SELECT id, fb_ad_set_id FROM ad_sets")}
    account_map_cache = {r['fb_ad_account_id']: r['id']
                         for r in fetch(conn, "SELECT id, fb_ad_account_id FROM ad_accounts")}
    acc_rows = fetch(conn, "SELECT fb_ad_account_id FROM ad_accounts")

    inserted = updated = no_ad = skipped_acc = 0
    failed_fb_ad_ids = set()   # 폴백도 실패한 ad_id 캐시 (반복 시도/로그 방지)

    for acc in acc_rows:
        if acc['fb_ad_account_id'] in no_access_fb_acc_ids:
            skipped_acc += 1
            continue
        acc_id = ad_account_id(acc['fb_ad_account_id'])
        log.info(f"  인사이트 조회: {acc_id}")

        # 커스텀 전환 목록
        custom_conv_map = {}
        for cc in paginate(f"{acc_id}/customconversions", {"fields": "id,name", "limit": 200}):
            custom_conv_map[cc['id']] = cc.get('name')

        # ad_set별 목표 전환 (커스텀 전환 우선, 표준 픽셀 이벤트, 없으면 optimization_goal 폴백)
        adset_goal_map = {}
        for ad_set in paginate(f"{acc_id}/adsets", {
            "fields":      "id,promoted_object,optimization_goal",
            "date_preset": "maximum",
            "limit":       100,
        }):
            po       = ad_set.get("promoted_object") or {}
            opt_goal = (ad_set.get("optimization_goal") or "").upper()
            custom_conv_id = po.get("custom_conversion_id")
            if custom_conv_id:
                # 1순위: 커스텀 전환
                adset_goal_map[ad_set['id']] = {
                    "conv_id":   custom_conv_id,
                    "conv_name": custom_conv_map.get(custom_conv_id, custom_conv_id),
                    "std_event": None,
                    "opt_goal":  None,
                }
            else:
                std_event = po.get("custom_event_type") or po.get("custom_event_str")
                if std_event:
                    # 2순위: 표준 픽셀 이벤트 — API 원본값 그대로 저장
                    adset_goal_map[ad_set['id']] = {
                        "conv_id":   None,
                        "conv_name": std_event,
                        "std_event": std_event,
                        "opt_goal":  None,
                    }
                elif opt_goal and opt_goal in _OPT_GOAL_TO_RESULT:
                    # 3순위: optimization_goal — API 원본값 그대로 저장
                    adset_goal_map[ad_set['id']] = {
                        "conv_id":   None,
                        "conv_name": opt_goal,
                        "std_event": None,
                        "opt_goal":  opt_goal,
                    }
        log.info(f"  goal_conv 매핑: {len(adset_goal_map)}개 adset (custom/standard/opt_goal 설정됨)")

        for ins in paginate(f"{acc_id}/insights", {
            "fields":         INSIGHT_FIELDS,
            "breakdowns":     "age,gender",
            "time_increment": "1",
            "date_preset":    "maximum",
            "level":          "ad",
            "limit":          500,
        }):
            fb_ad_id    = ins.get("ad_id")
            fb_adset_id = ins.get("adset_id")
            age_range   = ins.get("age")
            gender      = ins.get("gender")
            as_of_date  = ins.get("date_start")

            if not all([fb_ad_id, age_range, gender, as_of_date]):
                continue

            new_ad_id = fb_to_ad_id.get(fb_ad_id)
            if new_ad_id is None:
                if fb_ad_id in failed_fb_ad_ids:
                    # 이미 폴백 실패 확인된 ad_id → 조용히 스킵
                    no_ad += 10000
                    
                    continue
                # 직접 ID 조회로 삭제된 광고 폴백 INSERT 시도 (ad_id당 1회)
                new_ad_id = _insert_missing_ad(conn, fb_ad_id, fb_adset_id,
                                               acc['fb_ad_account_id'],
                                               adset_map_cache, account_map_cache)
                if new_ad_id is None:
                    log.warning(f"[MISSING] ads fb_ad_id={fb_ad_id} API에서도 조회 불가 — 이후 스킵")
                    failed_fb_ad_ids.add(fb_ad_id)
                    no_ad += 1
                    continue
                fb_to_ad_id[fb_ad_id] = new_ad_id  # 캐시 갱신

            goal_info = adset_goal_map.get(fb_adset_id, {})
            api_vals = parse_insight(
                ins,
                goal_conv_id=goal_info.get("conv_id"),
                goal_conv_name=goal_info.get("conv_name"),
                goal_std_event=goal_info.get("std_event"),
                goal_opt_goal=goal_info.get("opt_goal"),
            )

            db_rows = fetch(conn, """
                SELECT * FROM ad_performance_daily
                WHERE ad_id = %s AND age_range = %s AND gender = %s AND as_of_date = %s
            """, (new_ad_id, age_range, gender, as_of_date))

            now = datetime.now(timezone.utc)

            if not db_rows:
                # INSERT
                api_vals["created_at"] = now
                api_vals["updated_at"] = now
                cols = ["ad_id", "age_range", "gender", "as_of_date"] + list(api_vals.keys())
                vals = [new_ad_id, age_range, gender, as_of_date] + list(api_vals.values())
                col_str      = ", ".join(cols)
                placeholders = ", ".join(["%s"] * len(cols))
                with conn.cursor() as cur:
                    cur.execute(
                        f"INSERT INTO ad_performance_daily ({col_str}) VALUES ({placeholders})"
                        f" ON CONFLICT (ad_id, age_range, gender, as_of_date) DO NOTHING",
                        vals,
                    )
                conn.commit()
                inserted += 1
                continue

            db_row  = db_rows[0]
            updates = {}

            for col, api_val in api_vals.items():
                if api_val is None:
                    if db_row.get(col) is not None:
                        log.warning(
                            f"[API_NULL] ad_performance_daily.{col} "
                            f"ad_id={new_ad_id} date={as_of_date} "
                            f"age={age_range} gender={gender} → UPDATE 건너뜀"
                        )
                    continue

                db_val = db_row.get(col)
                if db_val is None:
                    updates[col] = api_val
                else:
                    # 값이 다르면 API 기준으로 UPDATE
                    try:
                        a, b = float(api_val), float(db_val)
                        if a != b:
                            updates[col] = api_val
                            log.info(
                                f"[DIFF→UPDATE] ad_performance_daily.{col} "
                                f"ad_id={new_ad_id} date={as_of_date} "
                                f"age={age_range} gender={gender} "
                                f"DB={db_val} → API={api_val}"
                            )
                    except (TypeError, ValueError):
                        if str(api_val).strip() != str(db_val).strip():
                            updates[col] = api_val

            if updates:
                updates["updated_at"] = now
                set_clause = ", ".join(f"{c} = %s" for c in updates)
                vals = list(updates.values()) + [new_ad_id, age_range, gender, as_of_date]
                with conn.cursor() as cur:
                    cur.execute(
                        f"UPDATE ad_performance_daily SET {set_clause} "
                        f"WHERE ad_id=%s AND age_range=%s AND gender=%s AND as_of_date=%s",
                        vals,
                    )
                conn.commit()
                updated += 1

    log.info(
        f"  ad_performance_daily: {inserted}건 신규 삽입 / "
        f"{updated}건 업데이트 / "
        f"{no_ad}건 ad 미매핑 스킵 / {skipped_acc}개 ad_account 스킵"
    )

    # ── NULL 파생지표 일괄 재계산 (기존 데이터 포함) ──
    fill_sqls = [
        ("ctr",
         """UPDATE ad_performance_daily
            SET ctr = ROUND(clicks::numeric / NULLIF(impressions, 0), 6), updated_at = NOW()
            WHERE ctr IS NULL AND impressions > 0 AND clicks IS NOT NULL"""),
        ("frequency",
         """UPDATE ad_performance_daily
            SET frequency = ROUND(impressions::numeric / NULLIF(reach, 0), 4), updated_at = NOW()
            WHERE frequency IS NULL AND reach IS NOT NULL AND reach > 0 AND impressions IS NOT NULL"""),
        ("cpc",
         """UPDATE ad_performance_daily
            SET cpc = ROUND(spend::numeric / NULLIF(clicks, 0), 4), updated_at = NOW()
            WHERE cpc IS NULL AND spend > 0 AND clicks > 0"""),
        ("cpm",
         """UPDATE ad_performance_daily
            SET cpm = ROUND(spend::numeric / NULLIF(impressions, 0) * 1000, 4), updated_at = NOW()
            WHERE cpm IS NULL AND spend > 0 AND impressions > 0"""),
    ]
    with conn.cursor() as cur:
        for col, sql in fill_sqls:
            cur.execute(sql)
            log.info(f"  [NULL FILL] {col}: {cur.rowcount}행 재계산")
    conn.commit()


# ─────────────────────────────────────────────────────────────
# 8. ig_contents  — 신규 INSERT / 기존 보완
# ─────────────────────────────────────────────────────────────

def check_ig_contents(conn, no_access_ig_ids):
    log.info("=== [8] ig_contents ===")

    ig_rows = fetch(conn, "SELECT id, fb_ig_id FROM ig_accounts WHERE is_active = true")
    inserted = checked = 0

    for ig in ig_rows:
        ig_db_id = ig['id']
        fb_ig_id = ig['fb_ig_id']

        if ig_db_id in no_access_ig_ids:
            log.info(f"  ig_accounts id={ig_db_id} 접근 불가 스킵")
            continue

        db_media = {
            r['fb_ig_media_id']: r
            for r in fetch(conn, "SELECT * FROM ig_contents WHERE ig_id = %s", (ig_db_id,))
        }

        for media in paginate(f"{fb_ig_id}/media", {
            "fields": "id,caption,media_type,media_product_type,permalink,timestamp",
            "limit":  100,
        }):
            fb_media_id = media.get('id')
            if not fb_media_id:
                continue

            # media_type: "IMAGE" / "VIDEO" / "CAROUSEL_ALBUM"
            # media_product_type: "FEED" / "REELS" / "STORY" / "AD"
            # → 릴스는 media_type="VIDEO" + media_product_type="REELS" 로 반환됨
            raw_type     = (media.get('media_type') or '').upper()
            product_type = (media.get('media_product_type') or '').upper()
            effective_type = (
                'REEL' if raw_type == 'VIDEO' and product_type == 'REELS'
                else raw_type
            )

            now = datetime.now(timezone.utc)

            if fb_media_id not in db_media:
                db_execute(conn, """
                    INSERT INTO ig_contents
                        (ig_id, fb_ig_media_id, caption, ig_media_type,
                         ig_permalink, ig_timestamp, created_at, updated_at)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                """, (
                    ig_db_id, fb_media_id,
                    media.get('caption'), effective_type,
                    media.get('permalink'), media.get('timestamp'),
                    now, now,
                ))
                log.info(f"[INSERT] ig_contents fb_ig_media_id={fb_media_id} type={effective_type}")
                inserted += 1
            else:
                db_row = db_media[fb_media_id]
                fill_and_log(conn, "ig_contents", "id", db_row['id'], db_row, {
                    "caption":       media.get("caption"),
                    "ig_media_type": effective_type,
                    "ig_permalink":  media.get("permalink"),
                    "ig_timestamp":  media.get("timestamp"),
                },
                force_update_cols={"ig_media_type"})
                checked += 1

    log.info(f"  ig_contents: {inserted}건 신규 삽입 / {checked}건 확인")


# ─────────────────────────────────────────────────────────────
# 9. ig_insights_total  — 일별 INSERT/UPDATE
# ─────────────────────────────────────────────────────────────

def _fetch_ig_insights_day(fb_ig_id, metrics, since_str, until_str, breakdown=None):
    """
    IG 계정 일별 인사이트 조회. 메트릭별 개별 호출로 부분 실패 허용.
    반환: {date_str: {key: value}}
    breakdown 있으면 key = "{metric}__{breakdown_value}"
    """
    since_ts = int(
        datetime.strptime(since_str, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp()
    )
    until_ts = int(
        (
            datetime.strptime(until_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            + timedelta(days=1)
        ).timestamp()
    )

    result = {}

    def _parse_response(data, metric_name):
        """API 응답에서 날짜별 값 추출."""
        for metric_obj in data.get("data", []):
            m_name = metric_obj.get("name", metric_name)
            for point in metric_obj.get("values", []):
                end_time = point.get("end_time", "")
                dt = end_time[:10] if len(end_time) >= 10 else None
                if not dt:
                    continue
                val = point.get("value")
                if breakdown and isinstance(val, dict):
                    for bk_key, bk_val in val.items():
                        result.setdefault(dt, {})[f"{m_name}__{bk_key}"] = bk_val
                else:
                    result.setdefault(dt, {})[m_name] = val

    # 메트릭별로 개별 호출 → 일부 실패해도 나머지 수집 가능
    for metric in metrics:
        params = {
            "metric": metric,
            "period": "day",
            "since":  since_ts,
            "until":  until_ts,
        }
        if breakdown:
            params["breakdown"] = breakdown

        data = api_get(f"{fb_ig_id}/insights", params)
        if data and "data" in data:
            _parse_response(data, metric)
        else:
            log.debug(f"  [IG_INSIGHTS] ig={fb_ig_id} metric={metric} breakdown={breakdown} → 응답 없음")

    return result


def check_ig_insights_total(conn, no_access_ig_ids):
    log.info("=== [9] ig_insights_total ===")

    ig_rows    = fetch(conn, "SELECT id, fb_ig_id FROM ig_accounts WHERE is_active = true")
    until_date = datetime.now(timezone.utc).date()
    since_date = until_date - timedelta(days=90)
    since_str  = since_date.strftime("%Y-%m-%d")
    until_str  = until_date.strftime("%Y-%m-%d")

    inserted = updated = 0

    for ig in ig_rows:
        ig_db_id = ig['id']
        fb_ig_id = ig['fb_ig_id']

        if ig_db_id in no_access_ig_ids:
            log.info(f"  ig_accounts id={ig_db_id} 접근 불가 스킵")
            continue

        # 날짜별 데이터 수집
        day_data = {}  # date_str → {col: val}

        # ── 1) 기본 지표 (메트릭별 개별 호출, 실패 허용) ──
        # reach, profile_views는 거의 모든 계정에서 유효
        basic_metrics = [
            "reach", "profile_views",
            "total_interactions", "likes", "comments",
            "shares", "saved", "replies", "reposts",
            "profile_links_taps",
        ]
        basic = _fetch_ig_insights_day(fb_ig_id, basic_metrics, since_str, until_str)
        for dt, v in basic.items():
            day_data.setdefault(dt, {}).update({
                "total_reach":        v.get("reach"),
                "profile_views":      v.get("profile_views"),
                "total_interactions": v.get("total_interactions"),
                "likes":              v.get("likes"),
                "comments":           v.get("comments"),
                "shares":             v.get("shares"),
                "saves":              v.get("saved"),
                "replies":            v.get("replies"),
                "reposts":            v.get("reposts"),
                "profile_links_taps": v.get("profile_links_taps"),
            })

        # ── 2) 팔로워 지표 ──
        follower = _fetch_ig_insights_day(fb_ig_id, ["follower_count"], since_str, until_str)
        for dt, v in follower.items():
            day_data.setdefault(dt, {}).update({
                "follows": v.get("follower_count"),  # 일별 순증가
            })

        # ── 3) reach breakdown: media_product_type ──
        reach_media = _fetch_ig_insights_day(
            fb_ig_id, ["reach"], since_str, until_str, breakdown="media_product_type"
        )
        for dt, v in reach_media.items():
            day_data.setdefault(dt, {}).update({
                "reach_ad":       v.get("reach__AD"),
                "reach_post":     v.get("reach__POST"),
                "reach_carousel": v.get("reach__CAROUSEL_CONTAINER"),
                "reach_reel":     v.get("reach__REEL"),
                "reach_story":    v.get("reach__STORY"),
            })

        # ── 4) views breakdown: media_product_type ──
        views_media = _fetch_ig_insights_day(
            fb_ig_id, ["views"], since_str, until_str, breakdown="media_product_type"
        )
        for dt, v in views_media.items():
            sub = {
                "views_ad":       v.get("views__AD"),
                "views_post":     v.get("views__POST"),
                "views_carousel": v.get("views__CAROUSEL_CONTAINER"),
                "views_reel":     v.get("views__REEL"),
                "views_story":    v.get("views__STORY"),
            }
            total = sum(x for x in sub.values() if x is not None) or None
            day_data.setdefault(dt, {}).update({"total_views": total, **sub})

        # ── 5) reach breakdown: follow_type ──
        reach_follow = _fetch_ig_insights_day(
            fb_ig_id, ["reach"], since_str, until_str, breakdown="follow_type"
        )
        for dt, v in reach_follow.items():
            day_data.setdefault(dt, {}).update({
                "reach_follower":      v.get("reach__FOLLOWER"),
                "reach_non_follower":  v.get("reach__NON_FOLLOWER"),
                "reach_follow_unknown": v.get("reach__UNKNOWN"),
            })

        # ── 6) views breakdown: follow_type ──
        views_follow = _fetch_ig_insights_day(
            fb_ig_id, ["views"], since_str, until_str, breakdown="follow_type"
        )
        for dt, v in views_follow.items():
            day_data.setdefault(dt, {}).update({
                "views_follower":      v.get("views__FOLLOWER"),
                "views_non_follower":  v.get("views__NON_FOLLOWER"),
                "views_follow_unknown": v.get("views__UNKNOWN"),
            })

        # ── DB upsert ──
        for date_str, cols in day_data.items():
            db_rows = fetch(conn, """
                SELECT * FROM ig_insights_total
                WHERE ig_id = %s AND as_of_date = %s
            """, (ig_db_id, date_str))

            now = datetime.now(timezone.utc)
            non_null_cols = {c: v for c, v in cols.items() if v is not None}

            if not db_rows:
                if not non_null_cols:
                    continue
                col_names    = ["ig_id", "as_of_date"] + list(non_null_cols.keys()) + ["created_at", "updated_at"]
                col_vals     = [ig_db_id, date_str] + list(non_null_cols.values()) + [now, now]
                col_str      = ", ".join(col_names)
                placeholders = ", ".join(["%s"] * len(col_names))
                db_execute(conn, f"""
                    INSERT INTO ig_insights_total ({col_str})
                    VALUES ({placeholders})
                    ON CONFLICT (ig_id, as_of_date) DO NOTHING
                """, col_vals)
                inserted += 1
            else:
                db_row  = db_rows[0]
                updates = {}
                for col, api_val in cols.items():
                    if api_val is None:
                        if db_row.get(col) is not None:
                            log.warning(
                                f"[API_NULL] ig_insights_total.{col} "
                                f"ig_id={ig_db_id} date={date_str} → UPDATE 건너뜀"
                            )
                        continue
                    db_val = db_row.get(col)
                    if db_val is None:
                        updates[col] = api_val
                    else:
                        try:
                            if float(api_val) != float(db_val):
                                updates[col] = api_val
                        except (TypeError, ValueError):
                            if str(api_val).strip() != str(db_val).strip():
                                updates[col] = api_val

                if updates:
                    updates["updated_at"] = now
                    set_clause = ", ".join(f"{c} = %s" for c in updates)
                    vals = list(updates.values()) + [ig_db_id, date_str]
                    db_execute(conn, f"""
                        UPDATE ig_insights_total SET {set_clause}
                        WHERE ig_id = %s AND as_of_date = %s
                    """, vals)
                    updated += 1

    log.info(f"  ig_insights_total: {inserted}건 신규 삽입 / {updated}건 업데이트")


# ─────────────────────────────────────────────────────────────
# 10. ig_content_insights  — 오늘 날짜 기준 upsert
# ─────────────────────────────────────────────────────────────

# 미디어 타입별 지원 메트릭
# IMAGE/CAROUSEL/VIDEO/REEL 공통 (period=lifetime)
_CONTENT_METRICS_BASE = [
    "reach", "likes", "comments", "shares", "saved",
    "total_interactions",
]
# VIDEO/REEL 전용 (period=lifetime) — IMAGE/CAROUSEL은 미지원
_CONTENT_METRICS_VIDEO = ["follows", "profile_visits", "profile_activity"]
# REEL 전용 (period=lifetime)
_CONTENT_METRICS_REELS = ["ig_reels_avg_watch_time", "ig_reels_video_view_total_time"]


def _parse_content_items(data_items, result):
    """data 배열을 파싱해 result dict에 누적.
    Meta API 응답 포맷: values 배열, value 직접, total_value 세 가지 모두 처리.
    """
    for item in data_items:
        name   = item.get("name")
        if not name:
            continue
        values = item.get("values") or []
        if values:
            result[name] = values[0].get("value")
        elif "total_value" in item:
            result[name] = item["total_value"]
        elif "value" in item:
            result[name] = item["value"]


def _fetch_content_insights(fb_media_id, is_video=False):
    """
    단건 콘텐츠 인사이트 조회. 미디어 타입별 3단계 요청.

    - 기본 메트릭: 모든 타입 공통, period=lifetime
    - views: VIDEO/REEL 전용, period=lifetime (IMAGE/CAROUSEL 미지원)
    - ig_reels_*: REEL 전용, period=lifetime (일반 VIDEO는 API에서 오류 반환 → 개별 무시)
    반환: {metric_name: value}
    """
    result = {}

    # ── 1차: 공통 기본 메트릭 (period=lifetime) 배치 요청 ──
    data = api_get(f"{fb_media_id}/insights", {
        "metric": ",".join(_CONTENT_METRICS_BASE),
        "period": "lifetime",
    })
    if data and "data" in data:
        _parse_content_items(data["data"], result)
    elif data is None:
        # API 자체 접근 불가 (삭제/만료 미디어 등) → 이후 요청 전부 스킵
        log.debug(f"  [CONTENT_INSIGHTS] {fb_media_id} 기본 메트릭 요청 실패 — 미디어 접근 불가, 스킵")
        return result
    else:
        # 배치 실패 시 메트릭별 개별 요청
        for metric in _CONTENT_METRICS_BASE:
            d = api_get(f"{fb_media_id}/insights", {"metric": metric, "period": "lifetime"})
            if d and "data" in d:
                _parse_content_items(d["data"], result)
            else:
                log.debug(f"  [CONTENT_INSIGHTS] {fb_media_id} metric={metric} → 응답 없음")

    if is_video:
        # ── 2차: VIDEO/REEL 전용 메트릭 (views, follows, profile_visits, profile_activity) ──
        video_metrics = ["views"] + _CONTENT_METRICS_VIDEO
        d = api_get(f"{fb_media_id}/insights", {
            "metric": ",".join(video_metrics),
            "period": "lifetime",
        })
        if d and "data" in d:
            _parse_content_items(d["data"], result)
        else:
            # 배치 실패 시 메트릭별 개별 요청
            for metric in video_metrics:
                d2 = api_get(f"{fb_media_id}/insights", {"metric": metric, "period": "lifetime"})
                if d2 and "data" in d2:
                    _parse_content_items(d2["data"], result)
                else:
                    log.debug(f"  [CONTENT_INSIGHTS] {fb_media_id} metric={metric} → 미지원")

        # ── 3차: 릴스 전용 메트릭 (period=lifetime) 배치 요청 ──
        d = api_get(f"{fb_media_id}/insights", {
            "metric": ",".join(_CONTENT_METRICS_REELS),
            "period": "lifetime",
        })
        if d and "data" in d:
            _parse_content_items(d["data"], result)
        else:
            # 배치 실패(일반 VIDEO 등) → 메트릭별 개별 시도
            for metric in _CONTENT_METRICS_REELS:
                d2 = api_get(f"{fb_media_id}/insights", {"metric": metric, "period": "lifetime"})
                if d2 and "data" in d2:
                    _parse_content_items(d2["data"], result)
                else:
                    log.debug(f"  [CONTENT_INSIGHTS] {fb_media_id} metric={metric} → 미지원 (일반 VIDEO로 추정)")

    return result


def check_ig_content_insights(conn, no_access_ig_ids):
    """
    ig_content_insights — 실행 시점의 누적값을 오늘 날짜로 INSERT/UPDATE.

    IG 콘텐츠 인사이트 API는 period=lifetime으로 게시 이후 현재까지의
    누적값만 반환한다. 매 실행마다 오늘 날짜 행에 현재 누적값을 기록한다.
      - 오늘 행 없음 → INSERT
      - 오늘 행 있음 → 값이 달라진 컬럼만 UPDATE
    과거 행은 건드리지 않는다 (당시 시점의 누적 스냅샷이므로).
    """
    log.info("=== [10] ig_content_insights ===")

    ig_rows   = fetch(conn, "SELECT id FROM ig_accounts WHERE is_active = true")
    today_str = datetime.now(timezone.utc).date().isoformat()

    inserted = updated = skipped = 0

    for ig in ig_rows:
        ig_db_id = ig['id']
        if ig_db_id in no_access_ig_ids:
            log.info(f"  ig_accounts id={ig_db_id} 접근 불가 스킵")
            continue

        contents = fetch(conn, """
            SELECT id, fb_ig_media_id, ig_media_type FROM ig_contents WHERE ig_id = %s
        """, (ig_db_id,))

        for content in contents:
            content_id  = content['id']
            fb_media_id = content['fb_ig_media_id']
            media_type  = (content.get('ig_media_type') or '').upper()
            is_video    = media_type in ("VIDEO", "REEL")

            # ── API 호출 1회: 현재 누적값 수집 ──
            raw = _fetch_content_insights(fb_media_id, is_video=is_video)
            if not raw:
                log.warning(f"[SKIP] ig_content_insights fb_ig_media_id={fb_media_id} API 응답 없음")
                skipped += 1
                continue

            api_cols = {
                "reach":                          raw.get("reach"),
                "likes":                          raw.get("likes"),
                "comments":                       raw.get("comments"),
                "shares":                         raw.get("shares"),
                "saved":                          raw.get("saved"),
                "total_interactions":             raw.get("total_interactions"),
                "views":                          raw.get("views"),
                "follows":                        raw.get("follows"),
                "profile_visits":                 raw.get("profile_visits"),
                "profile_activity":               raw.get("profile_activity"),
                "ig_reels_avg_watch_time":        raw.get("ig_reels_avg_watch_time"),
                "ig_reels_video_view_total_time": raw.get("ig_reels_video_view_total_time"),
            }
            non_null = {c: v for c, v in api_cols.items() if v is not None}
            if not non_null:
                skipped += 1
                continue

            now = datetime.now(timezone.utc)

            # ── 오늘 행 조회 ──
            existing = fetch(conn, """
                SELECT * FROM ig_content_insights
                WHERE content_id = %s AND as_of_date = %s
            """, (content_id, today_str))

            if not existing:
                # ── INSERT ──
                col_names    = ["content_id", "as_of_date"] + list(non_null.keys()) + ["created_at", "updated_at"]
                col_vals     = [content_id, today_str] + list(non_null.values()) + [now, now]
                col_str      = ", ".join(col_names)
                placeholders = ", ".join(["%s"] * len(col_names))
                db_execute(conn, f"""
                    INSERT INTO ig_content_insights ({col_str})
                    VALUES ({placeholders})
                    ON CONFLICT (content_id, as_of_date) DO NOTHING
                """, col_vals)
                inserted += 1
            else:
                # ── UPDATE: 달라진 컬럼만 ──
                db_row  = existing[0]
                updates = {}
                for col, api_val in api_cols.items():
                    if api_val is None:
                        continue
                    db_val = db_row.get(col)
                    if db_val is None:
                        updates[col] = api_val
                    else:
                        try:
                            if float(api_val) != float(db_val):
                                updates[col] = api_val
                        except (TypeError, ValueError):
                            if str(api_val).strip() != str(db_val).strip():
                                updates[col] = api_val
                if updates:
                    updates["updated_at"] = now
                    set_clause = ", ".join(f"{c} = %s" for c in updates)
                    vals = list(updates.values()) + [content_id, today_str]
                    db_execute(conn, f"""
                        UPDATE ig_content_insights SET {set_clause}
                        WHERE content_id = %s AND as_of_date = %s
                    """, vals)
                    updated += 1

    log.info(
        f"  ig_content_insights: {inserted}건 신규 삽입 / "
        f"{updated}건 업데이트 / {skipped}건 스킵"
    )


# ─────────────────────────────────────────────────────────────
# main
# ─────────────────────────────────────────────────────────────

def run():
    if not ACCESS_TOKEN:
        raise ValueError("META_ACCESS_TOKEN이 .env에 없습니다")
    if not DB_URL:
        raise ValueError("depart_data_URL이 .env에 없습니다")

    conn = get_conn()
    try:
        log.info("=== api_check 시작 ===")
        check_business_portfolios(conn)
        no_access_ig_ids   = check_ig_accounts(conn)
        no_access_fb_accs  = check_ad_accounts(conn)
        check_campaigns(conn, no_access_fb_accs)
        check_ad_sets(conn, no_access_fb_accs)
        check_ads(conn, no_access_fb_accs)
        check_ad_performance_daily(conn, no_access_fb_accs)
        check_ig_contents(conn, no_access_ig_ids)
        check_ig_insights_total(conn, no_access_ig_ids)
        check_ig_content_insights(conn, no_access_ig_ids)
        log.info("=== api_check 완료 ===")
    except Exception as e:
        conn.rollback()
        log.error(f"오류 발생: {e}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    run()
