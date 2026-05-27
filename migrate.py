"""
migrate.py — deplanADB + deplanDB → depart_data

마이그레이션 순서:
   1. clients               ← deplanDB.clients
   2. client_info           ← deplanADB.ad_account (brand_name, init_essential)
   3. client_members        ← deplanDB.account_members
   4. business_portfolios   ← deplanADB.business_portfolio
   5. ig_accounts           ← deplanADB.ig_account
   6. ad_accounts           ← deplanADB.ad_account
   7. campaigns             ← deplanADB.campaign
   8. ad_sets               ← deplanADB.ad_set
   9. ads                   ← deplanADB.ad
  10. ad_keywords            ← deplanADB.ad_keyword
  11. ad_performance_daily   ← deplanDB.ad_demographics_cumulative (primary)
                               + deplanADB.ad_performance_daily (더블체크)
  12. ig_insights_demographics ← deplanADB.followers_demographics_daily
  13. ig_insights_total      ← deplanDB.instagram_followers (primary)
                               + deplanADB.ig_insights_cumulative (더블체크)
  14. ig_organic_insights    ← deplanADB.account_organic_weekly
"""

import os
from datetime import datetime, timezone
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv()


# ─────────────────────────────────────────────────────────────
# DB 연결
# ─────────────────────────────────────────────────────────────

def get_conns():
    """(conn_adb, conn_db, conn_new) = (deplanADB, deplanDB, depart_data)"""
    conn_adb = psycopg2.connect(os.getenv("deplanADB_URL"))
    conn_db  = psycopg2.connect(os.getenv("deplanDB_URL"))
    conn_new = psycopg2.connect(os.getenv("depart_data_URL"))
    for c in (conn_adb, conn_db, conn_new):
        c.autocommit = False
    return conn_adb, conn_db, conn_new


def fetch(conn, sql, params=None):
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, params)
        return cur.fetchall()


def insert_many(conn, sql, rows):
    if not rows:
        return
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, rows)


# ─────────────────────────────────────────────────────────────
# 1. clients
# ─────────────────────────────────────────────────────────────

def migrate_clients(conn_db, conn_new):
    """deplanDB.clients → clients"""
    print("[1] clients")
    rows = fetch(conn_db, "SELECT * FROM clients ORDER BY id")
    client_id_map = {}  # deplanDB.clients.id → new clients.id

    for row in rows:
        with conn_new.cursor() as cur:
            cur.execute("""
                INSERT INTO clients (username, password, email, is_admin, is_active,
                                     last_login_at, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (row['username'], row['password'], row['email'],
                  row['is_admin'], row['is_active'], row['last_login_at'],
                  row['created_at'], row['updated_at']))
            client_id_map[row['id']] = cur.fetchone()[0]

    conn_new.commit()
    print(f"    → {len(rows)}건")
    return client_id_map


# ─────────────────────────────────────────────────────────────
# 공통: deplanDB fb_ad_account_id → old client_id 역방향 맵
# ─────────────────────────────────────────────────────────────

def build_fb_to_client_map(conn_db):
    """deplanDB.ad_accounts: fb_ad_account_id → client_id"""
    rows = fetch(conn_db, "SELECT fb_ad_account_id, client_id FROM ad_accounts")
    return {row['fb_ad_account_id']: row['client_id'] for row in rows}


# ─────────────────────────────────────────────────────────────
# 2. client_info
# ─────────────────────────────────────────────────────────────

def migrate_client_info(conn_adb, conn_db, conn_new, client_id_map):
    """deplanADB.ad_account(brand_name, init_essential) → client_info"""
    print("[2] client_info")
    fb_to_client = build_fb_to_client_map(conn_db)

    # deplanADB.ad_account 컬럼명: (fb_ad_account_id, brand_name, init_essential)
    rows = fetch(conn_adb, "SELECT fb_ad_account_id, brand_name, init_essential FROM ad_account")

    # 한 클라이언트에 여러 ad_account가 있을 수 있어 배열 합산
    merged = {}
    for row in rows:
        old_cid = fb_to_client.get(row['fb_ad_account_id'])
        new_cid = client_id_map.get(old_cid) if old_cid else None
        if new_cid is None:
            continue
        if new_cid not in merged:
            merged[new_cid] = {'brand_name': set(), 'init_essential': set()}
        if row['brand_name']:
            merged[new_cid]['brand_name'].update(row['brand_name'])
        if row['init_essential']:
            merged[new_cid]['init_essential'].update(row['init_essential'])

    data = [
        (cid,
         list(v['brand_name']) or None,
         list(v['init_essential']) or None)
        for cid, v in merged.items()
    ]
    insert_many(conn_new, """
        INSERT INTO client_info (client_id, brand_name, init_essential) VALUES %s
        ON CONFLICT (client_id) DO NOTHING
    """, data)
    conn_new.commit()
    print(f"    → {len(data)}건")


# ─────────────────────────────────────────────────────────────
# 3. client_members
# ─────────────────────────────────────────────────────────────

def migrate_client_members(conn_db, conn_new, client_id_map):
    """deplanDB.account_members → client_members"""
    print("[3] client_members")
    # account_members.ad_account_id → ad_accounts.id → ad_accounts.client_id
    acc_rows = fetch(conn_db, "SELECT id, client_id FROM ad_accounts")
    acc_to_client = {row['id']: row['client_id'] for row in acc_rows}

    # deplanDB.account_members 컬럼명: (ad_account_id, role, duty, name)
    rows = fetch(conn_db, "SELECT * FROM account_members ORDER BY id")
    data = []
    for row in rows:
        old_cid = acc_to_client.get(row['ad_account_id'])
        new_cid = client_id_map.get(old_cid) if old_cid else None
        data.append((
            new_cid, row['role'], row.get('duty'), row.get('name'),
            row.get('created_at'), row.get('updated_at'),
        ))

    insert_many(conn_new, """
        INSERT INTO client_members (client_id, role, sub_role, name, created_at, updated_at)
        VALUES %s
    """, data)
    conn_new.commit()
    print(f"    → {len(data)}건")


# ─────────────────────────────────────────────────────────────
# 4. business_portfolios
# ─────────────────────────────────────────────────────────────

def migrate_business_portfolios(conn_adb, conn_db, conn_new, client_id_map):
    """deplanADB.business_portfolio → business_portfolios"""
    print("[4] business_portfolios")
    fb_to_client = build_fb_to_client_map(conn_db)

    # deplanADB: business_id → fb_ad_account_id (계정 하나만 사용)
    # deplanADB.ad_account 컬럼명: (business_id, fb_ad_account_id)
    acc_rows = fetch(conn_adb, """
        SELECT DISTINCT ON (business_id) business_id, fb_ad_account_id
        FROM ad_account
        WHERE business_id IS NOT NULL
        ORDER BY business_id
    """)
    bp_to_fb = {row['business_id']: row['fb_ad_account_id'] for row in acc_rows}

    # deplanADB.business_portfolio 컬럼명: (business_id, fb_business_id, business_name)
    rows = fetch(conn_adb, "SELECT * FROM business_portfolio ORDER BY business_id")
    bp_id_map = {}  # deplanADB.business_portfolio.business_id → new business_portfolios.id

    for row in rows:
        fb_acc_id = bp_to_fb.get(row['business_id'])
        old_cid   = fb_to_client.get(fb_acc_id) if fb_acc_id else None
        new_cid   = client_id_map.get(old_cid) if old_cid else None

        with conn_new.cursor() as cur:
            cur.execute("""
                INSERT INTO business_portfolios
                    (client_id, fb_business_id, business_name, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s)
                RETURNING id
            """, (new_cid, row['business_id'], row.get('business_name'),
                  row.get('created_at'), row.get('updated_at')))
            bp_id_map[row['business_id']] = cur.fetchone()[0]

    conn_new.commit()
    print(f"    → {len(rows)}건")
    return bp_id_map


# ─────────────────────────────────────────────────────────────
# 5. ig_accounts
# ─────────────────────────────────────────────────────────────

def migrate_ig_accounts(conn_adb, conn_new, bp_id_map):
    """deplanADB.ig_account → ig_accounts"""
    print("[5] ig_accounts")

    # deplanADB.ig_account 컬럼명: (ig_id, ig_user_id, business_id, username, is_active, ...)
    rows = fetch(conn_adb, "SELECT * FROM ig_account ORDER BY ig_id")

    # ig_internal_id_map: deplanADB ig_account.ig_id → new ig_accounts.id
    # ig_fb_id_map:       ig_user_id (fb_ig_id)      → new ig_accounts.id
    ig_internal_id_map = {}
    ig_fb_id_map = {}

    for row in rows:
        # ig_account.business_id를 직접 사용 (ad_account 경유 X)
        old_bp_id = row.get('business_id')
        new_bp_id = bp_id_map.get(old_bp_id) if old_bp_id else None

        with conn_new.cursor() as cur:
            cur.execute("""
                INSERT INTO ig_accounts
                    (business_portfolio_id, fb_ig_id, username, is_active,
                     connected_at, disconnected_at, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (new_bp_id, row['ig_user_id'],
                  row.get('username'), row.get('is_active', True),
                  row.get('connected_at'), row.get('disconnected_at'),
                  row.get('created_at'), row.get('updated_at')))
            new_id = cur.fetchone()[0]
            ig_internal_id_map[row['ig_id']] = new_id
            ig_fb_id_map[row['ig_user_id']] = new_id

    conn_new.commit()
    print(f"    → {len(rows)}건")
    return ig_internal_id_map, ig_fb_id_map


# ─────────────────────────────────────────────────────────────
# 6. ad_accounts
# ─────────────────────────────────────────────────────────────

def migrate_ad_accounts(conn_adb, conn_new, bp_id_map, ig_fb_id_map):
    """deplanADB.ad_account → ad_accounts"""
    print("[6] ad_accounts")

    # deplanADB.ad_account 컬럼명: (account_id, business_id, ig_user_id, fb_ad_account_id, account_name, account_status)
    rows = fetch(conn_adb, "SELECT * FROM ad_account ORDER BY account_id")
    acc_id_map = {}  # deplanADB.ad_account.account_id → new ad_accounts.id

    for row in rows:
        new_bp_id = bp_id_map.get(row['business_id'])
        new_ig_id = ig_fb_id_map.get(row['ig_user_id']) if row.get('ig_user_id') else None

        with conn_new.cursor() as cur:
            cur.execute("""
                INSERT INTO ad_accounts
                    (business_portfolio_id, ig_account_id, fb_ad_account_id,
                     name, account_status, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (new_bp_id, new_ig_id, row['fb_ad_account_id'],
                  row.get('account_name'), row.get('account_status'),
                  row.get('created_at'),
                  row.get('updated_at') or row.get('created_at')))
            acc_id_map[row['account_id']] = cur.fetchone()[0]

    conn_new.commit()
    print(f"    → {len(rows)}건")
    return acc_id_map


# ─────────────────────────────────────────────────────────────
# 7. campaigns
# ─────────────────────────────────────────────────────────────

def migrate_campaigns(conn_adb, conn_new, acc_id_map):
    """deplanADB.campaign → campaigns"""
    print("[7] campaigns")

    # deplanADB.campaign.account_id 직접 사용
    rows = fetch(conn_adb, "SELECT * FROM campaign ORDER BY campaign_id")
    camp_id_map = {}

    for row in rows:
        old_acc_id = row.get('account_id')
        new_acc_id = acc_id_map.get(old_acc_id) if old_acc_id else None

        with conn_new.cursor() as cur:
            cur.execute("""
                INSERT INTO campaigns
                    (ad_account_id, fb_campaign_id, name,
                     objective, status, effective_status, fb_created_time, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (new_acc_id, row['fb_campaign_id'], row.get('campaign_name'),
                  row.get('objective'), row.get('status'), None,  # deplanADB에 effective_status 없음
                  row.get('created_time'),
                  row.get('created_at') or row.get('updated_at'),
                  row.get('updated_at')))
            camp_id_map[row['campaign_id']] = cur.fetchone()[0]

    conn_new.commit()
    print(f"    → {len(rows)}건")
    return camp_id_map


# ─────────────────────────────────────────────────────────────
# 8. ad_sets
# ─────────────────────────────────────────────────────────────

def migrate_ad_sets(conn_adb, conn_new, camp_id_map):
    """deplanADB.ad_set → ad_sets"""
    print("[8] ad_sets")

    # deplanADB.ad_set 컬럼명: (ad_set_id, campaign_id, fb_ad_set_id, ad_set_name, optimization_goal, billing_event, status, created_time)
    rows = fetch(conn_adb, "SELECT * FROM ad_set ORDER BY ad_set_id")
    ad_set_id_map = {}

    for row in rows:
        new_camp_id = camp_id_map.get(row['campaign_id'])

        with conn_new.cursor() as cur:
            cur.execute("""
                INSERT INTO ad_sets
                    (campaign_id, fb_ad_set_id, ad_set_name,
                     optimization_goal, billing_event, status, effective_status,
                     targeting_spec, fb_created_time, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (new_camp_id, row['fb_ad_set_id'], row.get('ad_set_name'),
                  row.get('optimization_goal'), row.get('billing_event'),
                  row.get('status'), row.get('effective_status'),
                  row.get('targeting_spec'), row.get('created_time'),
                  row.get('created_at') or row.get('updated_at'),
                  row.get('updated_at')))
            ad_set_id_map[row['ad_set_id']] = cur.fetchone()[0]

    conn_new.commit()
    print(f"    → {len(rows)}건")
    return ad_set_id_map


def migrate_ad_sets_additional(conn_db, conn_new):
    """deplanDB.ad_sets → ad_sets 보완
    deplanADB.ad_set에 없는 effective_status, targeting_spec을
    fb_ad_set_id 기준으로 이미 삽입된 행에 UPDATE
    """
    print("[8-2] ad_sets 보완 (deplanDB)")

    # deplanDB.ad_sets 컬럼명: (id, campaign_id, ad_account_id, fb_ad_set_id, ad_set_name, status, effective_status, targeting_spec, created_time)
    rows = fetch(conn_db, "SELECT fb_ad_set_id, effective_status, targeting_spec FROM ad_sets")

    count = 0
    for row in rows:
        with conn_new.cursor() as cur:
            cur.execute("""
                UPDATE ad_sets
                SET effective_status = %s,
                    targeting_spec   = %s
                WHERE fb_ad_set_id = %s
            """, (row.get('effective_status'), row.get('targeting_spec'), row['fb_ad_set_id']))
            count += cur.rowcount

    conn_new.commit()
    print(f"    → {count}건 업데이트")


# ─────────────────────────────────────────────────────────────
# 9. ads
# ─────────────────────────────────────────────────────────────

def migrate_ads(conn_adb, conn_new, ad_set_id_map, acc_id_map):
    """deplanADB.ad → ads"""
    print("[9] ads")

    # deplanADB.ad 컬럼명: (ad_id, ad_set_id, account_id, fb_ad_id, ad_name, body, status, source_instagram_media_id, landing_page_url, thumb_link, created_time)
    rows = fetch(conn_adb, "SELECT * FROM ad ORDER BY ad_id")
    ad_id_map = {}     # deplanADB.ad.ad_id   → new ads.id
    fb_ad_id_map = {}  # deplanADB.ad.fb_ad_id → new ads.id  (성과 데이터 매핑용)

    skipped = 0
    for row in rows:
        new_ad_set_id = ad_set_id_map.get(row['ad_set_id'])
        new_acc_id    = acc_id_map.get(row.get('account_id'))

        if new_ad_set_id is None:
            # ad_set이 deplanADB에 없음 → supplement_ads_from_db(deplanDB)에서 처리
            skipped += 1
            continue

        with conn_new.cursor() as cur:
            cur.execute("""
                INSERT INTO ads
                    (ad_set_id, account_id, fb_ad_id, ad_name, body, status, effective_status,
                     source_ig_media_id, landing_page_url, thumb_link,
                     fb_created_time, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (new_ad_set_id, new_acc_id, row['fb_ad_id'],
                  row.get('ad_name'), row.get('body'), None, row.get('status'),  # deplanADB.ad.status는 effective_status
                  row.get('source_instagram_media_id'), row.get('landing_page_url'),
                  row.get('thumb_link'), row.get('created_time'),
                  row.get('created_at') or row.get('updated_at') or datetime.now(timezone.utc),
                  row.get('updated_at') or row.get('created_at') or datetime.now(timezone.utc)))
            new_id = cur.fetchone()[0]
            ad_id_map[row['ad_id']] = new_id
            fb_ad_id_map[row['fb_ad_id']] = new_id

    conn_new.commit()
    print(f"    → {len(rows) - skipped}건 삽입, {skipped}건 스킵 (ad_set 미매핑, deplanDB 보완 예정)")
    return ad_id_map, fb_ad_id_map


# ─────────────────────────────────────────────────────────────
# 10. ad_keywords
# ─────────────────────────────────────────────────────────────

def migrate_ad_keywords(conn_adb, conn_new, ad_id_map):
    """deplanADB.ad_keyword → ad_keywords"""
    print("[10] ad_keywords")

    # deplanADB.ad_keyword 컬럼명: (ad_id, essential_keywords, variable_keywords)
    rows = fetch(conn_adb, "SELECT * FROM ad_keyword")
    data = [
        (ad_id_map[row['ad_id']], row.get('essential_keywords'),
         row.get('variable_keywords'), row.get('variable_update'))
        for row in rows if row['ad_id'] in ad_id_map
    ]
    insert_many(conn_new, """
        INSERT INTO ad_keywords (ad_id, essential_keywords, variable_keywords, updated_at)
        VALUES %s
        ON CONFLICT (ad_id) DO NOTHING
    """, data)
    conn_new.commit()
    print(f"    → {len(data)}건")


# ─────────────────────────────────────────────────────────────
# 10-2. deplanDB 보완: campaigns / ad_sets / ads
# ─────────────────────────────────────────────────────────────

def build_db_acc_id_map(conn_db, conn_new):
    """deplanDB.ad_accounts.id → new ad_accounts.id (fb_ad_account_id 기준)"""
    db_rows  = fetch(conn_db,  "SELECT id, fb_ad_account_id FROM ad_accounts")
    new_rows = fetch(conn_new, "SELECT id, fb_ad_account_id FROM ad_accounts")
    new_fb_to_id = {r['fb_ad_account_id']: r['id'] for r in new_rows}
    return {r['id']: new_fb_to_id.get(r['fb_ad_account_id']) for r in db_rows}


def supplement_campaigns_from_db(conn_db, conn_new, db_acc_id_map):
    """deplanDB.campaigns 중 새 DB에 없는 것만 추가 삽입.
    deplanDB.campaigns.id → new campaigns.id 전체 맵 반환."""
    print("[7-2] campaigns 보완 (deplanDB)")

    existing = {r['fb_campaign_id']: r['id']
                for r in fetch(conn_new, "SELECT id, fb_campaign_id FROM campaigns")}

    # deplanDB.campaigns 컬럼명: (id, ad_account_id, fb_campaign_id, name, objective, status, created_at, updated_at)
    db_rows = fetch(conn_db, "SELECT * FROM campaigns ORDER BY id")
    camp_db_id_map = {}
    inserted = 0

    for row in db_rows:
        fb_cid = row['fb_campaign_id']
        if fb_cid in existing:
            camp_db_id_map[row['id']] = existing[fb_cid]
            continue
        new_acc_id = db_acc_id_map.get(row['ad_account_id'])
        with conn_new.cursor() as cur:
            cur.execute("""
                INSERT INTO campaigns
                    (ad_account_id, fb_campaign_id, name, objective, status, effective_status,
                     fb_created_time, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (new_acc_id, fb_cid,
                  row.get('name'), row.get('objective'), row.get('status'), None,  # deplanDB.campaigns에 effective_status 없음
                  row.get('created_time') or row.get('created_at') or row.get('updated_at'),
                  row.get('created_at') or row.get('updated_at'),
                  row.get('updated_at') or row.get('created_at')))
            new_id = cur.fetchone()[0]
            camp_db_id_map[row['id']] = new_id
            existing[fb_cid] = new_id
            inserted += 1

    conn_new.commit()
    print(f"    → {inserted}건 추가")
    return camp_db_id_map


def supplement_ad_sets_from_db(conn_db, conn_new, camp_db_id_map):
    """deplanDB.ad_sets 중 새 DB에 없는 것만 추가 삽입.
    deplanDB.ad_sets.id → new ad_sets.id 전체 맵 반환."""
    print("[8-3] ad_sets 보완 (deplanDB)")

    existing = {r['fb_ad_set_id']: r['id']
                for r in fetch(conn_new, "SELECT id, fb_ad_set_id FROM ad_sets")}

    # deplanDB.ad_sets 컬럼명 확인
    db_rows = fetch(conn_db, "SELECT * FROM ad_sets ORDER BY id")
    ad_set_db_id_map = {}
    inserted = 0

    for row in db_rows:
        fb_sid = row['fb_ad_set_id']
        if fb_sid in existing:
            ad_set_db_id_map[row['id']] = existing[fb_sid]
            continue
        new_camp_id = camp_db_id_map.get(row['campaign_id'])
        with conn_new.cursor() as cur:
            cur.execute("""
                INSERT INTO ad_sets
                    (campaign_id, fb_ad_set_id, ad_set_name,
                     optimization_goal, billing_event, status, effective_status,
                     targeting_spec, fb_created_time, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (new_camp_id, fb_sid,
                  row.get('ad_set_name') or row.get('name'),
                  row.get('optimization_goal'), row.get('billing_event'),
                  row.get('status'), row.get('effective_status'),
                  row.get('targeting_spec'),
                  row.get('created_time') or row.get('created_at') or row.get('updated_at'),
                  row.get('created_at') or row.get('updated_at'),
                  row.get('updated_at') or row.get('created_at')))
            new_id = cur.fetchone()[0]
            ad_set_db_id_map[row['id']] = new_id
            existing[fb_sid] = new_id
            inserted += 1

    conn_new.commit()
    print(f"    → {inserted}건 추가")
    return ad_set_db_id_map


def supplement_ads_from_db(conn_db, conn_new, ad_set_db_id_map, db_acc_id_map, fb_ad_id_map):
    """deplanDB.ads 중 새 DB에 없는 것만 추가 삽입.
    fb_ad_id_map을 갱신해 반환."""
    print("[9-2] ads 보완 (deplanDB)")

    existing = {r['fb_ad_id']: r['id']
                for r in fetch(conn_new, "SELECT id, fb_ad_id FROM ads")}

    # TODO: deplanDB.ads 컬럼명 확인 (id, ad_set_id, ad_account_id, fb_ad_id, name, body, status, created_at, updated_at)
    db_rows = fetch(conn_db, "SELECT * FROM ads ORDER BY id")
    inserted = 0

    for row in db_rows:
        fb_aid = row['fb_ad_id']
        if fb_aid in existing:
            fb_ad_id_map[fb_aid] = existing[fb_aid]
            continue
        new_ad_set_id = ad_set_db_id_map.get(row['ad_set_id'])
        # deplanDB.ads의 account 컬럼명이 다를 수 있으므로 여러 키 시도
        new_acc_id = (db_acc_id_map.get(row.get('ad_account_id'))
                      or db_acc_id_map.get(row.get('account_id')))
        # 그래도 없으면 new_ad_set_id 기준으로 새 DB에서 역추적
        if new_acc_id is None and new_ad_set_id is not None:
            with conn_new.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("""
                    SELECT c.ad_account_id
                    FROM ad_sets s
                    JOIN campaigns c ON c.id = s.campaign_id
                    WHERE s.id = %s
                """, (new_ad_set_id,))
                res = cur.fetchone()
                if res:
                    new_acc_id = res['ad_account_id']
        with conn_new.cursor() as cur:
            cur.execute("""
                INSERT INTO ads
                    (ad_set_id, account_id, fb_ad_id, ad_name, body, status, effective_status,
                     source_ig_media_id, landing_page_url, thumb_link,
                     fb_created_time, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (new_ad_set_id, new_acc_id, fb_aid,
                  row.get('name') or row.get('ad_name'),
                  row.get('body'), row.get('status'), row.get('effective_status'),
                  row.get('source_ig_media_id'), row.get('landing_page_url'),
                  row.get('thumb_link'),
                  row.get('created_time') or row.get('created_at') or row.get('updated_at'),
                  row.get('created_at') or row.get('updated_at') or datetime.now(timezone.utc),
                  row.get('updated_at') or row.get('created_at') or datetime.now(timezone.utc)))
            new_id = cur.fetchone()[0]
            fb_ad_id_map[fb_aid] = new_id
            existing[fb_aid] = new_id
            inserted += 1

    conn_new.commit()
    print(f"    → {inserted}건 추가")
    return fb_ad_id_map


# ─────────────────────────────────────────────────────────────
# 11. ad_performance_daily
# ─────────────────────────────────────────────────────────────

def migrate_ad_performance_daily(conn_db, conn_adb, conn_new, fb_ad_id_map):
    """deplanDB.ad_demographics_cumulative → ad_performance_daily"""
    print("[11] ad_performance_daily")

    # deplanDB.ads: id → fb_ad_id 맵
    # deplanDB.ads 테이블명/컬럼명 확인
    db_ads = fetch(conn_db, "SELECT id, fb_ad_id FROM ads")
    db_ad_to_fb = {row['id']: row['fb_ad_id'] for row in db_ads}

    # deplanDB.ad_demographics_cumulative 컬럼명: (ad_id, age_range, gender, as_of_date, reach, impressions, clicks, spend)
    rows = fetch(conn_db, "SELECT * FROM ad_demographics_cumulative ORDER BY ad_id, as_of_date, age_range, gender")

    data = []
    skipped = 0
    for row in rows:
        fb_ad_id  = db_ad_to_fb.get(row['ad_id'])
        new_ad_id = fb_ad_id_map.get(fb_ad_id) if fb_ad_id else None
        if new_ad_id is None:
            skipped += 1
            continue
        data.append((
            new_ad_id,
            row.get('age_range'), row.get('gender'), row.get('as_of_date'),
            row.get('reach'), row.get('impressions'), row.get('clicks'),
            row.get('spend'), row.get('updated_at'),
        ))

    insert_many(conn_new, """
        INSERT INTO ad_performance_daily (
            ad_id, age_range, gender, as_of_date,
            reach, impressions, clicks, spend, updated_at
        ) VALUES %s
        ON CONFLICT (ad_id, age_range, gender, as_of_date) DO NOTHING
    """, data)
    conn_new.commit()
    print(f"    → {len(data)}건 삽입, {skipped}건 스킵 (ad 미매핑)")

    _check_ad_performance(conn_adb, conn_new, fb_ad_id_map)


def _check_ad_performance(conn_adb, conn_new, fb_ad_id_map):
    """deplanADB.ad_performance_daily와 새 DB 비교 더블체크 (bulk 방식)"""
    print("  [더블체크] ad_performance_daily")

    # deplanADB: fb_ad_id → new_ad_id 역맵 (매핑된 것만)
    a_ads = fetch(conn_adb, "SELECT ad_id, fb_ad_id FROM ad")
    a_ad_to_new = {}
    for row in a_ads:
        new_id = fb_ad_id_map.get(row['fb_ad_id'])
        if new_id:
            a_ad_to_new[row['ad_id']] = new_id

    if not a_ad_to_new:
        print("    ✓ 비교 대상 없음")
        return

    # deplanADB 집계: new_ad_id별 총 impressions
    a_rows = fetch(conn_adb, """
        SELECT ad_id, SUM(impressions) AS total_impr
        FROM ad_performance_daily
        GROUP BY ad_id
    """)
    a_totals = {}
    for row in a_rows:
        new_ad_id = a_ad_to_new.get(row['ad_id'])
        if new_ad_id:
            a_totals[new_ad_id] = row['total_impr'] or 0

    # 새 DB 집계: 동일 ad_id별 총 impressions
    new_rows = fetch(conn_new, """
        SELECT ad_id, SUM(impressions) AS total_impr
        FROM ad_performance_daily
        WHERE ad_id = ANY(%s)
        GROUP BY ad_id
    """, (list(a_totals.keys()),))
    new_totals = {row['ad_id']: (row['total_impr'] or 0) for row in new_rows}

    mismatches = []
    for new_ad_id, a_impr in a_totals.items():
        n_impr = new_totals.get(new_ad_id, 0)
        diff = abs(a_impr - n_impr)
        if diff > 0:
            mismatches.append({
                'new_ad_id': new_ad_id,
                'a_impr': a_impr,
                'new_impr': n_impr,
                'diff': diff,
            })

    mismatches.sort(key=lambda x: x['diff'], reverse=True)
    if mismatches:
        print(f"    ⚠ impressions 합계 차이 {len(mismatches)}건 (상위 10건):")
        for m in mismatches[:10]:
            print(f"      new_ad_id={m['new_ad_id']} "
                  f"deplanADB={m['a_impr']} new={m['new_impr']} diff={m['diff']}")
    else:
        print("    ✓ 차이 없음")


# ─────────────────────────────────────────────────────────────
# 12. ig_insights_demographics
# ─────────────────────────────────────────────────────────────

def migrate_ig_insights_demographics(conn_adb, conn_new, ig_internal_id_map):
    """deplanADB.followers_demographics_daily → ig_insights_demographics"""
    print("[12] ig_insights_demographics")

    # deplanADB.followers_demographics_daily 컬럼명: (ig_id, age_range, gender, value, created_at)
    #   ig_id = deplanADB.ig_account.ig_id (내부 PK)
    rows = fetch(conn_adb, "SELECT * FROM followers_demographics_daily")
    data = []
    for row in rows:
        new_ig_id = ig_internal_id_map.get(row['ig_id'])
        if new_ig_id is None:
            continue
        # created_at 날짜 부분을 as_of_date로 사용
        as_of_date = row['created_at'].date() if row.get('created_at') else None
        data.append((
            new_ig_id,
            row.get('age_range'), row.get('gender'), as_of_date,
            row.get('value'),
            row.get('created_at'),
            row.get('updated_at') or row.get('created_at'),
        ))

    insert_many(conn_new, """
        INSERT INTO ig_insights_demographics
            (ig_id, age_range, gender, as_of_date,
             followers, created_at, updated_at)
        VALUES %s
        ON CONFLICT (ig_id, age_range, gender, as_of_date) DO NOTHING
    """, data)
    conn_new.commit()
    print(f"    → {len(data)}건")


# ─────────────────────────────────────────────────────────────
# 13. ig_insights_total
# ─────────────────────────────────────────────────────────────

def migrate_ig_insights_total(conn_db, conn_adb, conn_new, ig_fb_id_map, ig_internal_id_map):
    """deplanDB.instagram_followers (primary) → ig_insights_total"""
    print("[13] ig_insights_total")

    # deplanDB.instagram_followers.instagram_account_id = fb_ig_id
    rows = fetch(conn_db, "SELECT * FROM instagram_followers ORDER BY instagram_account_id, recorded_at")
    data = []
    for row in rows:
        new_ig_id  = ig_fb_id_map.get(row['instagram_account_id'])
        if new_ig_id is None:
            continue
        as_of_date = row['recorded_at'].date() if row.get('recorded_at') else None
        data.append((
            new_ig_id, as_of_date,
            row.get('follower_count'), row.get('profile_views'),
            row.get('views'), row.get('likes'), row.get('comments'),
            row.get('shares'), row.get('saves'),
            row.get('total_interactions'), row.get('website_clicks'),
            row.get('updated_at'),
        ))

    insert_many(conn_new, """
        INSERT INTO ig_insights_total (
            ig_id, as_of_date,
            followers_count, profile_views,
            total_views, likes, comments, shares, saves,
            total_interactions, profile_links_taps,
            updated_at
        ) VALUES %s
        ON CONFLICT (ig_id, as_of_date) DO NOTHING
    """, data)
    conn_new.commit()
    print(f"    → {len(data)}건 삽입 (deplanDB 기준)")

    _check_ig_insights_total(conn_adb, conn_new, ig_internal_id_map)


def _check_ig_insights_total(conn_adb, conn_new, ig_internal_id_map):
    """deplanADB.ig_insights_cumulative와 새 DB 비교 더블체크"""
    print("  [더블체크] ig_insights_total")

    # deplanADB.ig_insights_cumulative 컬럼명 확인
    # ig_insights_cumulative.ig_id = deplanADB.ig_account.ig_id (내부 PK)
    # total_impressions 더블체크
    rows = fetch(conn_adb, "SELECT * FROM ig_insights_cumulative")
    mismatches = []

    for row in rows:
        new_ig_id  = ig_internal_id_map.get(row['ig_id'])
        if new_ig_id is None:
            continue
        as_of_date = row['created_at'].date() if row.get('created_at') else None

        with conn_new.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT total_reach, total_views
                FROM ig_insights_total
                WHERE ig_id = %s AND as_of_date = %s
            """, (new_ig_id, as_of_date))
            new = cur.fetchone()

        if new and new['total_reach'] is not None:
            diff = abs((new['total_reach'] or 0) - (row.get('total_reach') or 0))
            if diff > 10:  # 허용 오차
                mismatches.append({
                    'ig_id': row['ig_id'],
                    'date': as_of_date,
                    'a_reach': row.get('total_reach'),
                    'new_reach': new['total_reach'],
                    'diff': diff,
                })

    if mismatches:
        print(f"    ⚠ total_reach 차이 {len(mismatches)}건 (상위 10건):")
        for m in mismatches[:10]:
            print(f"      ig_id={m['ig_id']} date={m['date']} "
                  f"deplanADB={m['a_reach']} new={m['new_reach']} diff={m['diff']}")
    else:
        print("    ✓ 허용 오차 내 차이 없음")


# ─────────────────────────────────────────────────────────────
# 14. client_sprint_notes
# ─────────────────────────────────────────────────────────────

def migrate_client_sprint_notes(conn_db, conn_new, client_id_map):
    """deplanDB.account_sprint_notes → client_sprint_notes"""
    print("[14] client_sprint_notes")

    # account_sprint_notes.account_id → ad_accounts.client_id → client_id_map → new client_id
    # ad_accounts.id가 varchar라서 int로 캐스팅
    acc_rows = fetch(conn_db, "SELECT id::int AS id, client_id::int AS client_id FROM ad_accounts")
    acc_to_client = {row['id']: row['client_id'] for row in acc_rows}

    rows = fetch(conn_db, "SELECT * FROM account_sprint_notes ORDER BY id")
    data = []
    skipped = 0
    for row in rows:
        old_cid = acc_to_client.get(row['account_id'])
        new_cid = client_id_map.get(old_cid) if old_cid else None
        if new_cid is None:
            skipped += 1
            continue
        data.append((
            new_cid,
            row.get('sprint_number'),
            row.get('title'),
            row.get('focus'),
            row.get('objectives'),
            row.get('notes'),
            row.get('tags'),
            row.get('created_at'),
            row.get('updated_at'),
        ))

    insert_many(conn_new, """
        INSERT INTO client_sprint_notes
            (client_id, sprint_number, title, focus, objectives, notes, tags,
             created_at, updated_at)
        VALUES %s
    """, data)
    conn_new.commit()
    print(f"    → {len(data)}건 삽입, {skipped}건 스킵 (client 미매핑)")


# ─────────────────────────────────────────────────────────────
# 15. ig_organic_insights
# ─────────────────────────────────────────────────────────────

def migrate_ig_organic_insights(conn_adb, conn_new, ig_fb_id_map):
    """deplanADB.account_organic_weekly → ig_organic_insights"""
    print("[14] ig_organic_insights")

    # account_organic_weekly.account_id
    #   → ad_account.account_id → ad_account.ig_user_id
    #   → ig_fb_id_map[ig_user_id] = new ig_accounts.id
    acc_rows = fetch(conn_adb, """
        SELECT account_id, ig_user_id
        FROM ad_account
        WHERE ig_user_id IS NOT NULL
    """)
    acc_to_ig_fb = {row['account_id']: row['ig_user_id'] for row in acc_rows}

    # deplanADB.account_organic_weekly 컬럼명: (account_id, date_start, date_end, organic_impressions, update_at)
    rows = fetch(conn_adb, "SELECT * FROM account_organic_weekly ORDER BY account_id")
    data = []
    for row in rows:
        ig_user_id = acc_to_ig_fb.get(row['account_id'])
        new_ig_id  = ig_fb_id_map.get(ig_user_id) if ig_user_id else None
        if new_ig_id is None:
            continue
        # update_date는 KST date 타입 → UTC=KST 고정 시간(자정)으로 변환
        update_date = row.get('update_date')
        if update_date:
            from datetime import date as date_type
            dt = datetime.combine(update_date, datetime.min.time()).replace(tzinfo=timezone.utc)
        else:
            dt = datetime.now(timezone.utc)
        data.append((
            new_ig_id,
            row.get('date_start'), row.get('date_end'),
            row.get('organic_impressions'),
            dt, dt,  # created_at, updated_at 모두 동일 값
        ))

    insert_many(conn_new, """
        INSERT INTO ig_organic_insights
            (ig_id, date_start, date_end, organic_views, created_at, updated_at)
        VALUES %s
        ON CONFLICT (ig_id, date_start, date_end) DO NOTHING
    """, data)
    conn_new.commit()
    print(f"    → {len(data)}건")


# ─────────────────────────────────────────────────────────────
# main
# ─────────────────────────────────────────────────────────────

def migrate():
    conn_adb, conn_db, conn_new = get_conns()
    try:
        print("=== 마이그레이션 시작 ===\n")

        client_id_map = migrate_clients(conn_db, conn_new)
        migrate_client_info(conn_adb, conn_db, conn_new, client_id_map)
        migrate_client_members(conn_db, conn_new, client_id_map)

        bp_id_map = migrate_business_portfolios(conn_adb, conn_db, conn_new, client_id_map)

        ig_internal_id_map, ig_fb_id_map = migrate_ig_accounts(conn_adb, conn_new, bp_id_map)
        acc_id_map = migrate_ad_accounts(conn_adb, conn_new, bp_id_map, ig_fb_id_map)

        camp_id_map   = migrate_campaigns(conn_adb, conn_new, acc_id_map)
        ad_set_id_map = migrate_ad_sets(conn_adb, conn_new, camp_id_map)
        migrate_ad_sets_additional(conn_db, conn_new)
        ad_id_map, fb_ad_id_map = migrate_ads(conn_adb, conn_new, ad_set_id_map, acc_id_map)
        migrate_ad_keywords(conn_adb, conn_new, ad_id_map)

        # deplanDB에만 있는 campaign / ad_set / ad 보완
        db_acc_id_map    = build_db_acc_id_map(conn_db, conn_new)
        camp_db_id_map   = supplement_campaigns_from_db(conn_db, conn_new, db_acc_id_map)
        ad_set_db_id_map = supplement_ad_sets_from_db(conn_db, conn_new, camp_db_id_map)
        fb_ad_id_map     = supplement_ads_from_db(conn_db, conn_new, ad_set_db_id_map, db_acc_id_map, fb_ad_id_map)

        migrate_ad_performance_daily(conn_db, conn_adb, conn_new, fb_ad_id_map)
        migrate_ig_insights_demographics(conn_adb, conn_new, ig_internal_id_map)
        migrate_ig_insights_total(conn_db, conn_adb, conn_new, ig_fb_id_map, ig_internal_id_map)
        migrate_client_sprint_notes(conn_db, conn_new, client_id_map)
        migrate_ig_organic_insights(conn_adb, conn_new, ig_fb_id_map)

        print("\n=== 마이그레이션 완료 ===")
    except Exception as e:
        conn_new.rollback()
        print(f"\n오류 발생: {e}")
        raise
    finally:
        conn_adb.close()
        conn_db.close()
        conn_new.close()


if __name__ == "__main__":
    migrate()
