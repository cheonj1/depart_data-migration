"""
migrate_ig_insights_total.py — ig_insights_total 단독 마이그레이션

  deplanDB.instagram_followers (primary) → depart_data.ig_insights_total
  deplanADB.ig_insights_cumulative       → 더블체크

이미 depart_data.ig_accounts가 채워진 상태에서 실행한다.
"""

import os
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv()


# ─────────────────────────────────────────────────────────────
# DB 연결
# ─────────────────────────────────────────────────────────────

def get_conns():
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
# ID 맵 빌드 (depart_data.ig_accounts 기준)
# ─────────────────────────────────────────────────────────────

def build_ig_fb_id_map(conn_new):
    """fb_ig_id → depart_data.ig_accounts.id"""
    rows = fetch(conn_new, "SELECT id, fb_ig_id FROM ig_accounts")
    return {row['fb_ig_id']: row['id'] for row in rows}


def build_ig_internal_id_map(conn_adb, ig_fb_id_map):
    """deplanADB.ig_account.ig_id → depart_data.ig_accounts.id"""
    rows = fetch(conn_adb, "SELECT ig_id, ig_user_id FROM ig_account")
    return {
        row['ig_id']: ig_fb_id_map[row['ig_user_id']]
        for row in rows
        if row['ig_user_id'] in ig_fb_id_map
    }


# ─────────────────────────────────────────────────────────────
# 13. ig_insights_total
# ─────────────────────────────────────────────────────────────

def migrate_ig_insights_total(conn_db, conn_adb, conn_new, ig_fb_id_map, ig_internal_id_map):
    """deplanDB.instagram_followers (primary) → ig_insights_total"""
    print("[13] ig_insights_total")

    rows = fetch(conn_db, "SELECT * FROM instagram_followers ORDER BY instagram_account_id, recorded_at")
    data = []
    for row in rows:
        new_ig_id = ig_fb_id_map.get(row['instagram_account_id'])
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

    rows = fetch(conn_adb, "SELECT * FROM ig_insights_cumulative")
    mismatches = []

    for row in rows:
        new_ig_id = ig_internal_id_map.get(row['ig_id'])
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
            if diff > 10:
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
# main
# ─────────────────────────────────────────────────────────────

def main():
    conn_adb, conn_db, conn_new = get_conns()
    try:
        print("=== ig_insights_total 마이그레이션 시작 ===\n")

        ig_fb_id_map       = build_ig_fb_id_map(conn_new)
        ig_internal_id_map = build_ig_internal_id_map(conn_adb, ig_fb_id_map)

        migrate_ig_insights_total(conn_db, conn_adb, conn_new, ig_fb_id_map, ig_internal_id_map)

        print("\n=== 완료 ===")
    except Exception as e:
        conn_new.rollback()
        print(f"\n오류 발생: {e}")
        raise
    finally:
        conn_adb.close()
        conn_db.close()
        conn_new.close()


if __name__ == "__main__":
    main()
