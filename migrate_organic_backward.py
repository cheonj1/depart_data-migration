"""
migrate_organic_backward.py — depart_data.ig_organic_insights → deplanADB.account_organic_weekly

컬럼 매핑:
  ig_organic_insights.ig_id         → ig_accounts.fb_ig_id
                                       → ad_account.ig_user_id → account_id
  ig_organic_insights.organic_views → account_organic_weekly.organic_impressions
  ig_organic_insights.updated_at    → account_organic_weekly.update_date (date)
  date_start, date_end              → date_start, date_end (동일)

중복 처리:
  account_organic_weekly에 UNIQUE 제약이 없으므로 기존 (account_id, date_start, date_end)
  조합을 미리 로드한 뒤 소스 행과 비교해 중복 삽입을 방지한다.
"""

import os
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv()


def get_conns():
    """(conn_adb, conn_new) = (deplanADB, depart_data)"""
    conn_adb = psycopg2.connect(os.getenv("deplanADB_URL"))
    conn_new = psycopg2.connect(os.getenv("depart_data_URL"))
    conn_adb.autocommit = False
    conn_new.autocommit = False
    return conn_adb, conn_new


def fetch(conn, sql, params=None):
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, params)
        return cur.fetchall()


def migrate_organic_backward():
    conn_adb, conn_new = get_conns()
    try:
        print("=== 역방향 마이그레이션 시작: ig_organic_insights → account_organic_weekly ===\n")

        # 1. depart_data.ig_accounts: id → fb_ig_id 맵
        #    (ig_organic_insights.ig_id → ig_accounts.id FK 경유)
        ig_rows = fetch(conn_new, "SELECT id, fb_ig_id FROM ig_accounts")
        ig_id_to_fb = {row['id']: row['fb_ig_id'] for row in ig_rows}

        # 2. deplanADB.ad_account: ig_user_id → account_id 맵
        #    join 조건: depart_data.ig_accounts.fb_ig_id = deplanADB.ad_account.ig_user_id
        #    동일 ig_user_id에 account_id가 여러 개이면 직접 선택
        acc_rows = fetch(conn_adb, """
            SELECT ig_user_id, account_id, account_name, fb_ad_account_id
            FROM ad_account
            WHERE ig_user_id IS NOT NULL
            ORDER BY ig_user_id, account_id
        """)
        grouped: dict = {}
        for r in acc_rows:
            grouped.setdefault(r['ig_user_id'], []).append(r)

        fb_to_account_id: dict = {}
        for ig_user_id, candidates in grouped.items():
            if len(candidates) == 1:
                fb_to_account_id[ig_user_id] = candidates[0]['account_id']
            else:
                print(f"\n  ig_user_id={ig_user_id} 에 연결된 ad_account가 {len(candidates)}개입니다:")
                for i, c in enumerate(candidates):
                    print(f"    [{i}] account_id={c['account_id']}  "
                          f"account_name={c['account_name']}  "
                          f"fb_ad_account_id={c['fb_ad_account_id']}")
                while True:
                    choice = input("  사용할 번호를 입력하세요: ").strip()
                    if choice.isdigit() and 0 <= int(choice) < len(candidates):
                        fb_to_account_id[ig_user_id] = candidates[int(choice)]['account_id']
                        break
                    print(f"  0~{len(candidates)-1} 사이의 숫자를 입력해주세요.")

        # 3. 대상 테이블의 기존 (account_id, date_start, date_end) 로드 — 중복 삽입 방지
        existing_rows = fetch(conn_adb, """
            SELECT account_id, date_start, date_end FROM account_organic_weekly
        """)
        existing_keys = {
            (row['account_id'], row['date_start'], row['date_end'])
            for row in existing_rows
        }
        print(f"    기존 account_organic_weekly: {len(existing_keys)}건")

        # 4. 소스 데이터 읽기
        src_rows = fetch(conn_new, """
            SELECT ig_id, date_start, date_end, organic_views, updated_at
            FROM ig_organic_insights
            ORDER BY ig_id, date_start
        """)
        print(f"    소스 ig_organic_insights: {len(src_rows)}건")

        # 5. 변환 및 필터링
        to_insert = []
        skipped_no_map = 0
        skipped_duplicate = 0

        for row in src_rows:
            # ig_organic_insights.ig_id → ig_accounts.fb_ig_id
            fb_ig_id   = ig_id_to_fb.get(row['ig_id'])
            # ig_accounts.fb_ig_id = ad_account.ig_user_id → account_id
            account_id = fb_to_account_id.get(fb_ig_id) if fb_ig_id else None

            if account_id is None:
                skipped_no_map += 1
                continue

            # ad_account.account_id = account_organic_weekly.account_id
            key = (account_id, row['date_start'], row['date_end'])
            if key in existing_keys:
                skipped_duplicate += 1
                continue

            update_date = row['updated_at'].date() if row.get('updated_at') else None
            to_insert.append((
                account_id,
                row['date_start'],
                row['date_end'],
                row.get('organic_views'),
                update_date,
            ))
            existing_keys.add(key)  # 소스 내 중복도 방지

        # 6. 삽입
        if to_insert:
            with conn_adb.cursor() as cur:
                psycopg2.extras.execute_values(cur, """
                    INSERT INTO account_organic_weekly
                        (account_id, date_start, date_end, organic_impressions, update_date)
                    VALUES %s
                """, to_insert)
            conn_adb.commit()

        print(f"    → {len(to_insert)}건 삽입")
        print(f"    → {skipped_duplicate}건 스킵 (중복)")
        print(f"    → {skipped_no_map}건 스킵 (account_id 미매핑)")
        print("\n=== 역방향 마이그레이션 완료 ===")

    except Exception as e:
        conn_adb.rollback()
        print(f"\n오류 발생: {e}")
        raise
    finally:
        conn_adb.close()
        conn_new.close()


if __name__ == "__main__":
    migrate_organic_backward()
