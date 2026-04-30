"""
Meta Ads API - results / actions / 커스텀 전환 이름 통합 점검
=============================================================
  - ACCESS_TOKEN   : Meta API 액세스 토큰
  - AD_ACCOUNT_ID  : 광고 계정 ID (act_ 제외 숫자만)
  - CAMPAIGN_ID    : 조회할 캠페인 ID

사전 라이브러리 설치:
  pip install facebook-business
  python meta_results_checker_v2.py
"""

import json
import logging
from datetime import datetime
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.campaign import Campaign
from facebook_business.adobjects.ad import Ad
from facebook_business.adobjects.adaccount import AdAccount

# =============================================================================
ACCESS_TOKEN   = "EAALRoZCi9lYQBQZBWWXu67hjLrkvC17V9K5ZAK4UXvYD7Jtx34PvzhWZArK74Yla5xp0TjW6yd4SikRvu51fOCalZARRo36hgQmFQRqKTA3Uy9jy3LIDDl7C3eGSgKgFbEt4FQwEkqGw2TRgEJGDdvIU2xbEsZAjLQQZCGkCij2Gap9ZBPZCZBU3mzg5el8ZCZBftDCV"
AD_ACCOUNT_ID  = "434227940322781"     # act_ 제외
CAMPAIGN_ID    = "120244745168200526"

DATE_PRESET    = "maximum"
# =============================================================================


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


def pretty(data) -> str:
    return json.dumps(data, indent=2, ensure_ascii=False, default=str)

def log_section(title: str):
    log.info("=" * 70)
    log.info(f"  {title}")
    log.info("=" * 70)


INSIGHT_FIELDS = [
    "ad_id",
    "ad_name",
    "adset_id",
    "adset_name",
    "results",
    "cost_per_result",
    "actions",
    "action_values",
    "cost_per_action_type",
    "spend",
]


# =============================================================================
# 파싱 유틸
# =============================================================================

def parse_results(results: list) -> tuple[str, str]:
    """
    results 필드에서 (indicator_id, value) 반환
    값이 없으면 value = "0"
    """
    if not results:
        return None, "0"
    r = results[0]
    indicator = r.get("indicator", "")
    # indicator 형식: "actions:offsite_conversion.custom.{ID}"
    conv_id = indicator.split(".")[-1] if "custom." in indicator else indicator
    values = r.get("values")
    value = values[0]["value"] if values else "0"
    return conv_id, value


def parse_cost_per_result(cost_per_result: list) -> str:
    if not cost_per_result:
        return "0"
    values = cost_per_result[0].get("values")
    return values[0]["value"] if values else "0"


def get_action_value(actions: list, action_type: str) -> str:
    for a in (actions or []):
        if a.get("action_type") == action_type:
            return a.get("value", "0")
    return "0"


def get_cost_per_action(cost_per_action_type: list, action_type: str) -> str:
    for a in (cost_per_action_type or []):
        if a.get("action_type") == action_type:
            return a.get("value", "0")
    return "0"


# =============================================================================
# 커스텀 전환 이름 조회
# =============================================================================

def fetch_custom_conversion_map(ad_account_id: str) -> dict:
    """
    광고 계정의 모든 커스텀 전환 조회 → {conv_id: conv_name} 딕셔너리 반환
    """
    log_section("커스텀 전환 목록 조회")
    id_to_name = {}
    try:
        account = AdAccount(f"act_{ad_account_id}")
        convs = account.get_custom_conversions(
            fields=["id", "name", "event_source_url", "custom_event_type"]
        )
        conv_list = [dict(c) for c in convs]
        log.info(f"커스텀 전환 {len(conv_list)}개 발견:\n{pretty(conv_list)}")
        for c in conv_list:
            id_to_name[c["id"]] = c.get("name", c["id"])
    except Exception as e:
        log.error(f"커스텀 전환 조회 오류: {e}")
    return id_to_name


# =============================================================================
# 캠페인 / 광고 조회
# =============================================================================

def fetch_campaign_ads(campaign_id: str) -> list:
    campaign = Campaign(campaign_id)
    ads = campaign.get_ads(fields=["id", "name", "status", "adset_id"])
    return [dict(ad) for ad in ads]


def fetch_ad_insights(ad_id: str) -> dict | None:
    ad = Ad(ad_id)
    params = {"date_preset": DATE_PRESET}
    try:
        insights = ad.get_insights(fields=INSIGHT_FIELDS, params=params)
        rows = [dict(row) for row in insights]
        return rows[0] if rows else None
    except Exception as e:
        log.error(f"[ad_id={ad_id}] 인사이트 조회 오류: {e}")
        return None


# =============================================================================
# 메인
# =============================================================================

def main():
    log.info(f"Meta results 점검 시작 - {datetime.now().isoformat()}")
    log.info(f"AD_ACCOUNT_ID : {AD_ACCOUNT_ID}")
    log.info(f"CAMPAIGN_ID   : {CAMPAIGN_ID}")
    log.info(f"DATE_PRESET   : {DATE_PRESET}")

    FacebookAdsApi.init(access_token=ACCESS_TOKEN)
    api = FacebookAdsApi.get_default_api()
    log.info(f"Meta API 버전: {api.API_VERSION}")

    # 1. 커스텀 전환 이름 맵 구축
    conv_name_map = fetch_custom_conversion_map(AD_ACCOUNT_ID)

    # 2. 캠페인 내 광고 목록
    log_section("캠페인 내 광고 목록")
    ads = fetch_campaign_ads(CAMPAIGN_ID)
    log.info(f"총 {len(ads)}개 광고 발견:\n{pretty(ads)}")

    if not ads:
        log.warning("광고가 없습니다. CAMPAIGN_ID를 확인해주세요.")
        return

    # 3. 광고별 분석
    log_section("광고별 상세 분석")

    summary_rows = []

    for ad in ads:
        ad_id   = ad["id"]
        ad_name = ad.get("name", "")

        log.info(f"\n{'─' * 60}")
        log.info(f"광고명  : {ad_name}")
        log.info(f"광고 ID : {ad_id}")

        row = fetch_ad_insights(ad_id)

        if row is None:
            log.warning("→ 기간 내 인사이트 데이터 없음")
            summary_rows.append({
                "ad_name": ad_name,
                "결과_지표": "-",
                "결과_건수": "-",
                "결과당_비용": "-",
                "purchase_건수": "-",
                "spend": "-",
            })
            continue

        log.info(f"원본 응답:\n{pretty(row)}")

        # results 파싱
        conv_id, result_value = parse_results(row.get("results"))
        cpr = parse_cost_per_result(row.get("cost_per_result"))

        # 커스텀 전환 이름 변환
        conv_name = conv_name_map.get(conv_id, conv_id) if conv_id else "-"

        # actions에서 purchase 파싱
        purchase_count = get_action_value(row.get("actions"), "purchase")
        purchase_cpa   = get_cost_per_action(
            row.get("cost_per_action_type"), "purchase"
        )

        # offsite_conversion.fb_pixel_purchase도 추가 확인
        pixel_purchase = get_action_value(
            row.get("actions"), "offsite_conversion.fb_pixel_purchase"
        )

        spend = row.get("spend", "-")

        log.info(
            f"\n  ── 결과(results) ──────────────────────────────\n"
            f"  결과 지표      : {conv_name} ({conv_id})\n"
            f"  결과 건수      : {result_value}\n"
            f"  결과당 비용    : {cpr}\n"
            f"\n  ── 구매(purchase) ─────────────────────────────\n"
            f"  purchase       : {purchase_count}건 / CPA {purchase_cpa}\n"
            f"  fb_pixel_purch : {pixel_purchase}건\n"
            f"\n  ── 광고비 ─────────────────────────────────────\n"
            f"  spend          : {spend}"
        )

        summary_rows.append({
            "ad_name": ad_name,
            "결과_지표": conv_name,
            "결과_건수": result_value,
            "결과당_비용": cpr,
            "purchase_건수": purchase_count,
            "pixel_purchase_건수": pixel_purchase,
            "purchase_CPA": purchase_cpa,
            "spend": spend,
        })

    # 4. 최종 요약 테이블
    log_section("최종 요약")
    log.info(f"\n{pretty(summary_rows)}")

    log.info("=" * 70)
    log.info(f"점검 완료 - {datetime.now().isoformat()}")
    log.info("=" * 70)


if __name__ == "__main__":
    main()