-- =============================================================
-- depart_data database schema
-- All timestamps stored in UTC (timestamptz)
-- as_of_date: insert only when UTC date == KST date (app-level guard)
-- =============================================================

-- -------------------------------------------------------------
-- clients
-- -------------------------------------------------------------
CREATE TABLE clients (
    id              bigserial       PRIMARY KEY,
    username        text            NOT NULL,
    password        text            NOT NULL,
    email           text,
    is_admin        bool            DEFAULT false,
    is_active       bool            NOT NULL DEFAULT true,
    last_login_at   timestamptz     DEFAULT now(),
    created_at      timestamptz     NOT NULL DEFAULT now(),
    updated_at      timestamptz     NOT NULL DEFAULT now()
);

-- -------------------------------------------------------------
-- client_info
-- -------------------------------------------------------------
CREATE TABLE client_info (
    client_id       int8            PRIMARY KEY REFERENCES clients(id),
    brand_name      text[],
    init_essential  text[]
);

-- -------------------------------------------------------------
-- client_members
-- -------------------------------------------------------------
CREATE TABLE client_members (
    id          bigserial   PRIMARY KEY,
    client_id   int8        REFERENCES clients(id),
    role        varchar(64) NOT NULL,
    sub_role    varchar(64),
    name        text,
    created_at  timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz NOT NULL DEFAULT now()
);

-- -------------------------------------------------------------
-- client_sprint_notes  (client_id는 참조 컬럼만, FK 제약 없음)
-- -------------------------------------------------------------
CREATE TABLE client_sprint_notes (
    id              bigserial   PRIMARY KEY,
    client_id       int8        NOT NULL,
    sprint_number   int4        NOT NULL,
    title           text,
    focus           text,
    objectives      jsonb,
    notes           text,
    tags            text[],
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now()
);

-- -------------------------------------------------------------
-- business_portfolios
-- -------------------------------------------------------------
CREATE TABLE business_portfolios (
    id              bigserial       PRIMARY KEY,
    client_id       int8            REFERENCES clients(id),
    fb_business_id  varchar(64)     NOT NULL,
    business_name   text,
    created_at      timestamptz     NOT NULL DEFAULT now(),
    updated_at      timestamptz     NOT NULL DEFAULT now()
);

-- -------------------------------------------------------------
-- ig_accounts  (before ad_accounts: ad_accounts FK → ig_accounts)
-- -------------------------------------------------------------
CREATE TABLE ig_accounts (
    id                      bigserial   PRIMARY KEY,
    business_portfolio_id   int8        NOT NULL REFERENCES business_portfolios(id),
    fb_ig_id                varchar(64) NOT NULL,
    username                text,
    is_active               bool        NOT NULL DEFAULT true,
    connected_at            timestamptz,
    disconnected_at         timestamptz,
    created_at              timestamptz NOT NULL DEFAULT now(),
    updated_at              timestamptz NOT NULL DEFAULT now()
);

-- -------------------------------------------------------------
-- ad_accounts
-- -------------------------------------------------------------
CREATE TABLE ad_accounts (
    id                      bigserial       PRIMARY KEY,
    business_portfolio_id   int8            NOT NULL REFERENCES business_portfolios(id),
    ig_account_id           int8            REFERENCES ig_accounts(id),
    fb_ad_account_id        varchar(64)     NOT NULL,
    name                    text,
    currency                varchar(10),
    account_status          int4,
    created_at              timestamptz     NOT NULL DEFAULT now(),
    updated_at              timestamptz     NOT NULL DEFAULT now()
);

-- -------------------------------------------------------------
-- campaigns
-- -------------------------------------------------------------
CREATE TABLE campaigns (
    id              bigserial       PRIMARY KEY,
    ad_account_id   int8            NOT NULL REFERENCES ad_accounts(id),
    fb_campaign_id  varchar(64)     NOT NULL,
    name            text,
    objective       varchar(64),
    status          varchar(64),
    effective_status varchar(64),
    fb_created_time timestamptz     NOT NULL,
    created_at      timestamptz     NOT NULL DEFAULT now(),
    updated_at      timestamptz     NOT NULL DEFAULT now()
);

-- -------------------------------------------------------------
-- ad_sets
-- -------------------------------------------------------------
CREATE TABLE ad_sets (
    id                  bigserial       PRIMARY KEY,
    campaign_id         int8            NOT NULL REFERENCES campaigns(id),
    fb_ad_set_id        varchar(64)     NOT NULL,
    ad_set_name         text,
    optimization_goal   varchar(64),
    billing_event       varchar(64),
    status              varchar(64),
    effective_status    varchar(64),
    targeting_spec      jsonb,
    fb_created_time     timestamptz     NOT NULL,
    created_at          timestamptz     NOT NULL DEFAULT now(),
    updated_at          timestamptz     NOT NULL DEFAULT now()
);

-- -------------------------------------------------------------
-- ads
-- -------------------------------------------------------------
CREATE TABLE ads (
    id                  bigserial       PRIMARY KEY,
    ad_set_id           int8            NOT NULL REFERENCES ad_sets(id),
    account_id          int8            NOT NULL REFERENCES ad_accounts(id),
    fb_ad_id            varchar(64)     NOT NULL,
    ad_name             text,
    body                text,
    status              varchar(64),
    effective_status    varchar(64),
    source_ig_media_id  varchar(64),
    landing_page_url    text,
    thumb_link          text,
    fb_created_time     timestamptz     NOT NULL,
    created_at          timestamptz     NOT NULL DEFAULT now(),
    updated_at          timestamptz     NOT NULL DEFAULT now()
);

-- -------------------------------------------------------------
-- ad_performance_daily
-- -------------------------------------------------------------
CREATE TABLE ad_performance_daily (
    ad_id                       int8        NOT NULL REFERENCES ads(id),
    age_range                   varchar(50) NOT NULL,
    gender                      varchar(50) NOT NULL,
    as_of_date                  date        NOT NULL,
    reach                       int4,
    impressions                 int4,
    clicks                      int4,
    ctr                         float8,
    frequency                   float8,
    spend                       float8,
    purchase_count              int4,
    purchase_value              float8,
    purchase_roas               float8,
    goal_conv_count             int4,
    goal_conv_value             float8,
    goal_conv_cpa               float8,
    goal_conv_name              text,
    goal_conv_id                varchar(64),
    cpc                         float8,
    cpm                         float8,
    link_clicks                 int4,
    view_content                int4,
    add_to_cart                 int4,
    initiate_checkout           int4,
    complete_registration       int4,
    instagram_profile_visits    int4,
    website_landing_page_views  int4,
    inline_post_engagement      int4,
    post_reactions              int4,
    comments                    int4,
    post_saves                  int4,
    video_views                 int4,
    video_p25_watched           int4,
    video_p50_watched           int4,
    video_p75_watched           int4,
    video_p100_watched          int4,
    video_thruplay_watched      int4,
    created_at                  timestamptz NOT NULL DEFAULT now(),
    updated_at                  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (ad_id, age_range, gender, as_of_date)
);

-- -------------------------------------------------------------
-- ad_keywords
-- -------------------------------------------------------------
CREATE TABLE ad_keywords (
    ad_id               int8        PRIMARY KEY REFERENCES ads(id),
    essential_keywords  text[],
    variable_keywords   text[],
    updated_at          timestamptz NOT NULL DEFAULT now()
);

-- -------------------------------------------------------------
-- ig_organic_insights
-- -------------------------------------------------------------
CREATE TABLE ig_organic_insights (
    ig_id           int8    NOT NULL REFERENCES ig_accounts(id),
    date_start      date    NOT NULL,
    date_end        date    NOT NULL,
    organic_views   int4,
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (ig_id, date_start, date_end)
);

-- -------------------------------------------------------------
-- ig_insights_demographics
-- -------------------------------------------------------------
CREATE TABLE ig_insights_demographics (
    ig_id           int8        NOT NULL REFERENCES ig_accounts(id),
    age_range       varchar(50) NOT NULL,
    gender          varchar(50) NOT NULL,
    as_of_date      date        NOT NULL,
    followers       int4,
    engaged_audience int4,
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (ig_id, age_range, gender, as_of_date)
);

-- -------------------------------------------------------------
-- ig_insights_total
-- -------------------------------------------------------------
CREATE TABLE ig_insights_total (
    ig_id                   int8    NOT NULL REFERENCES ig_accounts(id),
    as_of_date              date    NOT NULL,
    total_reach             int4,
    reach_ad                int4,
    reach_post              int4,
    reach_carousel          int4,
    reach_reel              int4,
    reach_story             int4,
    reach_follower          int4,
    reach_non_follower      int4,
    reach_follow_unknown    int4,
    total_views             int4,
    views_ad                int4,
    views_post              int4,
    views_carousel          int4,
    views_reel              int4,
    views_story             int4,
    views_follower          int4,
    views_non_follower      int4,
    views_follow_unknown    int4,
    total_followers         int4,
    follows                 int4,
    unfollows               int4,
    profile_views           int4,
    likes                   int4,
    comments                int4,
    shares                  int4,
    saves                   int4,
    replies                 int4,
    reposts                 int4,
    profile_links_taps      int4,
    total_interactions      int4,
    created_at              timestamptz NOT NULL DEFAULT now(),
    updated_at              timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (ig_id, as_of_date)
);

-- -------------------------------------------------------------
-- ig_contents
-- -------------------------------------------------------------
CREATE TABLE ig_contents (
    id              bigserial       PRIMARY KEY,
    ig_id           int8            NOT NULL REFERENCES ig_accounts(id),
    fb_ig_media_id  varchar(64)     NOT NULL,
    caption         text,
    ig_media_type   text            NOT NULL,
    ig_permalink    text,
    ig_timestamp    timestamptz     NOT NULL,
    created_at      timestamptz     NOT NULL DEFAULT now(),
    updated_at      timestamptz     NOT NULL DEFAULT now()
);

-- -------------------------------------------------------------
-- ig_content_insights
-- -------------------------------------------------------------
CREATE TABLE ig_content_insights (
    content_id                      int8    NOT NULL REFERENCES ig_contents(id),
    as_of_date                      date    NOT NULL,
    reach                           int4,
    likes                           int4,
    comments                        int4,
    shares                          int4,
    saved                           int4,
    total_interactions              int4,
    views                           int4,
    follows                         int4,
    profile_visits                  int4,
    profile_activity                int4,
    ig_reels_avg_watch_time         int4,
    ig_reels_video_view_total_time  int4,
    created_at                      timestamptz NOT NULL DEFAULT now(),
    updated_at                      timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (content_id, as_of_date)
);
