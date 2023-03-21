{{
    config(
        materialized='table',dist='date_created',sort=['date_created', 'contributor_id'],
        tags=["dim"]
    )
}}

WITH usr_last_login AS (

    SELECT
        ualr.user_id
        , MAX(ualr.date_created) AS last_login
    FROM {{ source('qrp','user_activity_log_records') }} ualr
    WHERE ualr.type = 'LOGIN'
        AND ualr.date_created >= (CURRENT_TIMESTAMP - INTERVAL '6 month') -- noqa
    GROUP BY 1

),  fct_spam_profile_view as (

select * from
{{ source('crowdcrm','fct_spam_profile_view') }}

), user_unsub AS (

    SELECT
        user_id
        , type
        , is_unsubscribed
    FROM (
        SELECT
            user_unsub.user_id
            , user_unsub.type
            , user_unsub.is_unsubscribed
            , ROW_NUMBER() OVER (
                PARTITION BY user_unsub.user_id
                ORDER BY user_unsub.date_updated DESC
            ) AS last_row
        FROM {{ source('qrp','user_email_unsubscriptions') }} user_unsub
        WHERE user_unsub.type = 'RECRUITMENT'
    ) tmp
    WHERE tmp.last_row = 1

)

, usr_locale AS (

    SELECT
        user_id
        , locale_id
        , spoken_fluency
        , written_fluency
    FROM (
        SELECT
            user_id
            , locale_id
            , spoken_fluency
            , written_fluency
            , ROW_NUMBER() OVER (
                PARTITION BY user_id
                ORDER BY date_created DESC
            ) AS latest_user_locale
        FROM {{ source('qrp','exp_user_locales') }}
        WHERE is_primary = 1 AND status = 'ACTIVE'
    ) temp
    WHERE temp.latest_user_locale = 1

),  fraud_buckets as (
SELECT
  sp.user_id,
  round(sp.duplicates_flags_score_pct) AS duplicates_fraud_percent,
  round(sp.geographical_flags_score_pct) AS geographical_fraud_percent,
  round(sp.productivity_flag_score_pct) AS productivity_fraud_percent,
  round(sp.ip_collusion_flags_score_pct) AS ip_collusion_fraud_percent,
  round(sp.other_collusion_flags_score_pct) AS other_collusion_fraud_percent,
  round(sp.misc_profile_flags_score_pct) AS misc_profile_fraud_percent
FROM
  fct_spam_profile_view sp

),  fraud_related_notes as(
SELECT
  DISTINCT un.user_id,
  un.text,
  un.date_created AS note_creation_date
FROM
   {{ source('qrp','user_notes') }} un
  JOIN {{ source('qrp','users') }} u ON u.id = un.user_id
WHERE
  (
    lower(un.text) LIKE '%fraud%'
    OR lower(un.text) LIKE '%suspicious%'
    OR lower(un.text) LIKE '%cheat%'
  )

), countries_payrate AS (

  SELECT *
  FROM (
    SELECT
      country
      , pay_rate
      , ROW_NUMBER() OVER (PARTITION BY country ORDER BY date_updated DESC) AS row_number
    FROM {{ source('qrp','exp_country_payrates') }}
    WHERE state IS NULL
      AND rate_type = 'HOURLY'
  ) temp
  WHERE row_number = 1

), spam_profile_summary as (

SELECT
  b1.fact_date as fact_date,
  CAST('' AS TEXT)   AS audit_escalation_id ,
  b1.id as id,
  b1.tenant_name as tenant_name,
  b3.payoneer_id as payoneer_id,
  b1.maliciousness_level as maliciousness_level,
  b1.num_flags as num_flags,
  b1.full_name as full_name,
  b1.email as email,
  b1.date_created as date_created,
  b1.ac_status as ac_status,
  b3.payoneer_status as payoneer_status,
  b1.self_reported_country as self_reported_country,
  b1.registration_ip_country as registration_ip_country,
  CAST('' AS TEXT)  AS audit_notes ,
  CAST('' AS TEXT)  AS audit_type_new ,
  CAST('' AS TEXT)  AS audit_outcome ,
  b1.flag_ip_country_mismatch as flag_ip_country_mismatch,
  b1.flag_high_risk_ip_country_mismatch as flag_high_risk_ip_country_mismatch,
  b1.flag_excessive_invoice as flag_excessive_invoice,
  b1.flag_high_pct_close_time_ip_match_malicious as flag_high_pct_close_time_ip_match_malicious,
  b1.flag_high_pct_close_time_ip_match_any_users as flag_high_pct_close_time_ip_match_any_users,
  b1.flag_high_pct_close_time_ip_rep_country_mismatch as flag_high_pct_close_time_ip_rep_country_mismatch ,
  b1.flag_half_ips_match_yukon_aqm_ips as flag_half_ips_match_yukon_aqm_ips,
  b1.flag_half_ips_match_malicious_raters as flag_half_ips_match_malicious_raters,
  b1.flag_half_ips_match_same_project_raters as flag_half_ips_match_same_project_raters,
  b1.flag_half_ips_match_active_raters as flag_half_ips_match_active_raters,
  b1.flag_half_ips_match_suspicious_raters as flag_half_ips_match_suspicious_raters,
  b1.flag_half_toolbar_ips_match_same_project_raters as flag_half_toolbar_ips_match_same_project_raters,
  b1.flag_similar_quiz_history as flag_similar_quiz_history,
  b1.flag_same_photo_id_and_hash as flag_same_photo_id_and_hash,
  b1.flag_registration_ip_vpn as flag_registration_ip_vpn,
  b1.flag_same_soundex_and_location as flag_same_soundex_and_location,
  b1.flag_at_least_10_locales as flag_at_least_10_locales,
  b1.flag_revoked_for_fraud as flag_revoked_for_fraud,
  b1.flag_resume_hash_id_match as flag_resume_hash_id_match,
  b1.flag_tumbling_email as flag_tumbling_email,
  b1.flag_tumbling_name as flag_tumbling_name,
  b1.flag_timezone_country_mismatch as flag_timezone_country_mismatch,
  b1.flag_high_risk_timezone_country_mismatch as flag_high_risk_timezone_country_mismatch,
  b1.flag_sim_ticket_bodies as flag_sim_ticket_bodies,
  b1.flag_similar_addresses as flag_similar_addresses ,
  b1.flag_same_phone_numbers as flag_same_phone_numbers,
  b1.flag_at_least_10_languages as flag_at_least_10_languages,
  b1.flag_too_many_ips as flag_too_many_ips,
  b1.flag_resume_inconsistency as flag_resume_inconsistency,
  b1.email_in_resume as email_in_resume,
  b1.first_name_in_resume as first_name_in_resume,
  b1.last_name_in_resume as last_name_in_resume,
  b1.prim_phone_in_resume as prim_phone_in_resume,
  b1.common_name_key as common_name_key,
  b1.common_email_key as common_email_key,
  b1.num_locales as num_locales,
  b1.num_self_reported_languages as num_self_reported_languages,
  b4.amt_paid_or_approved_last_45_days as amt_paid_or_approved_last_45_days,
  b5.num_ips_last_45_days as num_ips_last_45_days,
  b1.percent_ip_close_timing_mal_users_last_45_days as percent_ip_close_timing_mal_users_last_45_days,
  b1.percent_ip_close_timing_any_users_last_45_days as percent_ip_close_timing_any_users_last_45_days,
  b1.percent_ip_close_timing_rep_country_mismatch_last_45_days as percent_ip_close_timing_rep_country_mismatch_last_45_days,
  b1.percent_ips_matching_yukon_aqm_ips_last_45_days as percent_ips_matching_yukon_aqm_ips_last_45_days,
  b1.percent_ips_matching_malicious_workers_last_45_days as percent_ips_matching_malicious_workers_last_45_days,
  b1.percent_ips_matching_active_workers_last_45_days as percent_ips_matching_active_workers_last_45_days,
  b1.percent_ips_matching_same_project_raters_last_45_days as percent_ips_matching_same_project_raters_last_45_days,
  b1.percent_ips_matching_suspicious_raters_last_45_days as percent_ips_matching_suspicious_raters_last_45_days,
  b1.percent_server_ips_matching_same_project_raters_last_45_days as percent_server_ips_matching_same_project_raters_last_45_days,
  b1.num_days_billed_12_hrs_last_45_days as num_days_billed_12_hrs_last_45_days,
  b1.count_of_tumbling_emails as count_of_tumbling_emails,
  b1.count_of_tumbling_names as count_of_tumbling_names,
  b1.time_between_tumbling_emails as time_between_tumbling_emails,
  b1.time_between_tumbling_names as time_between_tumbling_names,
  b1.count_of_others_with_sim_ticket_bodies as count_of_others_with_sim_ticket_bodies,
  b1.count_quizzes_with_similar_history as count_quizzes_with_similar_history,
  b1.duplicates_flags_score_pct as duplicates_flags_score_pct,
  b1.geographical_flags_score_pct as geographical_flags_score_pct,
  b1.productivity_flag_score_pct as productivity_flag_score_pct,
  b1.ip_collusion_flags_score_pct as ip_collusion_flags_score_pct,
  b1.other_collusion_flags_score_pct as other_collusion_flags_score_pct,
  b1.misc_profile_flags_score_pct as misc_profile_flags_score_pct,
  b2.audit_type as audit_type,
  b2.audit_date as audit_date
FROM
  (
    SELECT
      u.id,
      tenants.short_name as tenant_name,
      sp.fact_date,
      u.status AS ac_status,
      sp.audit_type,
      sp.audit_date,
      sp.maliciousness_level,
      u.first_name || ' ' || u.last_name AS full_name,
      u.email,
      u.date_created,
      u.country AS self_reported_country,
      ug.country_code AS registration_ip_country,
      sp.num_flags,
      sp.flag_ip_country_mismatch,
      sp.flag_high_risk_ip_country_mismatch,
      sp.flag_excessive_invoice,
      sp.flag_high_pct_close_time_ip_match_malicious,
      sp.flag_high_pct_close_time_ip_match_any_users,
      sp.flag_high_pct_close_time_ip_rep_country_mismatch,
      sp.flag_half_ips_match_yukon_aqm_ips,
      sp.flag_half_ips_match_malicious_raters,
      sp.flag_half_ips_match_same_project_raters,
      sp.flag_half_ips_match_active_raters,
      sp.flag_half_ips_match_suspicious_raters,
      sp.flag_half_toolbar_ips_match_same_project_raters,
      sp.flag_similar_quiz_history,
      sp.flag_same_photo_id_and_hash,
      sp.flag_registration_ip_vpn,
      sp.flag_same_soundex_and_location,
      sp.flag_at_least_10_locales,
      sp.flag_revoked_for_fraud,
      sp.flag_resume_hash_id_match,
      sp.flag_tumbling_email,
      sp.flag_tumbling_name,
      sp.flag_timezone_country_mismatch,
      sp.flag_high_risk_timezone_country_mismatch,
      sp.flag_sim_ticket_bodies,
      sp.flag_similar_addresses,
      sp.flag_same_phone_numbers,
      sp.flag_at_least_10_languages,
      sp.flag_too_many_ips,
      sp.flag_resume_inconsistency,
      sp.email_in_resume,
      sp.first_name_in_resume,
      sp.last_name_in_resume,
      sp.prim_phone_in_resume,
      sp.common_name_key,
      sp.common_email_key,
      sp.num_locales,
      sp.num_self_reported_languages,
      sp.amt_paid_or_approved_last_45_days,
      sp.num_ips_last_45_days,
      sp.percent_ip_close_timing_mal_users_last_45_days,
      sp.percent_ip_close_timing_any_users_last_45_days,
      sp.percent_ip_close_timing_rep_country_mismatch_last_45_days,
      sp.percent_ips_matching_yukon_aqm_ips_last_45_days,
      sp.percent_ips_matching_malicious_workers_last_45_days,
      sp.percent_ips_matching_active_workers_last_45_days,
      sp.percent_ips_matching_same_project_raters_last_45_days,
      sp.percent_ips_matching_suspicious_raters_last_45_days,
      sp.percent_server_ips_matching_same_project_raters_last_45_days,
      sp.num_days_billed_12_hrs_last_45_days,
      sp.count_of_tumbling_emails,
      sp.count_of_tumbling_names,
      sp.time_between_tumbling_emails,
      sp.time_between_tumbling_names,
      sp.count_of_others_with_sim_ticket_bodies,
      sp.count_quizzes_with_similar_history,
      sp.duplicates_flags_score_pct,
      sp.geographical_flags_score_pct,
      sp.productivity_flag_score_pct,
      sp.ip_collusion_flags_score_pct,
      sp.other_collusion_flags_score_pct,
      sp.misc_profile_flags_score_pct
    FROM
      {{ source('qrp','users') }} u
      LEFT JOIN fct_spam_profile_view sp ON u.id = sp.user_id
      LEFT JOIN {{ source('qrp','user_geos') }} ug ON ug.created_by_user_id = u.id
      LEFT JOIN  {{ source('qrp','tenants') }} as tenants on tenants.id = u.tenant_id
    ORDER BY
      sp.num_flags DESC,
      sp.amt_paid_or_approved_last_45_days DESC
  ) AS b1
  LEFT JOIN (
    SELECT
      t1.user_id,
      t1.audit_type,
      t1.audit_date
    FROM
      (
        SELECT
          un.user_id,
          un.text AS audit_type,
          un.date_created AS audit_date,
          row_number() over(
            PARTITION by un.user_id
            ORDER BY
              un.date_created DESC,
              un.text DESC
          ) AS rnk
        FROM
          {{ source('qrp','user_notes') }}  un
        WHERE
          (
            lower(un.text) LIKE '%integrity_client_audit%'
            OR lower(un.text) LIKE '%integrity_other_audit%'
            OR lower(un.text) LIKE '%integrity_client_cleared%'
            OR lower(un.text) LIKE '%integrity_other_cleared%'
            OR lower(un.text) LIKE '%integrity_client_unknown%'
            OR lower(un.text) LIKE '%integrity_other_unknown%'
            OR lower(un.text) LIKE '%integrity_client_terminated%'
            OR lower(un.text) LIKE '%integrity_other_terminated%'
          )
      ) AS t1
    WHERE
      t1.rnk = 1
  ) AS b2 ON b1.id = b2.user_id
  LEFT JOIN (
    SELECT
      upi.user_id,
      upi.payee_id AS payoneer_id,
      upi.last_payee_status AS payoneer_status
    FROM
      {{ source('qrp','user_payoneer_ids') }} upi
  ) AS b3 ON b1.id = b3.user_id
  LEFT JOIN (
    SELECT
      t1.id,
      sum(t1.amount) AS amt_paid_or_approved_last_45_days
    FROM
      (
        SELECT
          u.id,
          u.first_name,
          u.last_name,
          u.email,
          u.status AS platform_status,
          i.status AS invoice_status,
          upi.payee_id AS payoneer_id,
          i.start_date AS invoice_date,
          i.amount_paid AS amount
        FROM
          {{ source('qrp','users') }} u
          LEFT JOIN {{ source('qrp','user_payoneer_ids') }} upi ON upi.user_id = u.id
          LEFT JOIN {{ source('qrp','invoices') }} i ON i.user_id = u.id
        WHERE
          date(i.start_date) >= date(getdate ()) - '45 days' :: INTERVAL
          AND i.status IN ('PAID')
          AND i.amount_paid IS NOT NULL
        UNION
        SELECT
          u.id,
          u.first_name,
          u.last_name,
          u.email,
          u.status AS platform_status,
          i.status AS invoice_status,
          upi.payee_id AS payoneer_id,
          i.start_date AS invoice_date,
          i.amount_authorized AS amount
        FROM
          {{ source('qrp','users') }} u
        LEFT JOIN {{ source('qrp','user_payoneer_ids') }} upi ON upi.user_id = u.id
        LEFT JOIN {{ source('qrp','invoices') }} i ON i.user_id = u.id
        WHERE
          date(i.start_date) >= date(getdate ()) - '45 days' :: INTERVAL
          AND i.status IN ('APPROVED')
      ) AS t1
    GROUP BY
      1
  ) AS b4 ON b1.id = b4.id
  LEFT JOIN (
    SELECT
      u.id,
      count(
        DISTINCT CASE
          WHEN date(ualr.date_created) >= date(getdate ()) - '45 days' :: INTERVAL THEN ualr.ip_address
        END
      ) AS num_ips_last_45_days
    FROM {{ source('qrp','user_activity_log_records') }}  ualr
    JOIN {{ source('qrp','users') }} u ON u.id = ualr.user_id
    WHERE ualr.ip_address NOT IN (
      SELECT ip_address
      FROM recruiting.internal_ips
    )
    AND ualr.ip_address NOT in (
      SELECT ip_address
      FROM {{ source('crowdcrm','dim_suspicious_ips') }}
    )
    GROUP BY u.id
  ) AS b5 ON b1.id = b5.id
  ORDER BY fact_date desc


), fct_contributor_ip_addresses as (

SELECT
	  contributor_id
	  , LISTAGG(distinct take5_review_reason,',') AS take5_review_reasons
	  , SUM(CASE WHEN take5_fail = 'Take5-Fail' THEN 1 ELSE 0 END) AS count_of_take5_fail
	  , SUM(CASE WHEN take5_needs_review = 'Take5-NeedsReview' THEN 1 ELSE 0 END) AS count_of_take5_needs_review
 FROM fraud.fct_contributor_ip_addresses
 GROUP BY 1

),aggregated_ipqs_score as (
SELECT *
  FROM (
      SELECT
          user_id AS contributor_id
          , ips
    		, vpn
    		, active_vpn
    		, tor
    		, proxy
    		, bot_status
    		, ips_per_day
    		, no_vpn_ips
    		, no_vpn_unique_ips
    		, pay_rate_mismatch
    		, country_mismatch
    		, pay_rate_flag
    		, speed_flag
    		, m_score
    		, m_score_flag
    		, high_risk_flag
    		, m_score_new
    		, m_score_flag_new
    		, date_created AS ipqs_score_date_created
          , ROW_NUMBER() OVER (
              PARTITION BY user_id
              ORDER BY date_created DESC
          ) AS row_number
      FROM {{ source('crowdcrm','fct_aggregated_ipqs_score') }}
  ) temp
  WHERE temp.row_number = 1
),usr_profile AS (

    SELECT
        user_id
        , age
        , gender
        , raterqualification_provider
    FROM (
        SELECT
            user_id
            , age
            , gender
            , raterqualification_provider
            , ROW_NUMBER() OVER (
                PARTITION BY user_id
                ORDER BY date_created DESC
            ) AS latest_user_profile
        FROM {{ source('qrp','user_profiles') }}
    ) sub
    WHERE sub.latest_user_profile = 1

), dim_contributors as (

SELECT
    usr.id AS contributor_id
    , CONCAT('https://connect.appen.com/qrp/core/vendor/view/' , usr.id) as user_profile_link
    , ANY_VALUE(usr.tenant_id) AS tenant_id
    , ANY_VALUE(usr.campaign_id) AS campaign_id
    , ANY_VALUE(tenants.name) AS tenant
    , ANY_VALUE(tenants.employment_type) AS tenant_employment_type
    , ANY_VALUE(DATE_DIFF('Day', usr.date_updated, CURRENT_DATE)) AS num_days_in_user_status
    , ANY_VALUE(usr.first_name) AS first_name
    , ANY_VALUE(usr.last_name) AS last_name
    , ANY_VALUE(usr.first_name || ' ' || usr.last_name) AS contributor_name
    , ANY_VALUE(usr.email) AS email
    , ANY_VALUE(usr.state) AS state
    , ANY_VALUE(usr.city) AS city
    , ANY_VALUE(usr.country) AS country
    , ANY_VALUE(usr.status) AS status
    , ANY_VALUE(usr.express_status) AS express_status
    , ANY_VALUE(usr.pay_rate) AS pay_rate
    , ANY_VALUE(usr.first_hired_date) AS first_hired_date
    , ANY_VALUE(usr.termination_date) AS termination_date
    , ANY_VALUE(usr.termination_reason) AS termination_reason
    , ANY_VALUE(usr.num_hours_committed) AS num_hours_committed
    , ANY_VALUE(usr.primary_phone) AS phone_number
    , ANY_VALUE(usr.date_created) AS date_created
    , ANY_VALUE(usr.date_updated) AS last_user_update
    , ANY_VALUE(usr.is_locked) AS is_locked
    , ANY_VALUE(usr_profile.age) AS age
    , ANY_VALUE(usr_profile.gender) AS gender
    , ANY_VALUE(usr_profile.raterqualification_provider) AS raterqualification_provider
    , ANY_VALUE(COALESCE(usr.primary_locale, locales.code_3)) AS primary_locale
    , ANY_VALUE(SUBSTRING(COALESCE(usr.primary_locale, locales.code_3), 1, 3)) AS language
    , ANY_VALUE(SUBSTRING(COALESCE(usr.primary_locale, locales.code_3), 5, 3)) AS dialect
    , ANY_VALUE(locales.country_3) AS locale_country
    , ANY_VALUE(usr_locale.spoken_fluency) AS spoken_fluency
    , ANY_VALUE(usr_locale.written_fluency) AS written_fluency
    , ANY_VALUE(user_unsub.type) AS type
    , ANY_VALUE(user_unsub.is_unsubscribed) AS is_unsubs_in_type
    , ANY_VALUE(campaigns.source) AS user_application_source
    , ANY_VALUE(usr_last_login.last_login) AS last_login
    , ANY_VALUE(fb.duplicates_fraud_percent) AS duplicates_fraud_percent
    , ANY_VALUE(fb.geographical_fraud_percent) AS geographical_fraud_percent
    , ANY_VALUE(fb.productivity_fraud_percent) AS productivity_fraud_percent
    , ANY_VALUE(fb.ip_collusion_fraud_percent) AS ip_collusion_fraud_percent
    , ANY_VALUE(fb.other_collusion_fraud_percent) AS other_collusion_fraud_percent
    , ANY_VALUE(fb.misc_profile_fraud_percent) AS misc_profile_fraud_percent
    , ANY_VALUE(frn.text) AS text
    , ANY_VALUE(frn.note_creation_date) AS note_creation_date
    , ANY_VALUE(cp.pay_rate) AS user_country_pay_rate
    , ANY_VALUE(fspv.fact_date) AS fact_date
    , MAX(COALESCE(fspv.audit_escalation_id , 'N/A')) AS audit_escalation_id
    , ANY_VALUE(fspv.tenant_name) AS tenant_name
    , ANY_VALUE(fspv.payoneer_id) AS payoneer_id
    , ANY_VALUE(fspv.maliciousness_level) AS maliciousness_level
    , ANY_VALUE(fspv.num_flags) AS num_flags
    , ANY_VALUE(fspv.ac_status) AS ac_status
    , ANY_VALUE(fspv.payoneer_status) AS payoneer_status
    , ANY_VALUE(fspv.self_reported_country) AS self_reported_country
    , ANY_VALUE(fspv.registration_ip_country) AS registration_ip_country
    , MAX(COALESCE(fspv.audit_notes , 'N/A')) AS audit_notes
    , MAX(COALESCE(fspv.audit_type_new , 'N/A')) AS audit_type_new
    , MAX(COALESCE(fspv.audit_outcome , 'N/A')) AS audit_outcome
    , ANY_VALUE(fspv.flag_ip_country_mismatch) AS flag_ip_country_mismatch
    , ANY_VALUE(fspv.flag_high_risk_ip_country_mismatch) AS flag_high_risk_ip_country_mismatch
    , ANY_VALUE(fspv.flag_excessive_invoice) AS flag_excessive_invoice
    , ANY_VALUE(fspv.flag_high_pct_close_time_ip_match_malicious) AS flag_high_pct_close_time_ip_match_malicious
    , ANY_VALUE(fspv.flag_high_pct_close_time_ip_match_any_users) AS flag_high_pct_close_time_ip_match_any_users
    , ANY_VALUE(fspv.flag_high_pct_close_time_ip_rep_country_mismatch) AS flag_high_pct_close_time_ip_rep_country_mismatch
    , ANY_VALUE(fspv.flag_half_ips_match_yukon_aqm_ips) AS flag_half_ips_match_yukon_aqm_ips
    , ANY_VALUE(fspv.flag_half_ips_match_malicious_raters) AS flag_half_ips_match_malicious_raters
    , ANY_VALUE(fspv.flag_half_ips_match_same_project_raters) AS flag_half_ips_match_same_project_raters
    , ANY_VALUE(fspv.flag_half_ips_match_active_raters) AS flag_half_ips_match_active_raters
    , ANY_VALUE(fspv.flag_half_ips_match_suspicious_raters) AS flag_half_ips_match_suspicious_raters
    , ANY_VALUE(fspv.flag_half_toolbar_ips_match_same_project_raters) AS flag_half_toolbar_ips_match_same_project_raters
    , ANY_VALUE(fspv.flag_similar_quiz_history) AS flag_similar_quiz_history
    , ANY_VALUE(fspv.flag_same_photo_id_and_hash) AS flag_same_photo_id_and_hash
    , ANY_VALUE(fspv.flag_registration_ip_vpn) AS flag_registration_ip_vpn
    , ANY_VALUE(fspv.flag_same_soundex_and_location) AS flag_same_soundex_and_location
    , ANY_VALUE(fspv.flag_at_least_10_locales) AS flag_at_least_10_locales
    , ANY_VALUE(fspv.flag_revoked_for_fraud) AS flag_revoked_for_fraud
    , ANY_VALUE(fspv.flag_resume_hash_id_match) AS flag_resume_hash_id_match
    , ANY_VALUE(fspv.flag_tumbling_email) AS flag_tumbling_email
    , ANY_VALUE(fspv.flag_tumbling_name) AS flag_tumbling_name
    , ANY_VALUE(fspv.flag_timezone_country_mismatch) AS flag_timezone_country_mismatch
    , ANY_VALUE(fspv.flag_high_risk_timezone_country_mismatch) AS flag_high_risk_timezone_country_mismatch
    , ANY_VALUE(fspv.flag_sim_ticket_bodies) AS flag_sim_ticket_bodies
    , ANY_VALUE(fspv.flag_similar_addresses) AS flag_similar_addresses
    , ANY_VALUE(fspv.flag_same_phone_numbers) AS flag_same_phone_numbers
    , ANY_VALUE(fspv.flag_at_least_10_languages) AS flag_at_least_10_languages
    , ANY_VALUE(fspv.flag_too_many_ips) AS flag_too_many_ips
    , ANY_VALUE(fspv.flag_resume_inconsistency) AS flag_resume_inconsistency
    , ANY_VALUE(fspv.email_in_resume) AS email_in_resume
    , ANY_VALUE(fspv.first_name_in_resume) AS first_name_in_resume
    , ANY_VALUE(fspv.last_name_in_resume) AS last_name_in_resume
    , ANY_VALUE(fspv.prim_phone_in_resume) AS prim_phone_in_resume
    , ANY_VALUE(fspv.common_name_key) AS common_name_key
    , ANY_VALUE(fspv.common_email_key) AS common_email_key
    , ANY_VALUE(fspv.num_locales) AS num_locales
    , ANY_VALUE(fspv.num_self_reported_languages) AS num_self_reported_languages
    , ANY_VALUE(fspv.amt_paid_or_approved_last_45_days) AS amt_paid_or_approved_last_45_days
    , ANY_VALUE(fspv.num_ips_last_45_days) AS num_ips_last_45_days
    , ANY_VALUE(fspv.percent_ip_close_timing_mal_users_last_45_days) AS percent_ip_close_timing_mal_users_last_45_days
    , ANY_VALUE(fspv.percent_ip_close_timing_any_users_last_45_days) AS percent_ip_close_timing_any_users_last_45_days
    , ANY_VALUE(fspv.percent_ip_close_timing_rep_country_mismatch_last_45_days) AS percent_ip_close_timing_rep_country_mismatch_last_45_days
    , ANY_VALUE(fspv.percent_ips_matching_yukon_aqm_ips_last_45_days) AS percent_ips_matching_yukon_aqm_ips_last_45_days
    , ANY_VALUE(fspv.percent_ips_matching_malicious_workers_last_45_days) AS percent_ips_matching_malicious_workers_last_45_days
    , ANY_VALUE(fspv.percent_ips_matching_active_workers_last_45_days) AS percent_ips_matching_active_workers_last_45_days
    , ANY_VALUE(fspv.percent_ips_matching_same_project_raters_last_45_days) AS percent_ips_matching_same_project_raters_last_45_days
    , ANY_VALUE(fspv.percent_ips_matching_suspicious_raters_last_45_days) AS percent_ips_matching_suspicious_raters_last_45_days
    , ANY_VALUE(fspv.percent_server_ips_matching_same_project_raters_last_45_days) AS percent_server_ips_matching_same_project_raters_last_45_days
    , ANY_VALUE(fspv.num_days_billed_12_hrs_last_45_days) AS num_days_billed_12_hrs_last_45_days
    , ANY_VALUE(fspv.count_of_tumbling_emails) AS count_of_tumbling_emails
    , ANY_VALUE(fspv.count_of_tumbling_names) AS count_of_tumbling_names
    , ANY_VALUE(fspv.time_between_tumbling_emails) AS time_between_tumbling_emails
    , ANY_VALUE(fspv.time_between_tumbling_names) AS time_between_tumbling_names
    , ANY_VALUE(fspv.count_of_others_with_sim_ticket_bodies) AS count_of_others_with_sim_ticket_bodies
    , ANY_VALUE(fspv.count_quizzes_with_similar_history) AS count_quizzes_with_similar_history
    , ANY_VALUE(fspv.duplicates_flags_score_pct) AS duplicates_flags_score_pct
    , ANY_VALUE(fspv.geographical_flags_score_pct) AS geographical_flags_score_pct
    , ANY_VALUE(fspv.productivity_flag_score_pct) AS productivity_flag_score_pct
    , ANY_VALUE(fspv.ip_collusion_flags_score_pct) AS ip_collusion_flags_score_pct
    , ANY_VALUE(fspv.other_collusion_flags_score_pct) AS other_collusion_flags_score_pct
    , ANY_VALUE(fspv.misc_profile_flags_score_pct) AS misc_profile_flags_score_pct
    , ANY_VALUE(fspv.audit_type) AS audit_type
    , ANY_VALUE(fspv.audit_date) AS audit_date
    , ANY_VALUE(fcia.take5_review_reasons) AS take5_review_reasons
    , ANY_VALUE(fcia.count_of_take5_fail) AS count_of_take5_fail
    , ANY_VALUE(fcia.count_of_take5_needs_review) AS count_of_take5_needs_review
    , ANY_VALUE(ais.ips) AS ips
    , ANY_VALUE(ais.vpn) AS vpn
    , ANY_VALUE(ais.active_vpn) AS active_vpn
    , ANY_VALUE(ais.tor) AS tor
    , ANY_VALUE(ais.proxy) AS proxy
    , ANY_VALUE(ais.bot_status) AS bot_status
    , ANY_VALUE(ais.ips_per_day) AS ips_per_day
    , ANY_VALUE(ais.no_vpn_ips) AS no_vpn_ips
    , ANY_VALUE(ais.no_vpn_unique_ips) AS no_vpn_unique_ips
    , ANY_VALUE(ais.pay_rate_mismatch) AS pay_rate_mismatch
    , ANY_VALUE(ais.country_mismatch) AS country_mismatch
    , ANY_VALUE(ais.pay_rate_flag) AS pay_rate_flag
    , ANY_VALUE(ais.speed_flag) AS speed_flag
    , ANY_VALUE(ais.m_score) AS m_score
    , ANY_VALUE(ais.m_score_flag) AS m_score_flag
    , ANY_VALUE(ais.high_risk_flag) AS high_risk_flag
    , ANY_VALUE(ais.m_score_new) AS m_score_new
    , ANY_VALUE(ais.m_score_flag_new) AS m_score_flag_new
    , ANY_VALUE(ais.ipqs_score_date_created) AS ipqs_score_date_created
FROM {{ source('qrp','users') }} usr
LEFT JOIN usr_last_login
    ON usr.id = usr_last_login.user_id
LEFT JOIN usr_profile
    ON usr.id = usr_profile.user_id
LEFT JOIN {{ source('qrp','tenants') }} tenants
    ON usr.tenant_id = tenants.id
LEFT JOIN usr_locale
    ON usr.id = usr_locale.user_id
LEFT JOIN {{ source('qrp','exp_locales') }} locales
    ON usr_locale.locale_id = locales.id
LEFT JOIN user_unsub
    ON usr.id = user_unsub.user_id
LEFT JOIN {{ source('qrp','campaigns') }} campaigns
    ON usr.campaign_id = campaigns.id
LEFT JOIN fraud_buckets fb
    ON usr.id = fb.user_id
LEFT JOIN fraud_related_notes frn
    ON usr.id = frn.user_id
LEFT JOIN spam_profile_summary fspv
    ON usr.id = fspv.id
LEFT JOIN fct_contributor_ip_addresses fcia
    ON usr.id = fcia.contributor_id
LEFT JOIN aggregated_ipqs_score ais
    ON usr.id = ais.contributor_id
LEFT JOIN countries_payrate cp
    ON usr.country = cp.country
GROUP BY 1
)

select * from dim_contributors