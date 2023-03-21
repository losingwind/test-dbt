{{
    config(
        materialized='table',
        tags=["fact"]
    )
}}

WITH fct_contributors_verify_email AS (

    SELECT
        TO_DATE( (TIMESTAMP 'epoch' + time * INTERVAL '1 Second '), 'YYYY-MM-DD') AS mp_date_created
        , mp_date
        , mp_event_name
        , distinct_id
        , mp_user_id
        , utm_campaign
        , CASE
            WHEN utm_campaign IN (
                'Appen_AppleMedia_Cable_142-Acquisition_Campaign_2022'
                , 'Appen_AppleMedia_Chelan_1822-Acquisition_Campaign_2022'
                , 'Appen_AppleMedia_Echo_109-Acquisition_Campaign_2022'
                , 'Appen_AppleMedia_Index_106-Acquisition_Campaign_2022'
                , 'Appen_AppleMedia_Java_108-Acquisition_Campaign_2022'
                , 'Appen_AppleMedia_Rush_3767-Acquisition_Campaign_2022'
                , 'Appen_AppleMedia_Wells_4763-Acquisition_Campaign_2022'
                , 'Appen_BehavioralComputingFacesAnnotation_Stranger_4640-Acquisition_Campaign_2022'
                , 'Appen_Bluebird_Crescent_352-Acquisition_Campaign_2022'
                , 'Appen_Clickbait_CherryV2_2371-Acquisition_Campaign_2022'
                , 'Appen_FalconFlex_Rustler_4928-Acquisition_Campaign_2022'
                , 'Appen_GoogleAds_Arrow_87-Acquisition_Campaign_2022'
                , 'Appen_GoogleAssistant_ArrowButlerAuditors_2789-Acquisition_Campaign_2022'
                , 'Appen_GoogleAssistant_ArrowButler_610-Acquisition_Campaign_2022'
                , 'Appen_GoogleMaps_GoldcrestAtlas_2022-Acquisition_Campaign_2022'
                , 'Appen_GoogleMaps_GoldcrestPowWow_2334-Acquisition_Campaign_2022'
                , 'Appen_GoogleSearch_Yukon_1-Acquisition_Campaign_2022'
                , 'Appen_GroundTruth_Thames_39-Acquisition_Campaign_2022'
                , 'Appen_IGOffTopic_Conness_3572-Acquisition_Campaign_2022'
                , 'Appen_IGReels_DuPage_2804-Acquisition_Campaign_2022'
                , 'Appen_IGReels_Enoree_2478-Acquisition_Campaign_2022'
                , 'Appen_IGReels_ReedyTopN_4754-Acquisition_Campaign_2022'
                , 'Appen_IGReels_Reedy_2224-Acquisition_Campaign_2022'
                , 'Appen_IGReels_Rowley_4755-Acquisition_Campaign_2022'
                , 'Appen_IGReels_Toccata_3653-Acquisition_Campaign_2022'
                , 'Appen_MESActorReview_Tygart_4043-Acquisition_Campaign_2022'
                , 'Appen_Megataxon_Stonecoal_4340-Acquisition_Campaign_2022'
                , 'Appen_MisinfoMatching_Potomac_1060-Acquisition_Campaign_2022'
                , 'Appen_PublicFiguresLabeling_Paulins_2723-Acquisition_Campaign_2022'
                , 'Appen_UCI_Uolo_1373-Acquisition_Campaign_2022'
            ) THEN 'GRG'
            WHEN utm_campaign IS NOT NULL THEN 'Others'
            ELSE 'No Campaign'
        END AS campaign_source
        , utm_content
        , utm_source
    FROM {{ source('mixpanel_es_shasta','mp_master_event') }}
    WHERE mp_event_name = 'VerifyEmailSuccess'

)

SELECT *
FROM fct_contributors_verify_email
