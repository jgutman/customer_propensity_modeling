WITH
    actives AS (
    SELECT u.user_id
    FROM web.user_membership_status_changes u
    LEFT JOIN web.user_membership_status_changes i
      ON u.user_id = i.user_id
         AND i.created_at > u.created_at
         AND date( i.created_at ) <= current_date
    WHERE
      i.user_id IS NULL
      AND date( u.created_at ) <= current_date
      AND u.change_type <> 2
  ),
    latest_payment_profile AS (
      SELECT
        max( updated_at ) AS updated_at
        , user_id
      FROM web.vantiv_payment_profiles
      WHERE card_type IS NOT NULL
      GROUP BY 2
  ),
    user_card_type AS (
      SELECT
        vpp.user_id AS internal_user_id
        , vpp.card_type
      FROM web.vantiv_payment_profiles vpp
      INNER JOIN latest_payment_profile lpp
        ON lpp.user_id = vpp.user_id
           AND vpp.updated_at = lpp.updated_at
  ),
    cohorts AS (
      SELECT
        internal_user_id
        , min( delivery_date ) AS first_delivery_date
        , min( DATE( menu_order_placed ) ) AS first_order_billed_date
        , max( delivery_date ) AS most_recent_delivery_date
      FROM dw.menu_order_boxes
      WHERE
        status = 'shipped'
        AND delivery_schedule_type = 'normal'
        AND delivery_date <= :end_date
      GROUP BY 1
  ),
    mob AS (
      SELECT
        internal_user_id
        , delivery_schedule_name
        , status
        , min( delivery_date ) AS delivery_date
      FROM dw.menu_order_boxes
      WHERE delivery_date <= :end_date
            AND delivery_schedule_type = 'normal'
      GROUP BY 1, 2, 3
  ),
    four_week_order_rate AS (
      SELECT
        mob.internal_user_id
        , COUNT( mob.delivery_date ) AS num_boxes_first_4_weeks
        , SUM( CASE WHEN datediff( 'week', cohorts.first_delivery_date,
                                   mob.delivery_date ) = 3
        THEN 1 END ) AS ordered_week_4
      FROM cohorts
      INNER JOIN mob
        ON cohorts.internal_user_id = mob.internal_user_id
           AND datediff( 'week', cohorts.first_delivery_date, mob.delivery_date ) < 4
           AND mob.status = 'shipped'
      GROUP BY 1
  ),
    order_aggregate AS (
      SELECT
        mob.internal_user_id
        , SUM( CASE mob.status
               WHEN 'shipped'
                 THEN 1 END ) AS num_deliveries
        , SUM( CASE mob.status
               WHEN 'canceled'
                 THEN 1 END ) AS num_canceled_orders
        , COUNT( DISTINCT CASE mob.status
                          WHEN 'shipped'
                            THEN shipping_address_line_1 || shipping_zip_code
                          END ) AS num_addresses_delivered
        , AVG( cast( CASE mob.status
                     WHEN 'shipped'
                       THEN transit_time
                     END AS FLOAT ) ) AS avg_transit_time
        , SUM( CASE mob.status
               WHEN 'shipped'
                 THEN gov END ) AS total_gov
        , AVG( CASE mob.status
               WHEN 'shipped'
                 THEN gov END ) AS avg_gov
        , SUM( CASE mob.status
               WHEN 'shipped'
                 THEN subscription_plates END ) AS total_subscription_plates
        , SUM( CASE mob.status
               WHEN 'shipped'
                 THEN dessert_plates END ) AS total_dessert_plates
        , AVG( CASE mob.status
               WHEN 'shipped'
                 THEN subscription_plates END ) AS avg_plates_per_box
        , SUM( CASE WHEN num_2_portion_main_plates > 0
                         AND mob.status = 'shipped'
        THEN 1 END ) AS num_2_serving_boxes
        , SUM( CASE WHEN num_3_portion_main_plates > 0
                         AND mob.status = 'shipped'
        THEN 1 END ) AS num_3_serving_boxes
        , SUM( CASE WHEN num_4_portion_main_plates > 0
                         AND mob.status = 'shipped'
        THEN 1 END ) AS num_4_serving_boxes
        , SUM( CASE WHEN mob.status = 'shipped'
                         AND mob.delivery_date >= (:end_date - 28)
        THEN 1 END ) AS num_deliveries_month
        , SUM( CASE WHEN mob.status = 'canceled'
                         AND mob.delivery_date >= (:end_date - 28)
        THEN 1 END ) AS num_canceled_orders_month
        , count( DISTINCT CASE WHEN mob.status = 'shipped'
                                    AND mob.delivery_date >= (:end_date - 28)
        THEN shipping_address_line_1 || shipping_zip_code
                          END ) AS num_addresses_delivered_month
        , SUM( CASE WHEN mob.delivery_date >= (:end_date - 28)
                         AND mob.status = 'shipped'
        THEN gov END ) AS total_gov_month
        , AVG( CASE WHEN mob.delivery_date >= (:end_date - 28)
                         AND mob.status = 'shipped'
        THEN gov END ) AS avg_gov_month
        , SUM( CASE WHEN mob.delivery_date >= (:end_date - 28)
                         AND mob.status = 'shipped'
        THEN dessert_plates END )
        AS total_dessert_plates_month
      FROM mob
      INNER JOIN dw.menu_order_boxes mob2
        ON mob.internal_user_id = mob2.internal_user_id
           AND mob.delivery_schedule_name = mob2.delivery_schedule_name
           AND mob.delivery_date = mob2.delivery_date
           AND mob.status = mob2.status
      WHERE mob.delivery_date >= '2017-01-01'
      GROUP BY 1
  ),
    recipes_aggregate AS (
      SELECT
        mob.internal_user_id
        , COUNT( r.recipe_id ) AS total_recipes_ordered
        , COUNT( DISTINCT r.recipe_id ) AS distinct_recipes_ordered
        , AVG( r.price_per_portion ) AS avg_portion_price
      FROM mob
      INNER JOIN dw.menu_order_boxes mob2
        ON mob.internal_user_id = mob2.internal_user_id
           AND mob.delivery_schedule_name = mob2.delivery_schedule_name
           AND mob.delivery_date = mob2.delivery_date
           AND mob2.status = 'shipped'
      INNER JOIN web.ecommerce_menu_order_recipes r
        ON mob2.internal_menu_order_id = r.menu_order_id
      WHERE mob.delivery_date >= '2017-01-01'
      GROUP BY 1
  ),
    latest_dislikes AS (
      SELECT
        user_id
        , max( client_timestamp ) AS client_timestamp
      FROM dw.web_track_events wte
      WHERE event = 'Taste Preferences Updated'
            AND DATE( client_timestamp ) <= :end_date
      GROUP BY 1
  ),
    dislikes_current AS (
      SELECT
        uni.user_id
        , listagg( '"' || ert.slug || '"', ', ' )
          WITHIN GROUP (ORDER BY ert.id) AS all_dislikes
      FROM web.user_no_interests uni
      INNER JOIN web.ecommerce_recipe_tags ert
        ON uni.tag_id = ert.id
      WHERE DATE( ert.updated_at ) <= :end_date
      GROUP BY 1
  ),
    dislikes AS (
      SELECT
        wte.user_id
        , json_extract_path_text( properties, 'new_dislikes' ) AS all_dislikes
      FROM latest_dislikes ld
      INNER JOIN dw.web_track_events wte
        ON ld.user_id = wte.user_id
           AND ld.client_timestamp = wte.client_timestamp
           AND event = 'Taste Preferences Updated'
  ),
    dislikes_binary AS (
      SELECT
        COALESCE( dp.user_id, dc.user_id ) AS internal_user_id
        , COALESCE( dp.all_dislikes, dc.all_dislikes ) LIKE '%"beef"%'
        AS dislikes_beef
        , COALESCE( dp.all_dislikes, dc.all_dislikes ) LIKE '%"fish"%'
        AS dislikes_fish
        , COALESCE( dp.all_dislikes, dc.all_dislikes ) LIKE '%"lamb"%'
        AS dislikes_lamb
        , COALESCE( dp.all_dislikes, dc.all_dislikes ) LIKE '%"pork"%'
        AS dislikes_pork
        , COALESCE( dp.all_dislikes, dc.all_dislikes ) LIKE '%"poultry"%'
        AS dislikes_poultry
        , COALESCE( dp.all_dislikes, dc.all_dislikes ) LIKE '%"shellfish"%'
        AS dislikes_shellfish
        , COALESCE( dp.all_dislikes, dc.all_dislikes ) LIKE '%"vegetarian"%'
        AS dislikes_vegetarian
      FROM dislikes_current dc
      FULL OUTER JOIN dislikes dp
        ON dc.user_id = dp.user_id
  ),
    referrals AS (
      SELECT
        referrer_internal_user_id AS internal_user_id
        , COUNT( r.* ) AS referrals_earned_total
        , COUNT( sent_at ) AS referrals_sent_total
        , COUNT( converted_at ) AS referrals_redeemed_total
        , SUM( CASE WHEN DATE( referral_issued_at ) >= (:end_date - 7)
        THEN 1 END ) AS referrals_earned_week
        , SUM( CASE WHEN DATE( sent_at ) >= (:end_date - 7)
        THEN 1 END ) AS referrals_sent_week
        , SUM( CASE WHEN DATE( converted_at ) >= (:end_date - 7)
        THEN 1 END ) AS referrals_redeemed_week
        , SUM( CASE WHEN DATE( referral_issued_at ) >= (:end_date - 28)
        THEN 1 END ) AS referrals_earned_month
        , SUM( CASE WHEN DATE( sent_at ) >= (:end_date - 28)
        THEN 1 END ) AS referrals_sent_month
        , SUM( CASE WHEN DATE( converted_at ) >= (:end_date - 28)
        THEN 1 END ) AS referrals_redeemed_month
      FROM dw.user_referral_invites
      WHERE DATE( referral_issued_at ) <= :end_date
      GROUP BY 1
  ),
    ratings AS (
      SELECT
        user_id AS external_user_id
        , sum( CASE value
               WHEN 5
                 THEN 1 END ) AS five_star_count_total
        , sum( CASE value
               WHEN 4
                 THEN 1 END ) AS four_star_count_total
        , sum( CASE value
               WHEN 3
                 THEN 1 END ) AS three_star_count_total
        , sum( CASE value
               WHEN 2
                 THEN 1 END ) AS two_star_count_total
        , sum( CASE value
               WHEN 1
                 THEN 1 END ) AS one_star_count_total
        , count( * ) AS total_rating_count
        , count( CASE WHEN source = 'website'
        THEN value END )
        AS total_star_ratings_on_website
        , count( CASE WHEN source = 'mobile'
        THEN value END )
        AS total_star_ratings_on_mobile
        , count( CASE WHEN source = 'website'
        THEN notes END )
        AS total_reviews_on_website
        , count( CASE WHEN source = 'mobile'
        THEN notes END )
        AS total_reviews_on_mobile
        , count( notes ) AS total_review_count
        , sum( CASE WHEN value = 5
                         AND DATE( created_at ) >= (:end_date - 7)
        THEN 1 END ) AS five_star_count_week
        , sum( CASE WHEN value = 4
                         AND DATE( created_at ) >= (:end_date - 7)
        THEN 1 END ) AS four_star_count_week
        , sum( CASE WHEN value = 3
                         AND DATE( created_at ) >= (:end_date - 7)
        THEN 1 END ) AS three_star_count_week
        , sum( CASE WHEN value = 2
                         AND DATE( created_at ) >= (:end_date - 7)
        THEN 1 END ) AS two_star_count_week
        , sum( CASE WHEN value = 1
                         AND DATE( created_at ) >= (:end_date - 7)
        THEN 1 END ) AS one_star_count_week
        , sum( CASE WHEN DATE( created_at ) >= (:end_date - 7)
        THEN 1 END ) AS total_rating_count_week
        , count( CASE WHEN DATE( created_at ) >= (:end_date - 7)
        THEN notes END ) AS total_review_count_week
        , sum( CASE WHEN value = 5
                         AND DATE( created_at ) >= (:end_date - 28)
        THEN 1 END ) AS five_star_count_month
        , sum( CASE WHEN value = 4
                         AND DATE( created_at ) >= (:end_date - 28)
        THEN 1 END ) AS four_star_count_month
        , sum( CASE WHEN value = 3
                         AND DATE( created_at ) >= (:end_date - 28)
        THEN 1 END ) AS three_star_count_month
        , sum( CASE WHEN value = 2
                         AND DATE( created_at ) >= (:end_date - 28)
        THEN 1 END ) AS two_star_count_month
        , sum( CASE WHEN value = 1
                         AND DATE( created_at ) >= (:end_date - 28)
        THEN 1 END ) AS one_star_count_month
        , sum( CASE WHEN DATE( created_at ) >= (:end_date - 28)
        THEN 1 END ) AS total_rating_count_month
        , count( CASE WHEN DATE( created_at ) >= (:end_date - 28)
        THEN notes END ) AS total_review_count_month
      FROM review.reviews
      WHERE date( reviews.created_at ) BETWEEN '2017-01-01' AND :end_date
      GROUP BY 1
  ),
    issued_credits AS (
      SELECT
        user_id AS internal_user_id
        , sum( CASE WHEN event_type IN ('User::ServiceLog',
                                        'ReplacementCreditIssuance')
        THEN balance_change END )
        AS total_cx_credit_issued
        , sum( CASE WHEN event_type IN ('User::PromoRedemption',
                                        'LoyaltyMarketingEvent',
                                        'PromotionalCreditIssuance')
        THEN balance_change END )
        AS total_marketing_credit_issued
        , sum( CASE WHEN event_type = 'ReferralConversion'
        THEN balance_change END )
        AS total_referral_credit_issued
        , sum( CASE WHEN event_type IN ('User::ServiceLog',
                                        'ReplacementCreditIssuance')
                         AND DATE( created_at ) >= (:end_date - 7)
        THEN balance_change END )
        AS total_cx_credit_issued_week
        , sum( CASE WHEN event_type IN ('User::PromoRedemption',
                                        'LoyaltyMarketingEvent',
                                        'PromotionalCreditIssuance')
                         AND DATE( created_at ) >= (:end_date - 7)
        THEN balance_change END )
        AS total_marketing_credit_issued_week
        , sum( CASE WHEN event_type IN ('User::ServiceLog',
                                        'ReplacementCreditIssuance')
                         AND DATE( created_at ) >= (:end_date - 28)
        THEN balance_change END )
        AS total_cx_credit_issued_month
        , sum( CASE WHEN event_type IN ('User::PromoRedemption',
                                        'LoyaltyMarketingEvent',
                                        'PromotionalCreditIssuance')
                         AND DATE( created_at ) >= (:end_date - 28)
        THEN balance_change END )
        AS total_marketing_credit_issued_month
      FROM web.credit_events
      WHERE DATE( created_at ) BETWEEN '2017-01-01' AND :end_date
            AND balance_change > 0
      GROUP BY 1
  ),
    cx_issues AS (
      SELECT
        internal_user_id
        , COUNT( DISTINCT internal_menu_order_id ) AS boxes_with_issues_lifetime
        , COUNT( CASE issue_feature
                 WHEN 'Delivery issue (did not receive)'
                   THEN 1 END ) AS delivery_not_received_lifetime
        , COUNT( CASE issue_feature
                 WHEN 'Delivery issue (received late)'
                   THEN 1 END ) AS delivery_late_lifetime
        , COUNT( CASE issue_feature
                 WHEN 'Box damaged in transit'
                   THEN 1 END ) AS box_damaged_lifetime
        , COUNT( CASE issue_feature
                 WHEN 'Change/Skip box'
                   THEN 1 END ) AS box_changes_lifetime
        , COUNT( CASE issue_feature
                 WHEN 'Site usage/Product help'
                   THEN 1 END ) AS product_help_lifetime
        , COUNT( CASE issue_feature
                 WHEN 'Payment error/credit/refund'
                   THEN 1 END ) AS payment_error_lifetime
        , COUNT( CASE issue_feature
                 WHEN 'Food safety'
                   THEN 1 END ) AS food_safety_lifetime
        , COUNT( CASE issue_feature
                 WHEN 'Nutrition/dietary/ingredient inquiry'
                   THEN 1 END ) AS nutrition_ingredient_lifetime
        , COUNT( CASE issue_feature
                 WHEN 'Spoiled/Compromised ingredient'
                   THEN 1 END ) AS spoiled_ingredient_lifetime
        , COUNT( CASE issue_feature
                 WHEN 'Missing/Wrong ingredient'
                   THEN 1 END ) AS missing_ingredient_lifetime
        , COUNT( CASE issue_feature
                 WHEN 'Missing/Wrong meal'
                   THEN 1 END ) AS missing_meal_lifetime
        , COUNT( CASE issue_feature
                 WHEN 'Culinary issue: Confusion'
                   THEN 1 END ) AS culinary_confusion_lifetime
        , COUNT( CASE issue_feature
                 WHEN 'Poor/Inadequate value'
                   THEN 1 END ) AS poor_value_lifetime
        , COUNT( CASE issue_feature
                 WHEN 'Culinary issue: taste'
                   THEN 1 END ) AS culinary_taste_lifetime
        , COUNT( DISTINCT CASE WHEN :end_date - DATE( logged_at ) <= 28
        THEN internal_menu_order_id END )
        AS boxes_with_issues_month
        , COUNT( CASE WHEN :end_date - DATE( logged_at ) <= 28
                           AND issue_feature = 'Delivery issue (did not receive)'
        THEN 1 END ) AS delivery_not_received_month
        , COUNT( CASE WHEN :end_date - DATE( logged_at ) <= 28
                           AND issue_feature = 'Delivery issue (received late)'
        THEN 1 END ) AS delivery_late_month
        , COUNT( CASE WHEN :end_date - DATE( logged_at ) <= 28
                           AND issue_feature = 'Box damaged in transit'
        THEN 1 END ) AS box_damaged_month
        , COUNT( CASE WHEN :end_date - DATE( logged_at ) <= 28
                           AND issue_feature = 'Change/Skip box'
        THEN 1 END ) AS box_changes_month
        , COUNT( CASE WHEN :end_date - DATE( logged_at ) <= 28
                           AND issue_feature = 'Site usage/Product help'
        THEN 1 END ) AS product_help_month
        , COUNT( CASE WHEN :end_date - DATE( logged_at ) <= 28
                           AND issue_feature = 'Payment error/credit/refund'
        THEN 1 END ) AS payment_error_month
        , COUNT( CASE WHEN :end_date - DATE( logged_at ) <= 28
                           AND issue_feature = 'Food safety'
        THEN 1 END ) AS food_safety_month
        , COUNT( CASE WHEN :end_date - DATE( logged_at ) <= 28
                           AND issue_feature = 'Nutrition/dietary/ingredient inquiry'
        THEN 1 END ) AS nutrition_ingredient_month
        , COUNT( CASE WHEN :end_date - DATE( logged_at ) <= 28
                           AND issue_feature = 'Spoiled/Compromised ingredient'
        THEN 1 END ) AS spoiled_ingredient_month
        , COUNT( CASE WHEN :end_date - DATE( logged_at ) <= 28
                           AND issue_feature = 'Missing/Wrong ingredient'
        THEN 1 END ) AS missing_ingredient_month
        , COUNT( CASE WHEN :end_date - DATE( logged_at ) <= 28
                           AND issue_feature = 'Missing/Wrong meal'
        THEN 1 END ) AS missing_meal_month
        , COUNT( CASE WHEN :end_date - DATE( logged_at ) <= 28
                           AND issue_feature = 'Culinary issue: Confusion'
        THEN 1 END ) AS culinary_confusion_month
        , COUNT( CASE WHEN :end_date - DATE( logged_at ) <= 28
                           AND issue_feature = 'Poor/Inadequate value'
        THEN 1 END ) AS poor_value_month
        , COUNT( CASE WHEN :end_date - DATE( logged_at ) <= 28
                           AND issue_feature = 'Culinary issue: taste'
        THEN 1 END ) AS culinary_taste_month
        , COUNT( DISTINCT CASE WHEN :end_date - DATE( logged_at ) <= 7
        THEN internal_menu_order_id END )
        AS boxes_with_issues_week
        , COUNT( CASE WHEN :end_date - DATE( logged_at ) <= 7
                           AND issue_feature = 'Delivery issue (did not receive)'
        THEN 1 END ) AS delivery_not_received_week
        , COUNT( CASE WHEN :end_date - DATE( logged_at ) <= 7
                           AND issue_feature = 'Delivery issue (received late)'
        THEN 1 END ) AS delivery_late_week
        , COUNT( CASE WHEN :end_date - DATE( logged_at ) <= 7
                           AND issue_feature = 'Box damaged in transit'
        THEN 1 END ) AS box_damaged_week
        , COUNT( CASE WHEN :end_date - DATE( logged_at ) <= 7
                           AND issue_feature = 'Change/Skip box'
        THEN 1 END ) AS box_changes_week
        , COUNT( CASE WHEN :end_date - DATE( logged_at ) <= 7
                           AND issue_feature = 'Site usage/Product help'
        THEN 1 END ) AS product_help_week
        , COUNT( CASE WHEN :end_date - DATE( logged_at ) <= 7
                           AND issue_feature = 'Payment error/credit/refund'
        THEN 1 END ) AS payment_error_week
        , COUNT( CASE WHEN :end_date - DATE( logged_at ) <= 7
                           AND issue_feature = 'Food safety'
        THEN 1 END ) AS food_safety_week
        , COUNT( CASE WHEN :end_date - DATE( logged_at ) <= 7
                           AND issue_feature = 'Nutrition/dietary/ingredient inquiry'
        THEN 1 END ) AS nutrition_ingredient_week
        , COUNT( CASE WHEN :end_date - DATE( logged_at ) <= 7
                           AND issue_feature = 'Spoiled/Compromised ingredient'
        THEN 1 END ) AS spoiled_ingredient_week
        , COUNT( CASE WHEN :end_date - DATE( logged_at ) <= 7
                           AND issue_feature = 'Missing/Wrong ingredient'
        THEN 1 END ) AS missing_ingredient_week
        , COUNT( CASE WHEN :end_date - DATE( logged_at ) <= 7
                           AND issue_feature = 'Missing/Wrong meal'
        THEN 1 END ) AS missing_meal_week
        , COUNT( CASE WHEN :end_date - DATE( logged_at ) <= 7
                           AND issue_feature = 'Culinary issue: Confusion'
        THEN 1 END ) AS culinary_confusion_week
        , COUNT( CASE WHEN :end_date - DATE( logged_at ) <= 7
                           AND issue_feature = 'Poor/Inadequate value'
        THEN 1 END ) AS poor_value_week
        , COUNT( CASE WHEN :end_date - DATE( logged_at ) <= 7
                           AND issue_feature = 'Culinary issue: taste'
        THEN 1 END ) AS culinary_taste_week
      FROM dw.user_service_logs usl
      INNER JOIN analytics.cx_issues_grouping_lookup cigl
        ON cigl.reported_issue = lower( usl.reported_issue )
      WHERE DATE( logged_at ) BETWEEN '2017-01-01' AND :end_date
      GROUP BY 1
  ),
    web_browse_activity AS (
      SELECT
        user_id AS internal_user_id
        , count( DISTINCT date( client_timestamp ) ) AS num_days_using_website
        , DATEDIFF( 'day', max( DATE( client_timestamp ) ), :end_date )
        AS num_days_since_last_web_visit
      FROM dw.web_page_visits
      WHERE DATE( client_timestamp ) BETWEEN '2017-01-01' AND :end_date
      GROUP BY 1
  ),
    web_event_activity AS (
      SELECT
        user_id AS internal_user_id
        , count( DISTINCT CASE WHEN event = 'Skipped Box'
        THEN client_timestamp
                          END ) AS num_skips_on_website
        , count( DISTINCT CASE WHEN event = 'Unskipped Box'
        THEN client_timestamp
                          END ) AS num_unskips_on_website
        , count( DISTINCT CASE WHEN event = 'Box Recipes Changed'
        THEN client_timestamp
                          END ) AS num_recipe_changes_on_website
      FROM dw.web_track_events
      WHERE DATE( client_timestamp ) BETWEEN '2017-01-01' AND :end_date
            AND event IN ('Skipped Box', 'Unskipped Box', 'Box Recipes Changed')
      GROUP BY 1
  ),
    ios_browse_activity AS (
      SELECT
        external_user_id
        , count( DISTINCT date( client_timestamp ) ) AS num_days_using_ios
        , DATEDIFF( 'day', max( DATE( client_timestamp ) ), :end_date )
        AS num_days_since_last_ios_visit
      FROM dw.app_screen_views
      WHERE DATE( client_timestamp ) BETWEEN '2017-01-01' AND :end_date
      GROUP BY 1
  ),
    android_browse_activity AS (
      SELECT
        external_user_id
        , count( DISTINCT date( client_timestamp ) ) AS num_days_using_android
        , DATEDIFF( 'day', max( DATE( client_timestamp ) ), :end_date )
        AS num_days_since_last_android_visit
      FROM dw.android_views
      WHERE DATE( client_timestamp ) BETWEEN '2017-01-01' AND :end_date
      GROUP BY 1
  ),
    ios_event_activity AS (
      SELECT
        external_user_id
        , count( DISTINCT CASE WHEN event_name = 'Selected another recipe while editing a box'
        THEN client_timestamp END ) AS num_recipe_changes_on_ios
        , count( DISTINCT CASE WHEN (event_name IN ('Tapped skip week button',
                                                    'Skipped a week from the Feed',
                                                    'Tapped the skip button in the feature tour'))
        THEN client_timestamp END ) AS num_skips_on_ios
        , count( DISTINCT CASE WHEN event_name = 'Unskipped a week from the Feed'
        THEN client_timestamp END ) AS num_unskips_on_ios
      FROM dw.app_track_events
      WHERE DATE( client_timestamp ) BETWEEN '2017-01-01' AND :end_date
            AND event_name IN ('Selected another recipe while editing a box',
                               'Tapped skip week button',
                               'Skipped a week from the Feed', 'Tapped the skip button in the feature tour',
                               'Unskipped a week from the Feed')
      GROUP BY 1
  ),
    android_event_activity AS (
      SELECT
        external_user_id
        , count( DISTINCT CASE WHEN event_name = 'Tapped Swap in My Box on Upcoming Modify Box'
        THEN client_timestamp END ) AS num_recipe_changes_on_android
        , count( DISTINCT CASE WHEN event_name = 'Tapped "Skip Week" on Upcoming Home'
        THEN client_timestamp END ) AS num_skips_on_android
        , count( DISTINCT CASE WHEN event_name = 'Tapped "Unskip Week" on Upcoming Home'
        THEN client_timestamp END ) AS num_unskips_on_android
      FROM dw.android_events
      WHERE DATE( client_timestamp ) BETWEEN '2017-01-01' AND :end_date
            AND event_name IN ('Tapped Swap in My Box on Upcoming Modify Box',
                               'Tapped "Skip Week" on Upcoming Home',
                               'Tapped "Unskip Week" on Upcoming Home')
      GROUP BY 1
  ),
    census AS (
      SELECT zf.*
      FROM dw.demographics_census zf
      LEFT JOIN dw.demographics_census zf2
        ON zf2.zip_code = zf.zip_code
           AND zf2.fips < zf.fips
      WHERE zf2.zip_code IS NULL
  ),
    latest_box_defaults AS (
      SELECT
        user_id
        , max( CASE WHEN NOT has_overridden_shipping_address
        THEN delivery_date END ) AS delivery_date_default_address
        , max( CASE WHEN NOT has_overridden_delivery_date
        THEN delivery_date END ) AS delivery_date_default_day
      FROM web.boxes
      WHERE DATE( locked_at ) <= :end_date
      GROUP BY 1
  ),
    employee AS (
      SELECT DISTINCT user_id
      FROM web.users_discounts ud
      INNER JOIN web.discounts
        ON discount_id = discounts.id
      INNER JOIN web.discount_categories dc
        ON discount_category_id = dc.id
           AND dc.name = 'employee'
      --IN ('corporate', 'employee')
      WHERE DATE( discounts.created_at ) <= :end_date
  )
SELECT
  a.internal_user_id
  , date_diff( 'day', u.prospect_created_at, cohorts.first_delivery_date )
  AS days_from_email_submission_to_first_delivery
  , date_diff( 'day', u.source_created_at, cohorts.first_delivery_date )
  AS days_from_account_creation_to_first_delivery
  , date_diff( 'day', u.accepted_terms, cohorts.first_delivery_date )
  AS days_from_accepting_terms_to_first_delivery
  , date_diff( 'day', cohorts.first_order_billed_date, cohorts.first_delivery_date )
  AS days_from_conversion_to_first_delivery
  , date_diff( 'day', cohorts.most_recent_delivery_date, :end_date )
  AS days_from_most_recent_delivery
  , split_part( u.email, '@', 2 ) AS email_domain
  , to_char( b.delivery_date_default_day, 'Day' ) AS preferred_delivery_day
  , user_addresses.city
  , user_addresses.state
  , u.prospect_source_domain
  , u.who_referred_me IS NOT NULL AS referral_state
  , user_first_hit_utm.first_hit_utm_medium
  , user_first_hit_utm.last_hit_utm_medium
  , user_card_type.card_type
  , four_week_order_rate.num_boxes_first_4_weeks
  , COALESCE( four_week_order_rate.ordered_week_4, 0 ) AS ordered_week_4
  , order_aggregate.num_deliveries
  , COALESCE( order_aggregate.num_canceled_orders, 0 ) AS num_canceled_orders
  , order_aggregate.num_addresses_delivered
  , order_aggregate.avg_transit_time
  , order_aggregate.total_gov
  , order_aggregate.avg_gov
  , order_aggregate.total_subscription_plates
  , order_aggregate.total_dessert_plates
  , order_aggregate.avg_plates_per_box
  , COALESCE( order_aggregate.num_2_serving_boxes, 0 ) AS num_2_serving_boxes
  , COALESCE( order_aggregate.num_3_serving_boxes, 0 ) AS num_3_serving_boxes
  , COALESCE( order_aggregate.num_4_serving_boxes, 0 ) AS num_4_serving_boxes
  , COALESCE( order_aggregate.num_deliveries_month, 0 ) AS num_deliveries_month
  , COALESCE( order_aggregate.num_canceled_orders_month, 0 ) AS num_canceled_orders_month
  , order_aggregate.num_addresses_delivered_month
  , COALESCE( order_aggregate.total_gov_month, 0 ) AS total_gov_month
  , COALESCE( order_aggregate.avg_gov_month, 0 ) AS avg_gov_month
  , COALESCE( order_aggregate.total_dessert_plates_month, 0 ) AS total_dessert_plates_month
  , COALESCE( recipes_aggregate.total_recipes_ordered, 0 ) AS total_recipes_ordered
  , COALESCE( recipes_aggregate.distinct_recipes_ordered, 0 ) AS distinct_recipes_ordered
  , recipes_aggregate.avg_portion_price
  , COALESCE( dislikes_binary.dislikes_beef, FALSE ) AS dislikes_beef
  , COALESCE( dislikes_binary.dislikes_fish, FALSE ) AS dislikes_fish
  , COALESCE( dislikes_binary.dislikes_lamb, FALSE ) AS dislikes_lamb
  , COALESCE( dislikes_binary.dislikes_pork, FALSE ) AS dislikes_pork
  , COALESCE( dislikes_binary.dislikes_poultry, FALSE ) AS dislikes_poultry
  , COALESCE( dislikes_binary.dislikes_shellfish, FALSE ) AS dislikes_shellfish
  , COALESCE( dislikes_binary.dislikes_vegetarian, FALSE ) AS dislikes_vegetarian
  , COALESCE( referrals.referrals_earned_total, 0 ) AS referrals_earned_total
  , COALESCE( referrals.referrals_sent_total, 0 ) AS referrals_sent_total
  , COALESCE( referrals.referrals_redeemed_total, 0 ) AS referrals_redeemed_total
  , COALESCE( referrals.referrals_earned_week, 0 ) AS referrals_earned_week
  , COALESCE( referrals.referrals_sent_week, 0 ) AS referrals_sent_week
  , COALESCE( referrals.referrals_redeemed_week, 0 ) AS referrals_redeemed_week
  , COALESCE( referrals.referrals_earned_month, 0 ) AS referrals_earned_month
  , COALESCE( referrals.referrals_sent_month, 0 ) AS referrals_sent_month
  , COALESCE( referrals.referrals_redeemed_month, 0 ) AS referrals_redeemed_month
  , COALESCE( ratings.five_star_count_total, 0 ) AS five_star_count_total
  , COALESCE( ratings.four_star_count_total, 0 ) AS four_star_count_total
  , COALESCE( ratings.three_star_count_total, 0 ) AS three_star_count_total
  , COALESCE( ratings.two_star_count_total, 0 ) AS two_star_count_total
  , COALESCE( ratings.one_star_count_total, 0 ) AS one_star_count_total
  , COALESCE( ratings.total_rating_count, 0 ) AS total_rating_count
  , COALESCE( ratings.total_star_ratings_on_website, 0 ) AS total_star_ratings_on_website
  , COALESCE( ratings.total_star_ratings_on_mobile, 0 ) AS total_star_ratings_on_mobile
  , COALESCE( ratings.total_reviews_on_website, 0 ) AS total_reviews_on_website
  , COALESCE( ratings.total_reviews_on_mobile, 0 ) AS total_reviews_on_mobile
  , COALESCE( ratings.total_review_count, 0 ) AS total_review_count
  , COALESCE( ratings.five_star_count_week, 0 ) AS five_star_count_week
  , COALESCE( ratings.four_star_count_week, 0 ) AS four_star_count_week
  , COALESCE( ratings.three_star_count_week, 0 ) AS three_star_count_week
  , COALESCE( ratings.two_star_count_week, 0 ) AS two_star_count_week
  , COALESCE( ratings.one_star_count_week, 0 ) AS one_star_count_week
  , COALESCE( ratings.total_rating_count_week, 0 ) AS total_rating_count_week
  , COALESCE( ratings.total_review_count_week, 0 ) AS total_review_count_week
  , COALESCE( ratings.five_star_count_month, 0 ) AS five_star_count_month
  , COALESCE( ratings.four_star_count_month, 0 ) AS four_star_count_month
  , COALESCE( ratings.three_star_count_month, 0 ) AS three_star_count_month
  , COALESCE( ratings.two_star_count_month, 0 ) AS two_star_count_month
  , COALESCE( ratings.one_star_count_month, 0 ) AS one_star_count_month
  , COALESCE( ratings.total_rating_count_month, 0 ) AS total_rating_count_month
  , COALESCE( ratings.total_review_count_month, 0 ) AS total_review_count_month
  , COALESCE( issued_credits.total_cx_credit_issued, 0 ) AS total_cx_credit_issued
  , COALESCE( issued_credits.total_marketing_credit_issued, 0 ) AS total_marketing_credit_issued
  , COALESCE( issued_credits.total_referral_credit_issued, 0 ) AS total_referral_credit_issued
  , COALESCE( issued_credits.total_cx_credit_issued_week, 0 ) AS total_cx_credit_issued_week
  , COALESCE( issued_credits.total_marketing_credit_issued_week, 0 )
  AS total_marketing_credit_issued_week
  , COALESCE( issued_credits.total_cx_credit_issued_month, 0 ) AS total_cx_credit_issued_month
  , COALESCE( issued_credits.total_marketing_credit_issued_month, 0 )
  AS total_marketing_credit_issued_month
  , COALESCE( cx_issues.boxes_with_issues_lifetime, 0 )
  AS boxes_with_issues_lifetime
  , COALESCE( cx_issues.delivery_not_received_lifetime, 0 )
  AS delivery_not_received_lifetime
  , COALESCE( cx_issues.delivery_late_lifetime, 0 )
  AS delivery_late_lifetime
  , COALESCE( cx_issues.box_damaged_lifetime, 0 )
  AS box_damaged_lifetime
  , COALESCE( cx_issues.box_changes_lifetime, 0 )
  AS box_changes_lifetime
  , COALESCE( cx_issues.product_help_lifetime, 0 )
  AS product_help_lifetime
  , COALESCE( cx_issues.payment_error_lifetime, 0 )
  AS payment_error_lifetime
  , COALESCE( cx_issues.food_safety_lifetime, 0 )
  AS food_safety_lifetime
  , COALESCE( cx_issues.nutrition_ingredient_lifetime, 0 )
  AS nutrition_ingredient_lifetime
  , COALESCE( cx_issues.spoiled_ingredient_lifetime, 0 )
  AS spoiled_ingredient_lifetime
  , COALESCE( cx_issues.missing_ingredient_lifetime, 0 )
  AS missing_ingredient_lifetime
  , COALESCE( cx_issues.missing_meal_lifetime, 0 )
  AS missing_meal_lifetime
  , COALESCE( cx_issues.culinary_confusion_lifetime, 0 )
  AS culinary_confusion_lifetime
  , COALESCE( cx_issues.poor_value_lifetime, 0 )
  AS poor_value_lifetime
  , COALESCE( cx_issues.culinary_taste_lifetime, 0 )
  AS culinary_taste_lifetime
  , COALESCE( cx_issues.boxes_with_issues_month, 0 )
  AS boxes_with_issues_month
  , COALESCE( cx_issues.delivery_not_received_month, 0 )
  AS delivery_not_received_month
  , COALESCE( cx_issues.delivery_late_month, 0 )
  AS delivery_late_month
  , COALESCE( cx_issues.box_damaged_month, 0 )
  AS box_damaged_month
  , COALESCE( cx_issues.box_changes_month, 0 )
  AS box_changes_month
  , COALESCE( cx_issues.product_help_month, 0 )
  AS product_help_month
  , COALESCE( cx_issues.payment_error_month, 0 )
  AS payment_error_month
  , COALESCE( cx_issues.food_safety_month, 0 )
  AS food_safety_month
  , COALESCE( cx_issues.nutrition_ingredient_month, 0 )
  AS nutrition_ingredient_month
  , COALESCE( cx_issues.spoiled_ingredient_month, 0 )
  AS spoiled_ingredient_month
  , COALESCE( cx_issues.missing_ingredient_month, 0 )
  AS missing_ingredient_month
  , COALESCE( cx_issues.missing_meal_month, 0 )
  AS missing_meal_month
  , COALESCE( cx_issues.culinary_confusion_month, 0 )
  AS culinary_confusion_month
  , COALESCE( cx_issues.poor_value_month, 0 )
  AS poor_value_month
  , COALESCE( cx_issues.culinary_taste_month, 0 )
  AS culinary_taste_month
  , COALESCE( cx_issues.boxes_with_issues_week, 0 )
  AS boxes_with_issues_week
  , COALESCE( cx_issues.delivery_not_received_week, 0 )
  AS delivery_not_received_week
  , COALESCE( cx_issues.delivery_late_week, 0 )
  AS delivery_late_week
  , COALESCE( cx_issues.box_damaged_week, 0 )
  AS box_damaged_week
  , COALESCE( cx_issues.box_changes_week, 0 )
  AS box_changes_week
  , COALESCE( cx_issues.product_help_week, 0 )
  AS product_help_week
  , COALESCE( cx_issues.payment_error_week, 0 )
  AS payment_error_week
  , COALESCE( cx_issues.food_safety_week, 0 )
  AS food_safety_week
  , COALESCE( cx_issues.nutrition_ingredient_week, 0 )
  AS nutrition_ingredient_week
  , COALESCE( cx_issues.spoiled_ingredient_week, 0 )
  AS spoiled_ingredient_week
  , COALESCE( cx_issues.missing_ingredient_week, 0 )
  AS missing_ingredient_week
  , COALESCE( cx_issues.missing_meal_week, 0 )
  AS missing_meal_week
  , COALESCE( cx_issues.culinary_confusion_week, 0 )
  AS culinary_confusion_week
  , COALESCE( cx_issues.poor_value_week, 0 )
  AS poor_value_week
  , COALESCE( cx_issues.culinary_taste_week, 0 )
  AS culinary_taste_week
  , COALESCE( android_browse_activity.num_days_using_android, 0 ) AS num_days_using_android
  , COALESCE( android_browse_activity.num_days_since_last_android_visit, 0 )
  AS num_days_since_last_android_visit
  , COALESCE( android_event_activity.num_recipe_changes_on_android, 0 )
  AS num_recipe_changes_on_android
  , COALESCE( android_event_activity.num_skips_on_android, 0 ) AS num_skips_on_android
  , COALESCE( android_event_activity.num_unskips_on_android, 0 ) AS num_unskips_on_android
  , COALESCE( ios_event_activity.num_recipe_changes_on_ios, 0 ) AS num_recipe_changes_on_ios
  , COALESCE( ios_event_activity.num_skips_on_ios, 0 ) AS num_skips_on_ios
  , COALESCE( ios_event_activity.num_unskips_on_ios, 0 ) AS num_unskips_on_ios
  , census.pop_population
  , census.pop_201004
  , census.pop_2010_2013
  , census.pop_2010
  , census.pop_under_5
  , census.pop_under_18
  , census.pop_65
  , census.pop_female
  , census.pop_white
  , census.pop_africanamerican
  , census.pop_nativeamerican
  , census.pop_asian
  , census.pop_pacificislander
  , census.pop_tworaces
  , census.pop_latino
  , census.pop_whitenonhispanic
  , census.pop_samehouse
  , census.pop_foreignborn
  , census.pop_languageother
  , census.edu_highschool
  , census.edu_bachelors
  , census.pop_veteran
  , census.hou_traveltime
  , census.hou_housingunits
  , census.hou_homeownership
  , census.hou_multiunit
  , census.hou_medianvalue
  , census.hou_households
  , census.hou_personsperhouse
  , census.inc_income
  , census.inc_median
  , census.inc_poverty
  , census.lan_area
  , census.lan_poppsm
FROM actives a
INNER JOIN dw.users u
  ON a.internal_user_id = u.internal_user_id
     AND u.source_created_at <= :end_date
LEFT JOIN latest_box_defaults b
  ON b.user_id = u.internal_user_id
LEFT JOIN web.boxes b2
  ON b2.user_id = b.user_id
     AND b2.delivery_date = b.delivery_date_default_address
LEFT JOIN web.user_addresses
  ON user_addresses.user_id = b2.user_id
     AND user_addresses.id = b2.shipping_address_id
LEFT JOIN dw.user_first_hit_utm
  ON user_first_hit_utm.internal_user_id = u.internal_user_id
LEFT JOIN user_card_type
  ON user_card_type.internal_user_id = u.internal_user_id
INNER JOIN cohorts
  ON cohorts.internal_user_id = u.internal_user_id
LEFT JOIN ratings
  ON ratings.external_user_id = u.external_id
LEFT JOIN referrals
  ON referrals.internal_user_id = u.internal_user_id
INNER JOIN four_week_order_rate
  ON four_week_order_rate.internal_user_id = u.internal_user_id
LEFT JOIN dislikes_binary
  ON dislikes_binary.internal_user_id = u.internal_user_id
LEFT JOIN recipes_aggregate
  ON recipes_aggregate.internal_user_id = u.internal_user_id
LEFT JOIN order_aggregate
  ON order_aggregate.internal_user_id = u.internal_user_id
LEFT JOIN issued_credits
  ON issued_credits.internal_user_id = u.internal_user_id
LEFT JOIN cx_issues
  ON cx_issues.internal_user_id = u.internal_user_id
LEFT JOIN web_browse_activity
  ON web_browse_activity.internal_user_id = u.internal_user_id
LEFT JOIN web_event_activity
  ON web_event_activity.internal_user_id = u.internal_user_id
LEFT JOIN ios_browse_activity
  ON ios_browse_activity.external_user_id = u.external_id
LEFT JOIN ios_event_activity
  ON ios_event_activity.external_user_id = u.external_id
LEFT JOIN android_event_activity
  ON android_event_activity.external_user_id = u.external_id
LEFT JOIN android_browse_activity
  ON android_browse_activity.external_user_id = u.external_id
LEFT JOIN census
  ON LEFT( user_addresses.zip_code, 5 ) = census.zip_code
LEFT JOIN employee
  ON u.internal_user_id = employee.user_id
WHERE employee.user_id IS NULL
      AND u.email NOT LIKE '%@plated.com'
