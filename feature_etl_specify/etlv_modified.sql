with
actives as (
    select u.user_id AS internal_user_id
    from web.user_membership_status_changes u
    left join web.user_membership_status_changes i
        on u.user_id = i.user_id
        and i.created_at > u.created_at
        and date( i.created_at ) <= :end_date
    where
        i.user_id IS NULL
        and date( u.created_at ) <= :end_date
        and u.change_type IN (0,1,4)
        --activation, reactivation, unpause
),
latest_payment_profile as (
    select max(updated_at) as updated_at,
        user_id
    from web.vantiv_payment_profiles
    where card_type is not null
    group by 2
),
user_card_type as (
    select vpp.user_id AS internal_user_id,
        vpp.card_type
    from web.vantiv_payment_profiles vpp
    inner join latest_payment_profile lpp
      on lpp.user_id = vpp.user_id
      and vpp.updated_at = lpp.updated_at
),
mob as (
    select internal_user_id,
        delivery_schedule_name,
        status = 'canceled' AS canceled,
        min( delivery_date ) AS delivery_date,
        min( internal_menu_order_id ) AS internal_menu_order_id,
        min( date( menu_order_placed )) as order_billed_date,
        max( delivery_date ) as last_delivery_date
    from dw.menu_order_boxes
    where
        delivery_schedule_type = 'normal'
        and date( provisional_box_locked_at ) <= :end_date
  GROUP BY 1,2,3
),
cohorts as (
    select internal_user_id,
        min( delivery_date) AS first_delivery_date,
        min( order_billed_date ) AS first_order_billed_date,
        max( case when last_delivery_date <= :end_date
          then last_delivery_date end)
          AS most_recent_delivery_date
    from mob
    where not canceled
    GROUP BY 1
),
four_week_order_rate as (
    select mob.internal_user_id,
      count( mob.delivery_date ) AS num_boxes_first_4_weeks,
      sum( case when DATEDIFF('week', cohorts.first_delivery_date,
          mob.delivery_date ) = 3 then 1 end)
          AS ordered_week_4
    from cohorts
    inner join mob
      on cohorts.internal_user_id = mob.internal_user_id
      and DATEDIFF('week', cohorts.first_delivery_date, mob.delivery_date) < 4
      and not mob.canceled
    GROUP BY 1
),
order_aggregate as (
    select mob.internal_user_id,
        sum( case when not mob.canceled
          then 1 end ) AS num_deliveries,
        sum( case when mob.canceled
          then 1 end ) AS num_canceled_orders,
        count( distinct case when not mob.canceled
            then shipping_address_line_1 || shipping_zip_code end )
            AS num_addresses_delivered,
        avg( cast( case when not mob.canceled
            then transit_time end as float ))
            AS avg_transit_time,
        sum( case when not mob.canceled
            then gov end) AS total_gov,
        avg( case when not mob.canceled
            then gov end) AS avg_gov,
        sum( case when not mob.canceled
            then subscription_plates end )
            AS total_subscription_plates,
        sum( case when not mob.canceled
            then dessert_plates end )
            AS total_dessert_plates,
        avg( case when not mob.canceled
            then subscription_plates end )
            AS avg_plates_per_box,
        sum( case when num_2_portion_main_plates > 0
            and not mob.canceled
            then 1 end ) AS num_2_serving_boxes,
        sum( case when num_3_portion_main_plates > 0
            and not mob.canceled
            then 1 end ) AS num_3_serving_boxes,
        sum( case when num_4_portion_main_plates > 0
            and not mob.canceled
            then 1 end ) AS num_4_serving_boxes,
        sum( case when not mob.canceled
            and mob.delivery_date >= :end_date - 28
            then 1 end ) AS num_deliveries_month,
        sum( case when mob.canceled
            and mob.delivery_date >= :end_date - 28
            then 1 end ) AS num_canceled_orders_month,
        count( distinct case when not mob.canceled
            and mob.delivery_date >= :end_date - 28
            then shipping_address_line_1 || shipping_zip_code end )
            AS num_addresses_delivered_month,
        sum( case when not mob.canceled
            and mob.delivery_date >= :end_date - 28
            then gov end ) AS total_gov_month,
        avg( case when not mob.canceled
            and mob.delivery_date >= :end_date - 28
            then gov end ) AS avg_gov_month,
        sum( case when not mob.canceled
            and mob.delivery_date >= :end_date - 28
            then dessert_plates end )
            AS total_dessert_plates_month
    from mob
    inner join dw.menu_order_boxes mob2
      on mob.internal_menu_order_id = mob2.internal_menu_order_id
    GROUP BY 1
),
ecommerce_menu_order_recipes_re as (
    select distinct ecommerce_menu_orders.id,
        recipe_id
    from web.ecommerce_menu_orders
    inner join web.boxes
        on ecommerce_menu_orders.box_id = boxes.id
    inner join web.receipts
        on boxes.receipt_id = receipts.id
    inner join web.receipt_line_items
        on receipt_line_items.receipt_id = receipts.id
    inner join web.ecommerce_recipe_versions
        on receipt_line_items.item_id = ecommerce_recipe_versions.id
        and receipt_line_items.item_type = 'Ecommerce::RecipeVersion'
),
recipes_aggregate as (
    select mob.internal_user_id,
        count( r.recipe_id ) AS total_recipes_ordered,
        count( distinct r.recipe_id ) AS distinct_recipes_ordered
    from mob
    inner join dw.menu_order_boxes mob2
        on mob.internal_menu_order_id = mob2.internal_menu_order_id
        and not mob.canceled
    inner join ecommerce_menu_order_recipes_re r
        on mob2.internal_menu_order_id = r.id
  GROUP BY 1
),
dislikes_historic as (
    select distinct wte.user_id,
      json_extract_path_text(properties, 'new_dislikes', true) AS all_dislikes
    from dw.web_track_events wte
    inner join (
        select user_id,
          event,
          max(client_timestamp) AS client_timestamp
        from dw.web_track_events
        where date( client_timestamp ) <= :end_date
        and event = 'Taste Preferences Updated'
        GROUP BY 1,2
     ) wte2
     on wte.user_id = wte2.user_id
       and wte.event = wte2.event
       and wte.client_timestamp = wte2.client_timestamp
    where json_extract_path_text( properties, 'new_dislikes', true) <> '[]'
),
dislikes_current as (
  select uni.user_id,
      listagg('"' || ert.slug || '"') AS all_dislikes
  from web.user_no_interests uni
  inner join web.ecommerce_recipe_tags ert
    on uni.tag_id = ert.id
  where date( uni.created_at ) <= :end_date
  GROUP BY 1
),
dislikes_binary as (
    select coalesce( dh.user_id, dc.user_id ) AS internal_user_id,
      coalesce( dh.all_dislikes, dc.all_dislikes )
        like '%"beef"%' AS dislikes_beef,
      coalesce( dh.all_dislikes, dc.all_dislikes )
        like '%"fish"%' AS dislikes_fish,
      coalesce( dh.all_dislikes, dc.all_dislikes )
        like '%"lamb"%' AS dislikes_lamb,
      coalesce( dh.all_dislikes, dc.all_dislikes )
        like '%"pork"%' AS dislikes_pork,
      coalesce( dh.all_dislikes, dc.all_dislikes )
        like '%"poultry"%' AS dislikes_poultry,
      coalesce( dh.all_dislikes, dc.all_dislikes )
        like '%"shellfish"%' AS dislikes_shellfish,
      coalesce( dh.all_dislikes, dc.all_dislikes )
        like '%"vegetarian"%' AS dislikes_vegetarian
    from dislikes_historic dh
    full outer join dislikes_current dc
    on dc.user_id = dh.user_id
),
referrals as (
    select referrer_internal_user_id AS internal_user_id,
        count( r.* ) AS referrals_earned_total,
        count( case when date(sent_at) <= :end_date
            then 1 end ) AS referrals_sent_total,
        count( case when date(converted_at) <= :end_date
            then 1 end ) AS referrals_redeemed_total,
        sum( case when date( referral_issued_at ) >= :end_date - 7
          then 1 end ) AS referrals_earned_week,
        sum( case when date( sent_at ) >= :end_date - 7
          then 1 end ) AS referrals_sent_week,
        sum( case when date( converted_at ) >= :end_date - 7
          then 1 end ) AS referrals_redeemed_week,
        sum( case when date( referral_issued_at ) >= :end_date - 28
          then 1 end ) AS referrals_earned_month,
        sum( case when date( sent_at ) >= :end_date - 28
          then 1 end ) AS referrals_sent_month,
        sum( case when date( converted_at ) >= :end_date - 28
          then 1 end ) AS referrals_redeemed_month
    from dw.user_referral_invites
    where date( referral_issued_at ) <= :end_date
    GROUP BY 1
),
ratings as (
    select user_id as external_user_id,
        sum( case value when 5 then 1 end )
            AS five_star_count_total,
        sum( case value when 4 then 1 end )
            AS four_star_count_total,
        sum( case value when 3 then 1 end )
            AS three_star_count_total,
        sum( case value when 2 then 1 end )
            AS two_star_count_total,
        sum( case value when 1 then 1 end )
            AS one_star_count_total,
        count(*) AS total_rating_count,
        count( case when source = 'website'
            then 1 end ) AS total_star_ratings_on_website,
        count( case when source = 'mobile'
            then 1 end ) AS total_star_ratings_on_mobile,
        count( case when source = 'website'
            then notes end ) AS total_reviews_on_website,
        count( case when source = 'mobile'
            then notes end ) AS total_reviews_on_mobile,
        count(notes) AS total_review_count,
        sum( case when value = 5
             and date( created_at ) >= :end_date - 7
             then 1 end ) AS five_star_count_week,
        sum( case when value = 4
             and date( created_at ) >= :end_date - 7
             then 1 end ) AS four_star_count_week,
        sum( case when value = 3
             and date( created_at ) >= :end_date - 7
             then 1 end ) AS three_star_count_week,
        sum( case when value = 2
             and date( created_at ) >= :end_date - 7
             then 1 end ) AS two_star_count_week,
        sum( case when value = 1
             and date( created_at ) >= :end_date - 7
             then 1 end ) AS one_star_count_week,
        sum( case when date( created_at ) >= :end_date - 7
             then 1 end ) AS total_rating_count_week,
        count( case when date( created_at ) >= :end_date - 7
             then notes end ) AS total_review_count_week,
        sum( case when value = 5
             and date( created_at ) >= :end_date - 28
             then 1 end ) AS five_star_count_month,
        sum( case when value = 4
             and date( created_at ) >= :end_date - 28
             then 1 end ) AS four_star_count_month,
        sum( case when value = 3
             and date( created_at ) >= :end_date - 28
             then 1 end ) AS three_star_count_month,
        sum( case when value = 2
             and date( created_at ) >= :end_date - 28
             then 1 end ) AS two_star_count_month,
        sum( case when value = 1
             and date( created_at ) >= :end_date - 28
             then 1 end ) AS one_star_count_month,
        sum( case when date( created_at ) >= :end_date - 28
             then 1 end ) AS total_rating_count_month,
        count( case when date( created_at ) >= :end_date - 28
             then notes end ) AS total_review_count_month
    from review.reviews
      where date(reviews.created_at) <= :end_date
    group by 1
),
issued_credits as (
    select user_id AS internal_user_id,
        sum( case when event_type IN (
              'User::ServiceLog',
              'ReplacementCreditIssuance')
            then balance_change end )
            AS total_cx_credit_issued,
        sum( case when event_type IN (
              'User::PromoRedemption',
              'LoyaltyMarketingEvent',
              'PromotionalCreditIssuance')
             then balance_change end )
             AS total_marketing_credit_issued,
        sum( case when event_type = 'ReferralConversion'
            then balance_change end )
            AS total_referral_credit_issued,
        sum( case when event_type IN (
              'User::ServiceLog',
              'ReplacementCreditIssuance')
            and date( created_at ) >= :end_date - 7
            then balance_change end )
            AS total_cx_credit_issued_week,
        sum( case when event_type IN (
              'User::PromoRedemption',
              'LoyaltyMarketingEvent',
              'PromotionalCreditIssuance')
            and date( created_at ) >= :end_date - 7
            then balance_change end )
            AS total_marketing_credit_issued_week,
        sum( case when event_type IN (
              'User::ServiceLog',
              'ReplacementCreditIssuance')
            and date( created_at ) >= :end_date - 28
            then balance_change end )
            AS total_cx_credit_issued_month,
        sum( case when event_type IN (
              'User::PromoRedemption',
              'LoyaltyMarketingEvent',
              'PromotionalCreditIssuance')
            and date( created_at ) >= :end_date - 28
            then balance_change end)
            AS total_marketing_credit_issued_month
    from web.credit_events
    where date( created_at ) <= :end_date
    and balance_change > 0
    GROUP BY 1
),
cx_issues as (
    select internal_user_id,
      count( distinct internal_menu_order_id )
        AS boxes_with_issues_lifetime,
      count( case issue_feature
         when 'Delivery issue (did not receive)'
         then 1 end ) AS delivery_not_received_lifetime,
      count( case issue_feature
         when 'Delivery issue (received late)'
         then 1 end ) AS delivery_late_lifetime,
      count( case issue_feature
         when 'Box damaged in transit'
         then 1 end ) AS box_damaged_lifetime,
      count( case issue_feature
         when 'Change/Skip box'
         then 1 end ) AS box_changes_lifetime,
      count( case issue_feature
         when 'Site usage/Product help'
         then 1 end ) AS product_help_lifetime,
      count( case issue_feature
         when 'Payment error/credit/refund'
         then 1 end ) AS payment_error_lifetime,
      count( case issue_feature
         when 'Food safety'
         then 1 end) AS food_safety_lifetime,
      count( case issue_feature
         when 'Nutrition/dietary/ingredient inquiry'
         then 1 end ) AS nutrition_ingredient_lifetime,
      count( case issue_feature
         when 'Spoiled/Compromised ingredient'
         then 1 end ) AS spoiled_ingredient_lifetime,
      count( case issue_feature
         when 'Missing/Wrong ingredient'
         then 1 end ) AS missing_ingredient_lifetime,
      count( case issue_feature
         when 'Missing/Wrong meal'
         then 1 end ) AS missing_meal_lifetime,
      count( case issue_feature
         when 'Culinary issue: Confusion'
         then 1 end ) AS culinary_confusion_lifetime,
      count( case issue_feature
         when 'Poor/Inadequate value'
         then 1 end ) AS poor_value_lifetime,
      count( case issue_feature
         when 'Culinary issue: taste'
         then 1 end ) AS culinary_taste_lifetime,
      count( distinct case
         when date( logged_at ) >= :end_date - 28
         then internal_menu_order_id end )
         AS boxes_with_issues_month,
      count( case when date( logged_at ) >= :end_date - 28
         and issue_feature = 'Delivery issue (did not receive)'
         then 1 end ) AS delivery_not_received_month,
      count( case when date( logged_at ) >= :end_date - 28
         and issue_feature = 'Delivery issue (received late)'
         then 1 end ) AS delivery_late_month,
      count( case when date( logged_at ) >= :end_date - 28
         and issue_feature = 'Box damaged in transit'
         then 1 end ) AS box_damaged_month,
      count( case when date( logged_at ) >= :end_date - 28
         and issue_feature = 'Change/Skip box'
         then 1 end ) AS box_changes_month,
      count( case when date( logged_at ) >= :end_date - 28
         and issue_feature = 'Site usage/Product help'
         then 1 end ) AS product_help_month,
      count( case when date( logged_at ) >= :end_date - 28
         and issue_feature = 'Payment error/credit/refund'
         then 1 end ) AS payment_error_month,
      count( case when date( logged_at ) >= :end_date - 28
         and issue_feature = 'Food safety'
         then 1 end ) AS food_safety_month,
      count( case when date( logged_at ) >= :end_date - 28
         and issue_feature = 'Nutrition/dietary/ingredient inquiry'
         then 1 end ) AS nutrition_ingredient_month,
      count( case when date( logged_at ) >= :end_date - 28
         and issue_feature = 'Spoiled/Compromised ingredient'
         then 1 end ) AS spoiled_ingredient_month,
      count( case when date( logged_at ) >= :end_date - 28
         and issue_feature = 'Missing/Wrong ingredient'
         then 1 END ) AS missing_ingredient_month,
      count( case when date( logged_at ) >= :end_date - 28
         and issue_feature = 'Missing/Wrong meal'
         then 1 end ) AS missing_meal_month,
      count( case when date( logged_at ) >= :end_date - 28
         and issue_feature = 'Culinary issue: Confusion'
         then 1 end ) AS culinary_confusion_month,
      count( case when date( logged_at ) >= :end_date - 28
         and issue_feature = 'Poor/Inadequate value'
         then 1 end ) AS poor_value_month,
      count( case when date( logged_at ) >= :end_date - 28
         and issue_feature = 'Culinary issue: taste'
         then 1 end ) AS culinary_taste_month,
      count( distinct case
         when date( logged_at ) >= :end_date - 7
         then internal_menu_order_id end )
         AS boxes_with_issues_week,
      count( case when date( logged_at ) >= :end_date - 7
         and issue_feature = 'Delivery issue (did not receive)'
         then 1 end ) AS delivery_not_received_week,
      count( case when date( logged_at ) >= :end_date - 7
         and issue_feature = 'Delivery issue (received late)'
         then 1 end ) AS delivery_late_week,
      count( case when date( logged_at ) >= :end_date - 7
         and issue_feature = 'Box damaged in transit'
         then 1 end ) AS box_damaged_week,
      count( case when date( logged_at ) >= :end_date - 7
         and issue_feature = 'Change/Skip box'
         then 1 end ) AS box_changes_week,
      count( case when date( logged_at ) >= :end_date - 7
         and issue_feature = 'Site usage/Product help'
         then 1 end ) AS product_help_week,
      count( case when date( logged_at ) >= :end_date - 7
         and issue_feature = 'Payment error/credit/refund'
         then 1 end ) AS payment_error_week,
      count( case when date( logged_at ) >= :end_date - 7
         and issue_feature = 'Food safety'
         then 1 end ) AS food_safety_week,
      count( case when date( logged_at ) >= :end_date - 7
         and issue_feature = 'Nutrition/dietary/ingredient inquiry'
         then 1 end ) AS nutrition_ingredient_week,
      count( case when date( logged_at ) >= :end_date - 7
         and issue_feature = 'Spoiled/Compromised ingredient'
         then 1 end ) AS spoiled_ingredient_week,
      count( case when date( logged_at ) >= :end_date - 7
         and issue_feature = 'Missing/Wrong ingredient'
         then 1 end ) AS missing_ingredient_week,
      count( case when date( logged_at ) >= :end_date - 7
         and issue_feature = 'Missing/Wrong meal'
         then 1 end ) AS missing_meal_week,
      count( case when date( logged_at ) >= :end_date - 7
         and issue_feature = 'Culinary issue: Confusion'
         then 1 end ) AS culinary_confusion_week,
      count( case when date( logged_at ) >= :end_date - 7
         and issue_feature = 'Poor/Inadequate value'
         then 1 end ) AS poor_value_week,
      count( case when date( logged_at ) >= :end_date - 7
         and issue_feature = 'Culinary issue: taste'
         then 1 end ) AS culinary_taste_week
    from dw.user_service_logs usl
    inner join analytics.cx_issues_grouping_lookup cigl
      on cigl.reported_issue = lower( usl.reported_issue )
    where date( logged_at ) <= :end_date
    GROUP BY 1
),
web_browse_activity as (
    select user_id AS internal_user_id,
        count( distinct date( client_timestamp )) AS num_days_using_website,
        DATEDIFF('day', max( date( client_timestamp )), :end_date)
          AS num_days_since_last_web_visit
    from dw.web_page_visits
      where date( client_timestamp ) <= :end_date
    GROUP BY 1
),
web_event_activity as(
    select user_id AS internal_user_id,
        count( distinct case when event = 'Skipped Box'
          then client_timestamp end) AS num_skips_on_website,
        count( distinct case when event = 'Unskipped Box'
          then client_timestamp end) AS num_unskips_on_website,
        count( distinct case when event = 'Box Recipes Changed'
          then client_timestamp end) AS num_recipe_changes_on_website
    from dw.web_track_events
      where date( client_timestamp ) <= :end_date
      and event IN ('Skipped Box', 'Unskipped Box', 'Box Recipes Changed')
    GROUP BY 1
),
ios_browse_activity as (
    select external_user_id,
        count( distinct date( client_timestamp )) AS num_days_using_ios,
        DATEDIFF('day', max( date( client_timestamp )), :end_date)
          AS num_days_since_last_ios_visit
    from dw.app_screen_views
    where date( client_timestamp ) <= :end_date
    GROUP BY 1
),
android_browse_activity as(
    select external_user_id,
        count( distinct date( client_timestamp )) AS num_days_using_android,
        DATEDIFF('day', max( DATE( client_timestamp )), :end_date)
          AS num_days_since_last_android_visit
    from dw.android_views
    where date( client_timestamp ) <= :end_date
    GROUP BY 1
),
ios_event_activity as(
    select external_user_id,
        count( distinct case when
          event_name = 'Selected another recipe while editing a box'
          then client_timestamp end)
          AS num_recipe_changes_on_ios,
        count(distinct case when (event_name IN (
            'Tapped skip week button',
            'Skipped a week from the Feed',
            'Tapped the skip button in the feature tour'))
          then client_timestamp end) AS num_skips_on_ios,
        count(distinct case when
          event_name = 'Unskipped a week from the Feed'
          then client_timestamp end) AS num_unskips_on_ios
    from dw.app_track_events
    where date( client_timestamp ) <= :end_date
      and event_name IN (
        'Selected another recipe while editing a box',
        'Tapped skip week button',
        'Skipped a week from the Feed',
        'Tapped the skip button in the feature tour',
        'Unskipped a week from the Feed')
    GROUP BY 1
),
android_event_activity as(
    select external_user_id,
        count( distinct case when
          event_name = 'Tapped Swap in My Box on Upcoming Modify Box'
          then client_timestamp end ) AS num_recipe_changes_on_android,
        count( distinct case when
          event_name = 'Tapped "Skip Week" on Upcoming Home'
          then client_timestamp end ) AS num_skips_on_android,
        count( distinct case when
          event_name = 'Tapped "Unskip Week" on Upcoming Home'
          then client_timestamp end ) AS num_unskips_on_android
    from dw.android_events
    where date( client_timestamp ) <= :end_date
      and event_name IN (
        'Tapped Swap in My Box on Upcoming Modify Box',
        'Tapped "Skip Week" on Upcoming Home',
        'Tapped "Unskip Week" on Upcoming Home')
    GROUP BY 1
),
census as (
    select zip_code,
      avg(pop_population) AS pop_population,
      avg(pop_201004) AS pop_201004,
      avg(pop_2010_2013) AS pop_2010_2013,
      avg(pop_2010) AS pop_2010,
      avg(pop_under_5) AS pop_under_5,
      avg(pop_under_18) AS pop_under_18,
      avg(pop_65) AS pop_65,
      avg(pop_samehouse) AS pop_samehouse,
      avg(pop_foreignborn) AS pop_foreignborn,
      avg(pop_languageother) AS pop_languageother,
      avg(edu_highschool) AS edu_highschool,
      avg(edu_bachelors) AS edu_bachelors,
      avg(pop_veteran) AS pop_veteran,
      avg(hou_traveltime) AS hou_traveltime,
      avg(hou_housingunits) AS hou_housingunits,
      avg(hou_homeownership) AS hou_homeownership,
      avg(hou_multiunit) AS hou_multiunit,
      avg(hou_medianvalue) AS hou_medianvalue,
      avg(hou_households) AS hou_households,
      avg(hou_personsperhouse) AS hou_personsperhouse,
      avg(inc_income) AS inc_income,
      avg(inc_median) AS inc_median,
      avg(inc_poverty) AS inc_poverty,
      avg(lan_area) AS lan_area,
      avg(lan_poppsm) AS lan_poppsm
    from dw.demographics_census
    GROUP BY 1
),
latest_box_defaults as (
    select internal_user_id,
      max( case when not provisional_box_custom_address
        then internal_menu_order_id end ) AS default_address,
      max( case when not provisional_box_custom_delivery_date
        then delivery_date end ) AS default_day
    from dw.menu_order_boxes
    where delivery_schedule_type = 'normal'
        and date( provisional_box_locked_at ) <= :end_date
    GROUP BY 1
),
first_last_hit as (
    select distinct internal_user_id,
      first_hit_utm_medium,
      last_hit_utm_medium
    from dw.user_first_hit_utm
    where date( first_order_placed_at ) <= :end_date
),
default_address as (
    select distinct internal_menu_order_id,
      shipping_zip_code,
      shipping_city,
      shipping_state
    from dw.menu_order_boxes
    where delivery_schedule_type = 'normal'
      and date( provisional_box_locked_at ) <= :end_date
      and not provisional_box_custom_address
),
employee as (
    select distinct ud.user_id
    from web.users_discounts ud
    inner join web.discounts
      on discount_id = discounts.id
    inner join web.discount_categories dc
      on discount_category_id = dc.id
      and dc.name = 'employee'
    where date( discounts.created_at ) <= :end_date
)
select
    a.internal_user_id,
    date( source_created_at) <= :end_date AS valid_account_creation,
    date( prospect_created_at ) <= :end_date AS valid_prospect_creation,
    date( accepted_terms ) <= :end_date AS valid_accepted_terms,
    DATE_DIFF( 'day', u.prospect_created_at,
      cohorts.first_delivery_date )
      AS days_from_email_submission_to_first_delivery,
    DATE_DIFF( 'day', u.source_created_at,
      cohorts.first_delivery_date)
      AS days_from_account_creation_to_first_delivery,
    DATE_DIFF( 'day', u.accepted_terms,
      cohorts.first_delivery_date)
      AS days_from_accepting_terms_to_first_delivery,
    DATE_DIFF( 'day', cohorts.first_order_billed_date,
      cohorts.first_delivery_date )
      AS days_from_conversion_to_first_delivery,
    DATE_DIFF( 'day', cohorts.most_recent_delivery_date,
      :end_date ) AS days_from_most_recent_delivery,
    split_part( u.email, '@', 2 ) AS email_domain,
    to_char( b.default_day, 'Day') AS preferred_delivery_day,
    b2.shipping_city,
    b2.shipping_state,
    u.prospect_source_domain,
    u.who_referred_me is not null AS referral_state,
    first_last_hit.first_hit_utm_medium,
    first_last_hit.last_hit_utm_medium,
    user_card_type.card_type,
    four_week_order_rate.num_boxes_first_4_weeks,
    COALESCE( four_week_order_rate.ordered_week_4, 0) AS ordered_week_4,
    order_aggregate.num_deliveries,
    COALESCE( order_aggregate.num_canceled_orders, 0) AS num_canceled_orders,
    order_aggregate.num_addresses_delivered,
    order_aggregate.avg_transit_time,
    order_aggregate.total_gov,
    order_aggregate.avg_gov,
    order_aggregate.total_subscription_plates,
    order_aggregate.total_dessert_plates,
    order_aggregate.avg_plates_per_box,
    COALESCE( order_aggregate.num_2_serving_boxes, 0) AS num_2_serving_boxes,
    COALESCE( order_aggregate.num_3_serving_boxes, 0) AS num_3_serving_boxes,
    COALESCE( order_aggregate.num_4_serving_boxes, 0) AS num_4_serving_boxes,
    COALESCE( order_aggregate.num_deliveries_month, 0) AS num_deliveries_month,
    COALESCE( order_aggregate.num_canceled_orders_month, 0)
      AS num_canceled_orders_month,
    order_aggregate.num_addresses_delivered_month,
    COALESCE( order_aggregate.total_gov_month, 0) AS total_gov_month,
    COALESCE( order_aggregate.avg_gov_month, 0) AS avg_gov_month,
    COALESCE( order_aggregate.total_dessert_plates_month, 0)
      AS total_dessert_plates_month,
    COALESCE( recipes_aggregate.total_recipes_ordered, 0)
      AS total_recipes_ordered,
    COALESCE( recipes_aggregate.distinct_recipes_ordered, 0)
      AS distinct_recipes_ordered,
    COALESCE( dislikes_binary.dislikes_beef, false) AS dislikes_beef,
    COALESCE( dislikes_binary.dislikes_fish, false) AS dislikes_fish,
    COALESCE( dislikes_binary.dislikes_lamb, false) AS dislikes_lamb,
    COALESCE( dislikes_binary.dislikes_pork, false) AS dislikes_pork,
    COALESCE( dislikes_binary.dislikes_poultry, false) AS dislikes_poultry,
    COALESCE( dislikes_binary.dislikes_shellfish, false)
      AS dislikes_shellfish,
    COALESCE( dislikes_binary.dislikes_vegetarian, false)
      AS dislikes_vegetarian,
    COALESCE( referrals.referrals_earned_total, 0) AS referrals_earned_total,
    COALESCE( referrals.referrals_sent_total, 0) AS referrals_sent_total,
    COALESCE( referrals.referrals_redeemed_total, 0) AS referrals_redeemed_total,
    COALESCE( referrals.referrals_earned_week, 0) AS referrals_earned_week,
    COALESCE( referrals.referrals_sent_week, 0) AS referrals_sent_week,
    COALESCE( referrals.referrals_redeemed_week, 0) AS referrals_redeemed_week,
    COALESCE( referrals.referrals_earned_month, 0) AS referrals_earned_month,
    COALESCE( referrals.referrals_sent_month, 0) AS referrals_sent_month,
    COALESCE( referrals.referrals_redeemed_month, 0) AS referrals_redeemed_month,
    COALESCE( ratings.five_star_count_total, 0) AS five_star_count_total,
    COALESCE( ratings.four_star_count_total, 0) AS four_star_count_total,
    COALESCE( ratings.three_star_count_total, 0) AS three_star_count_total,
    COALESCE( ratings.two_star_count_total, 0) AS two_star_count_total,
    COALESCE( ratings.one_star_count_total, 0) AS one_star_count_total,
    COALESCE( ratings.total_rating_count, 0) AS total_rating_count,
    COALESCE( ratings.total_star_ratings_on_website, 0)
      AS total_star_ratings_on_website,
    COALESCE( ratings.total_star_ratings_on_mobile, 0)
      AS total_star_ratings_on_mobile,
    COALESCE( ratings.total_reviews_on_website, 0) AS total_reviews_on_website,
    COALESCE( ratings.total_reviews_on_mobile, 0) AS total_reviews_on_mobile,
    COALESCE( ratings.total_review_count, 0) AS total_review_count,
    COALESCE( ratings.five_star_count_week, 0) AS five_star_count_week,
    COALESCE( ratings.four_star_count_week, 0) AS four_star_count_week,
    COALESCE( ratings.three_star_count_week, 0) AS three_star_count_week,
    COALESCE( ratings.two_star_count_week, 0) AS two_star_count_week,
    COALESCE( ratings.one_star_count_week, 0) AS one_star_count_week,
    COALESCE( ratings.total_rating_count_week, 0) AS total_rating_count_week,
    COALESCE( ratings.total_review_count_week, 0) AS total_review_count_week,
    COALESCE( ratings.five_star_count_month, 0) AS five_star_count_month,
    COALESCE( ratings.four_star_count_month, 0) AS four_star_count_month,
    COALESCE( ratings.three_star_count_month, 0) AS three_star_count_month,
    COALESCE( ratings.two_star_count_month, 0) AS two_star_count_month,
    COALESCE( ratings.one_star_count_month, 0) AS one_star_count_month,
    COALESCE( ratings.total_rating_count_month, 0) AS total_rating_count_month,
    COALESCE( ratings.total_review_count_month, 0) AS total_review_count_month,
    COALESCE( issued_credits.total_cx_credit_issued, 0)
      AS total_cx_credit_issued,
    COALESCE( issued_credits.total_marketing_credit_issued, 0)
      AS total_marketing_credit_issued,
    COALESCE( issued_credits.total_referral_credit_issued, 0)
      AS total_referral_credit_issued,
    COALESCE( issued_credits.total_cx_credit_issued_week, 0)
      AS total_cx_credit_issued_week,
    COALESCE( issued_credits.total_marketing_credit_issued_week, 0)
      AS total_marketing_credit_issued_week,
    COALESCE( issued_credits.total_cx_credit_issued_month, 0)
      AS total_cx_credit_issued_month,
    COALESCE( issued_credits.total_marketing_credit_issued_month, 0)
      AS total_marketing_credit_issued_month,
    COALESCE( cx_issues.boxes_with_issues_lifetime, 0 )
      AS boxes_with_issues_lifetime,
    COALESCE( cx_issues.delivery_not_received_lifetime, 0 )
      AS delivery_not_received_lifetime,
    COALESCE( cx_issues.delivery_late_lifetime, 0 )
      AS delivery_late_lifetime,
    COALESCE( cx_issues.box_damaged_lifetime, 0 )
      AS box_damaged_lifetime,
    COALESCE( cx_issues.box_changes_lifetime, 0 )
      AS box_changes_lifetime,
    COALESCE( cx_issues.product_help_lifetime, 0 )
      AS product_help_lifetime,
    COALESCE( cx_issues.payment_error_lifetime, 0 )
      AS payment_error_lifetime,
    COALESCE( cx_issues.food_safety_lifetime, 0 )
      AS food_safety_lifetime,
    COALESCE( cx_issues.nutrition_ingredient_lifetime, 0 )
      AS nutrition_ingredient_lifetime,
    COALESCE( cx_issues.spoiled_ingredient_lifetime, 0 )
      AS spoiled_ingredient_lifetime,
    COALESCE( cx_issues.missing_ingredient_lifetime, 0 )
      AS missing_ingredient_lifetime,
    COALESCE( cx_issues.missing_meal_lifetime, 0 )
      AS missing_meal_lifetime,
    COALESCE( cx_issues.culinary_confusion_lifetime, 0 )
      AS culinary_confusion_lifetime,
    COALESCE( cx_issues.poor_value_lifetime, 0 )
      AS poor_value_lifetime,
    COALESCE( cx_issues.culinary_taste_lifetime, 0 )
      AS culinary_taste_lifetime,
    COALESCE( cx_issues.boxes_with_issues_month, 0 )
      AS boxes_with_issues_month,
    COALESCE( cx_issues.delivery_not_received_month, 0 )
      AS delivery_not_received_month,
    COALESCE( cx_issues.delivery_late_month, 0 )
      AS delivery_late_month,
    COALESCE( cx_issues.box_damaged_month, 0 )
      AS box_damaged_month,
    COALESCE( cx_issues.box_changes_month, 0 )
      AS box_changes_month,
    COALESCE( cx_issues.product_help_month, 0 )
      AS product_help_month,
    COALESCE( cx_issues.payment_error_month, 0 )
      AS payment_error_month,
    COALESCE( cx_issues.food_safety_month, 0 )
      AS food_safety_month,
    COALESCE( cx_issues.nutrition_ingredient_month, 0 )
      AS nutrition_ingredient_month,
    COALESCE( cx_issues.spoiled_ingredient_month, 0 )
      AS spoiled_ingredient_month,
    COALESCE( cx_issues.missing_ingredient_month, 0 )
      AS missing_ingredient_month,
    COALESCE( cx_issues.missing_meal_month, 0 )
      AS missing_meal_month,
    COALESCE( cx_issues.culinary_confusion_month, 0 )
      AS culinary_confusion_month,
    COALESCE( cx_issues.poor_value_month, 0 )
      AS poor_value_month,
    COALESCE( cx_issues.culinary_taste_month, 0 )
      AS culinary_taste_month,
    COALESCE( cx_issues.boxes_with_issues_week, 0 )
      AS boxes_with_issues_week,
    COALESCE( cx_issues.delivery_not_received_week, 0 )
      AS delivery_not_received_week,
    COALESCE( cx_issues.delivery_late_week, 0 )
      AS delivery_late_week,
    COALESCE( cx_issues.box_damaged_week, 0 )
      AS box_damaged_week,
    COALESCE( cx_issues.box_changes_week, 0 )
      AS box_changes_week,
    COALESCE( cx_issues.product_help_week, 0 )
      AS product_help_week,
    COALESCE( cx_issues.payment_error_week, 0 )
      AS payment_error_week,
    COALESCE( cx_issues.food_safety_week, 0 )
      AS food_safety_week,
    COALESCE( cx_issues.nutrition_ingredient_week, 0 )
      AS nutrition_ingredient_week,
    COALESCE( cx_issues.spoiled_ingredient_week, 0 )
      AS spoiled_ingredient_week,
    COALESCE( cx_issues.missing_ingredient_week, 0 )
      AS missing_ingredient_week,
    COALESCE( cx_issues.missing_meal_week, 0 )
      AS missing_meal_week,
    COALESCE( cx_issues.culinary_confusion_week, 0 )
      AS culinary_confusion_week,
    COALESCE( cx_issues.poor_value_week, 0 )
      AS poor_value_week,
    COALESCE( cx_issues.culinary_taste_week, 0 )
      AS culinary_taste_week,
    COALESCE( web_browse_activity.num_days_using_website, 0)
      AS num_days_using_website,
    COALESCE( web_browse_activity.num_days_since_last_web_visit, 0)
      AS num_days_since_last_web_visit,
    COALESCE( web_event_activity.num_skips_on_website, 0)
      AS num_skips_on_website,
    COALESCE( web_event_activity.num_unskips_on_website, 0)
      AS num_unskips_on_website,
    COALESCE( web_event_activity.num_recipe_changes_on_website, 0)
      AS num_recipe_changes_on_website,
    COALESCE( ios_browse_activity.num_days_using_ios, 0)
      AS num_days_using_ios,
    COALESCE( ios_browse_activity.num_days_since_last_ios_visit, 0)
      AS num_days_since_last_ios_visit,
    COALESCE( android_browse_activity.num_days_using_android, 0)
      AS num_days_using_android,
    COALESCE( android_browse_activity.num_days_since_last_android_visit, 0)
      AS num_days_since_last_android_visit,
    COALESCE( android_event_activity.num_recipe_changes_on_android, 0)
      AS num_recipe_changes_on_android,
    COALESCE( android_event_activity.num_skips_on_android, 0)
      AS num_skips_on_android,
    COALESCE( android_event_activity.num_unskips_on_android, 0)
      AS num_unskips_on_android,
    COALESCE( ios_event_activity.num_recipe_changes_on_ios, 0)
      AS num_recipe_changes_on_ios,
    COALESCE( ios_event_activity.num_skips_on_ios, 0)
      AS num_skips_on_ios,
    COALESCE( ios_event_activity.num_unskips_on_ios, 0)
      AS num_unskips_on_ios,
    census.pop_population,
    census.pop_201004,
    census.pop_2010_2013,
    census.pop_2010,
    census.pop_under_5,
    census.pop_under_18,
    census.pop_65,
    census.pop_samehouse,
    census.pop_foreignborn,
    census.pop_languageother,
    census.edu_highschool,
    census.edu_bachelors,
    census.pop_veteran,
    census.hou_traveltime,
    census.hou_housingunits,
    census.hou_homeownership,
    census.hou_multiunit,
    census.hou_medianvalue,
    census.hou_households,
    census.hou_personsperhouse,
    census.inc_income,
    census.inc_median,
    census.inc_poverty,
    census.lan_area,
    census.lan_poppsm
from actives a
inner join dw.users u
  on a.internal_user_id = u.internal_user_id
inner join cohorts
  on cohorts.internal_user_id = u.internal_user_id
left join four_week_order_rate
  on four_week_order_rate.internal_user_id = u.internal_user_id
left join latest_box_defaults b
  on b.internal_user_id = u.internal_user_id
left join default_address b2
  on b2.internal_menu_order_id = b.default_address
left join first_last_hit
  on first_last_hit.internal_user_id = u.internal_user_id
left join user_card_type
  on user_card_type.internal_user_id = u.internal_user_id
left join ratings
  on ratings.external_user_id = u.external_id
left join referrals
  on referrals.internal_user_id = u.internal_user_id
left join dislikes_binary
  on dislikes_binary.internal_user_id = u.internal_user_id
left join recipes_aggregate
  on recipes_aggregate.internal_user_id = u.internal_user_id
left join order_aggregate
  on order_aggregate.internal_user_id = u.internal_user_id
left join issued_credits
  on issued_credits.internal_user_id = u.internal_user_id
left join cx_issues
  on cx_issues.internal_user_id = u.internal_user_id
left join web_browse_activity
  on web_browse_activity.internal_user_id = u.internal_user_id
left join web_event_activity
  on web_event_activity.internal_user_id = u.internal_user_id
left join ios_browse_activity
  on ios_browse_activity.external_user_id = u.external_id
left join ios_event_activity
  on ios_event_activity.external_user_id = u.external_id
left join android_event_activity
  on android_event_activity.external_user_id = u.external_id
left join android_browse_activity
  on android_browse_activity.external_user_id = u.external_id
left join census
  on LEFT(b2.shipping_zip_code, 5) = census.zip_code
left join employee
  ON u.internal_user_id = employee.user_id
where employee.user_id IS null
  and u.email not like '%@plated.com'
