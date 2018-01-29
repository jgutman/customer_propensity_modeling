-- Eligible subscribers with an active subscription at the end date of the
-- input time period and have received at least 1 box by the end of the input
-- time period
WITH actives AS (
  SELECT u.internal_user_id
  FROM dw.user_subscription_events u
    LEFT JOIN dw.user_subscription_events i
      ON u.internal_user_id = i.internal_user_id
      AND i.subscription_changed_at > u.subscription_changed_at
      AND DATE(i.subscription_changed_at) <= :end_date
  WHERE
    i.internal_user_id IS NULL
    AND DATE(u.subscription_changed_at) <= :end_date
    AND u.subscription_status_change_event <> 'cancelation'
),
subscription_activated AS (
  SELECT a.internal_user_id
  , DATE(convert_timezone('America/New_York', source_created_at))
    AS join_date
  , email like '%@plated.com' AS plated_email
  FROM actives a
  INNER JOIN dw.users u
    ON a.internal_user_id = u.internal_user_id
),
boxes_ordered AS (
  SELECT a.internal_user_id
  , max(nth_delivery)
    AS n_boxes_delivered
  FROM actives a
  INNER JOIN dw.menu_order_boxes b
    ON a.internal_user_id = b.internal_user_id
    AND status = 'shipped'
    AND delivery_date <= :end_date
  GROUP BY 1
),
employee AS (
  SELECT DISTINCT user_id
  FROM web.users_discounts ud
  INNER JOIN web.discounts
    ON discount_id = discounts.id
  INNER JOIN web.discount_categories dc
    ON discount_category_id = dc.id
    AND dc.name IN ('corporate', 'employee')
  WHERE DATE(discounts.created_at) <= :end_date
)
SELECT a.internal_user_id
  , DATEDIFF(week, s.join_date, :end_date)
    AS weeks_since_activation
  , n_boxes_delivered
FROM actives a
  LEFT JOIN subscription_activated s
    ON a.internal_user_id = s.internal_user_id
  LEFT JOIN boxes_ordered bo
    ON a.internal_user_id = bo.internal_user_id
  LEFT JOIN employee e
    ON a.internal_user_id = e.user_id
WHERE e.user_id IS null
  AND NOT s.plated_email
  AND n_boxes_delivered IS NOT NULL
