WITH my_CTE AS (
    SELECT 
        gp.user_id, 
        gp.revenue_amount_usd, 
        gp.payment_date,
        gpu."language", 
        gpu.game_name,
        gpu.age,
        DATE_TRUNC('month', gp.payment_date)::date AS month_start,
        MIN(DATE_TRUNC('month', gp.payment_date)) OVER (PARTITION BY gp.user_id) AS first_payment_month,
        LAG(gp.revenue_amount_usd) OVER (PARTITION BY gp.user_id ORDER BY DATE_TRUNC('month', gp.payment_date)) AS pre_revenue,
        LEAD(DATE_TRUNC('month', gp.payment_date)) OVER (PARTITION BY gp.user_id ORDER BY DATE_TRUNC('month', gp.payment_date)) AS next_payment, -- Визначення наступного платежу
        LEAD(DATE_TRUNC('month', gp.payment_date)) OVER (PARTITION BY gp.user_id ORDER BY DATE_TRUNC('month', gp.payment_date)) IS NULL AS is_churned -- Маркер користувача, що відтік
    FROM 
        project.games_payments AS gp
    LEFT JOIN 
        project.games_paid_users AS gpu
    ON 
        gp.user_id = gpu.user_id
)
SELECT 
    mc.month_start,
    mc.age,
    mc."language",
    SUM(mc.revenue_amount_usd) AS MRR_$,                 -- Загальний дохід за місяць
    COUNT(DISTINCT mc.user_id) AS Paid_users,            -- Кількість платних користувачів
    ROUND(SUM(mc.revenue_amount_usd) / NULLIF(COUNT(DISTINCT mc.user_id), 0)::numeric, 2) AS ARPPU_$, -- Середній дохід на одного користувача (ARPPU)
   -- Нові платні користувачі
    COUNT(DISTINCT CASE 
        WHEN mc.first_payment_month = mc.month_start THEN mc.user_id 
        ELSE NULL END) AS new_paid_users,
    -- Новий MRR (від користувачів, що стали платними у відповідний місяць)
    SUM(CASE 
        WHEN mc.first_payment_month = mc.month_start THEN mc.revenue_amount_usd 
        ELSE 0 END) AS New_MRR_$,
    -- Expansion MRR (приріст MRR у користувачів, що сплатили більше в поточному місяці)
    SUM(CASE 
        WHEN mc.pre_revenue IS NOT NULL AND mc.revenue_amount_usd > mc.pre_revenue 
        THEN mc.revenue_amount_usd - mc.pre_revenue 
        ELSE 0 END) AS Expansion_MRR_$,
    -- Contraction MRR (зменьшення MRR у користувачів, що сплатили меньше в поточному місяці)
    SUM(CASE 
        WHEN mc.pre_revenue IS NOT NULL AND mc.revenue_amount_usd < mc.pre_revenue 
        THEN mc.revenue_amount_usd - mc.pre_revenue   
        ELSE 0 END) AS Contraction_MRR_$,
    -- Churned Users (зсув на один місяць вперед для визначення відтоку)
    LAG(COUNT(DISTINCT CASE 
        WHEN mc.is_churned = TRUE 
             AND mc.month_start >= '2022-03-01' -- Початок періоду відліку
        THEN mc.user_id 
        ELSE NULL END), 1) OVER (ORDER BY mc.month_start) AS churned_users, -- Зсув на один місяць
    -- Churn Rate
    ROUND(COUNT(DISTINCT CASE 
            WHEN mc.is_churned = TRUE THEN mc.user_id 
            ELSE NULL END)::numeric / NULLIF(LAG(COUNT(DISTINCT mc.user_id)) OVER (ORDER BY mc.month_start), 0)::numeric, 4) *100 AS Churn_rate,
    -- Churned Revenue
	SUM(CASE 
        WHEN mc.is_churned = TRUE 
             AND mc.first_payment_month < mc.month_start 
        THEN mc.revenue_amount_usd 
        ELSE 0 END) AS churned_revenue,
    -- Revenue Churn Rate
    ROUND(SUM(CASE 
            WHEN mc.is_churned = TRUE 
                AND mc.month_start = mc.first_payment_month 
            THEN mc.revenue_amount_usd 
            ELSE 0 END) / NULLIF(LAG(SUM(mc.revenue_amount_usd)) OVER (ORDER BY mc.month_start), 0)::numeric, 4) * 100 AS revenue_churn_rate
FROM 
    my_CTE AS mc
GROUP BY 
    mc.month_start,
    mc.age,
    mc."language"
ORDER BY 
    mc.month_start;