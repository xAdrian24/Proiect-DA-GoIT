-- Proiect 2_final_version
-- Crearea unui CTE (Common Table Expression) pentru a calcula venitul lunar pe utilizator și joc
with monthly_revenue as (
    select
        -- Extrage doar data lunară din payment_date
        date(date_trunc('month', payment_date)) as payment_month,
        user_id,
        game_name,
        -- Calculează suma veniturilor pentru fiecare lună, utilizator și joc
        sum(revenue_amount_usd) as total_revenue
    from project.games_payments gp 
    group by 1, 2, 3
),
-- Crearea unui CTE pentru a adăuga coloane de tip lag și lead pentru veniturile anterioare și următoare
revenue_lag_lead_months as (
    select
        *,
        -- Calculează luna calendaristică anterioară și următoare
        date(payment_month - interval '1' month) as previous_calendar_month,
        date(payment_month + interval '1' month) as next_calendar_month,
        -- Obține venitul total al lunii anterioare folosind funcția lag
        lag(total_revenue) over(partition by user_id order by payment_month) as previous_paid_month_revenue,
        -- Obține luna plătită anterioară și următoare folosind funcțiile lag și lead
        lag(payment_month) over(partition by user_id order by payment_month) as previous_paid_month,
        lead(payment_month) over(partition by user_id order by payment_month) as next_paid_month
    from monthly_revenue
),
-- Crearea unui CTE pentru a calcula diverse metrice de venit
revenue_metrics as (
    select
        payment_month,
        user_id,
        game_name,
        total_revenue,
        -- Calcul pentru venituri noi (new MRR)
        case 
            when previous_paid_month is null 
                then total_revenue
        end as new_mrr,
        -- Calcul pentru venituri de extindere (expansion revenue)
        case 
            when previous_paid_month = previous_calendar_month 
                and total_revenue > previous_paid_month_revenue 
                then total_revenue - previous_paid_month_revenue
        end as expansion_revenue,
        -- Calcul pentru contracția veniturilor (contraction revenue)
        case 
            when previous_paid_month = previous_calendar_month 
                and total_revenue < previous_paid_month_revenue 
                then total_revenue - previous_paid_month_revenue
        end as contraction_revenue,
        -- Calcul pentru veniturile recâștigate de la clienții care au revenit după un churn (back from churn revenue)
        case 
            when previous_paid_month != previous_calendar_month 
                and previous_paid_month is not null
                then total_revenue
        end as back_from_churn_revenue,
        -- Calcul pentru veniturile pierdute din cauza churn-ului (churned revenue)
        case 
            when next_paid_month is null 
            or next_paid_month != next_calendar_month
                then total_revenue
        end as churned_revenue,
        -- Calcul pentru luna în care s-a produs churn-ul (churn month)
        case 
            when next_paid_month is null 
            or next_paid_month != next_calendar_month
                then next_calendar_month
        end as churn_month
    from revenue_lag_lead_months
)
-- Select final, adăugând detalii despre utilizator din altă tabelă (games_paid_users)
select
    rm.*, -- Toate coloanele din revenue_metrics
    gpu.language,
    gpu.has_older_device_model,
    gpu.age 
from revenue_metrics rm
left join project.games_paid_users gpu using(user_id);
