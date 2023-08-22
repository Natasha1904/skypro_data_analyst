-- Задание 1

with first_payments as --шаг 1
(select user_id, date_trunc('day',transaction_datetime) as first_payment_date
from (select *
    ,  row_number() over (partition by user_id order by transaction_datetime) as rn_transactions
from skyeng_db.payments
where status_name='success') tmp
where rn_transactions=1),
all_dates as --шаг 2
(select distinct date_trunc('day',class_start_datetime) as dt
from skyeng_db.classes
where date_trunc('year', class_start_datetime)='2016-01-01'),
all_dates_by_user as --шаг 3
(select user_id, dt
from first_payments a
    join all_dates b
        on b.dt>=a.first_payment_date),
payments_by_dates as --шаг 4 
(select user_id, date_trunc('day',transaction_datetime) as payment_day
    , sum(classes) as transaction_balance_change
from skyeng_db.payments
where status_name='success'
group by user_id, date_trunc('day',transaction_datetime)),
payments_by_dates_cumsum as --шаг 5 
(select a.user_id,dt,transaction_balance_change
      , sum(transaction_balance_change) over (partition by a.user_id order by dt) as transaction_balance_change_cs
from all_dates_by_user a
    left join payments_by_dates b
        on a.user_id=b.user_id
        and a.dt=b.payment_day),
classes_by_dates as --шаг 6 
(select user_id
     , date_trunc('day',class_start_datetime) as classes_lose
     , (count(*))*(-1) as classes
from skyeng_db.classes
where (class_status='success' or class_status='failed_by_student') and class_type!='trial'
group by user_id, date_trunc('day',class_start_datetime)),
classes_by_dates_dates_cumsum as --шаг 7
(select a.user_id,dt,classes
     , sum(coalesce(classes,0)) over (partition by a.user_id order by dt) as classes_cs
     , coalesce(classes,0) as classes_1
from all_dates_by_user a
    left join classes_by_dates b
    on a.user_id=b.user_id
    and a.dt=b.classes_lose),
balances as --шаг 8 
(select a.user_id, a.dt, transaction_balance_change, transaction_balance_change_cs
    , classes_1, classes_cs, (classes_cs + transaction_balance_change_cs) as balance
from payments_by_dates_cumsum a
    join classes_by_dates_dates_cumsum b
        on a.user_id=b.user_id
        and a.dt=b.dt)
select * from balances
order by user_id, dt
limit 1000


-- Задание 2

with first_payments as --шаг 1
(select user_id, date_trunc('day',transaction_datetime) as first_payment_date
from (select *
    ,  row_number() over (partition by user_id order by transaction_datetime) as rn_transactions
from skyeng_db.payments
where status_name='success') tmp
where rn_transactions=1),
all_dates as --шаг 2
(select distinct date_trunc('day',class_start_datetime) as dt
from skyeng_db.classes
where date_trunc('year', class_start_datetime)='2016-01-01'),
all_dates_by_user as --шаг 3
(select user_id, dt
from first_payments a
    join all_dates b
        on b.dt>=a.first_payment_date),
payments_by_dates as --шаг 4 
(select user_id, date_trunc('day',transaction_datetime) as payment_day
    , sum(classes) as transaction_balance_change
from skyeng_db.payments
where status_name='success'
group by user_id, date_trunc('day',transaction_datetime)),
payments_by_dates_cumsum as --шаг 5 
(select a.user_id,dt,transaction_balance_change
      , sum(transaction_balance_change) over (partition by a.user_id order by dt) as transaction_balance_change_cs
from all_dates_by_user a
    left join payments_by_dates b
        on a.user_id=b.user_id
        and a.dt=b.payment_day),
classes_by_dates as --шаг 6 
(select user_id
     , date_trunc('day',class_start_datetime) as classes_lose
     , (count(*))*(-1) as classes
from skyeng_db.classes
where (class_status='success' or class_status='failed_by_student') and class_type!='trial'
group by user_id, date_trunc('day',class_start_datetime)),
classes_by_dates_dates_cumsum as --шаг 7
(select a.user_id,dt,classes
     , sum(coalesce(classes,0)) over (partition by a.user_id order by dt) as classes_cs
     , coalesce(classes,0) as classes_1
from all_dates_by_user a
    left join classes_by_dates b
    on a.user_id=b.user_id
    and a.dt=b.classes_lose),
balances as --шаг 8 
(select a.user_id, a.dt, transaction_balance_change, transaction_balance_change_cs
    , classes_1, classes_cs, (classes_cs + transaction_balance_change_cs) as balance
from payments_by_dates_cumsum a
    join classes_by_dates_dates_cumsum b
        on a.user_id=b.user_id
        and a.dt=b.dt)
select 
     dt,
     sum(transaction_balance_change) as sum_transaction_balance_change,
     sum(transaction_balance_change_cs) as transaction_balance_change_cs,
     sum(classes_1) as sum_classes_1 ,
     sum(classes_cs) as sum_classes_cs,
     sum(balance) as sum_balance
from balances
group by  dt
order by  dt
