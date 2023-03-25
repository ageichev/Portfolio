-- Task 2.1

SELECT
m,
countIf(correct>20) quant_active_students
from (SELECT
        toStartOfMonth(timest) m,
        st_id,
        sum(correct) correct
        from default.peas
        group by m,st_id     
)
group by m;


-- Task 2.2

WITH
groups as (
    SELECT
    * 
    from default.studs
),

checks as (
    select 
    * 
    from default.final_project_check
),
tasks as (
    select 
    * 
    from default.peas
),
-- Делаем отдельно датасетс фильтрами по предмету изучения математике
math as 
(
    SELECT
    groups.st_id st_id,
    test_grp,
    sum(money) revenue,
    sum(correct) tasks,
    arrayStringConcat(groupArray(checks.subject), ',') bought_subject
    from groups
    left join checks using (st_id)
    left join tasks  on groups.st_id=tasks.st_id
    where tasks.subject = 'Math'
    group by groups.st_id, test_grp
    
)

-- Получаем финальные метрики
SELECT

l.test_grp,
sum(l.revenue)/count(l.st_id) ARPU,
avg(if(l.tasks>10,l.revenue,null)) ARPAU,
count(if(l.revenue>0,st_id,null))/count(l.st_id) CR_to_purchase,
countIf(math.tasks>=2 and bought_subject like '%Math%' )/countIf(math.tasks>=2) CR_to_purchase_math

from(
    SELECT
    groups.st_id st_id,
    test_grp,
    sum(money) revenue,
    sum(correct) tasks
    from groups 
    left join checks using (st_id)
    left join tasks  on groups.st_id=tasks.st_id
    group by groups.st_id, test_grp
 ) as l
left join math using (st_id)
group by test_grp
limit 100
    


    

