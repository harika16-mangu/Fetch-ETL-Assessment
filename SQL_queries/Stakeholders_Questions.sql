--Question 1: What are the top 5 brands by receipts scanned for most recent month?
--Query:
with brands as (
select distinct b.brandcode,b.name,b.cpg_id from analytics.public.brands b
), 
receipt as (
select distinct r.receipt_id,i.rewardsproductpartnerid,r.datescanned from analytics.public.receipts r join analytics.public.receipt_items i on r.receipt_id=i.receipt_id
where MONTH(r.datescanned)=3
),
top_brands as(
select distinct c.name,count(d.receipt_id) as total_receipts,dense_rank() over(order by count(d.receipt_id)desc) as brand_rankings from brands as c join receipt as d on c.cpg_id=d.rewardsproductpartnerid
group by c.name
)
select e.name,e.total_receipts,brand_rankings from top_brands as e
where e.brand_rankings<6;

--Question 2: When considering average spend from receipts with 'rewardsReceiptStatus’ of ‘Accepted’ or ‘Rejected’, which is greater?
--Query:
with receipt1 as(
select round(avg(totalspent),2) as average,rewardsreceiptstatus from analytics.public.receipts
group by rewardsreceiptstatus
having rewardsreceiptstatus='FINISHED'
),
receipt2 as(
select round(avg(totalspent),2)as average,rewardsreceiptstatus from analytics.public.receipts
group by rewardsreceiptstatus
having rewardsreceiptstatus='REJECTED'
)
select rewardsreceiptstatus,average from receipt1
union all
select rewardsreceiptstatus,average from receipt2

--Question 3: When considering total number of items purchased from receipts with 'rewardsReceiptStatus’ of ‘Accepted’ or ‘Rejected’, which is greater?
--Query:
select count(case when rewardsreceiptstatus='FINISHED' THEN PURCHASEDITEMCOUNT END) AS accepted_items,
count(case when rewardsreceiptstatus='REJECTED' THEN PURCHASEDITEMCOUNT END) as rejected_items
from receipts r

--Question 4: Which brand has the most spend among users who were created within the past 6 months?
--Query:
select distinct b.name,t.total_amount_spent from (select u.user_id,sum(r.totalspent) as total_amount_spent from receipts r join users u
on r.userid=u.user_id
where month(createdate) between 1 and 6
group by u.user_id
)as t inner join brands b
order by t.total_amount_spent desc
limit 5;

--Which brand has the most transactions among users who were created within the past 6 months?
--Query:
with user1 as (select distinct u.user_id,sum(r.totalspent)as total_Spent,count(distinct r.receipt_id) as total_receipts from receipts r join users u on r.userid=u.user_id
where month(r.createdate) between 1 and 6
group by u.user_id
),
brand1 as(select distinct b.name from brands b)
select h.name,r.total_receipts,r.total_Spent
from user1 r join brand1 h
order by r.total_receipts desc
limit 5;
