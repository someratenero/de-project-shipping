--shipping_country_rates
DROP TABLE IF EXISTS public.shipping_country_rates CASCADE;

CREATE TABLE public.shipping_country_rates(
   id serial ,
   shipping_country text,
   shipping_country_base_rate numeric(14,3),
   PRIMARY KEY  (id)
);

CREATE INDEX shipping_country_rates_id ON public.shipping_country_rates(shipping_country);

INSERT INTO public.shipping_country_rates (shipping_country, shipping_country_base_rate)
SELECT DISTINCT shipping_country, shipping_country_base_rate from public.shipping;

--shipping_agreement

DROP TABLE IF EXISTS public.shipping_agreement CASCADE;

CREATE TABLE public.shipping_agreement(
agreementid bigint, 
agreement_number text,
agreement_rate  numeric(14,3) ,
agreement_commission numeric(14,3),
PRIMARY KEY (agreementid)
);

CREATE INDEX shipping_agremeent_id on public.shipping_agreement(agreementid);

INSERT INTO public.shipping_agreement (agreementid, agreement_number, agreement_rate, agreement_commission)
SELECT 
DISTINCT vendor_agreement_description[1]::bigint as agreementid,
vendor_agreement_description[2]::text as agreement_number,
vendor_agreement_description[3]::numeric(14,3) as agreement_rate,
vendor_agreement_description[4]::numeric(14,3) as agreement_commission
FROM
(SELECT REGEXP_SPLIT_TO_ARRAY(vendor_agreement_description, e'\\:+') as vendor_agreement_description FROM public.shipping s) t

--shipping_transfer
DROP TABLE IF EXISTS public.shipping_transfer CASCADE;
CREATE TABLE public.shipping_transfer(
id serial,
transfer_type text,
transfer_model text,
shipping_transfer_rate numeric (14,3),
PRIMARY KEY (id)
);

CREATE INDEX shipping_transfer_id on public.shipping_transfer(transfer_type);

INSERT INTO public.shipping_transfer (transfer_type, transfer_model, shipping_transfer_rate)
SELECT 
DISTINCT shipping_transfer_description[1]::text as transfer_type,
shipping_transfer_description[2]::text as transfer_model,
shipping_transfer_rate
FROM
(SELECT REGEXP_SPLIT_TO_ARRAY(shipping_transfer_description,e'\\:+') as shipping_transfer_description, shipping_transfer_rate FROM public.shipping s) t;

--shipping_info
DROP TABLE IF EXISTS public.shipping_info CASCADE;
CREATE TABLE public.shipping_info (
shippingid bigint,
vendorid int8,
payment_amount numeric (14,3),
shipping_plan_datetime timestamp, 
transfer_type_id bigint,
shipping_country_id bigint,
agreementid bigint,
FOREIGN KEY (transfer_type_id) REFERENCES public.shipping_transfer(id) ON UPDATE CASCADE,
FOREIGN KEY (shipping_country_id) REFERENCES public.shipping_country_rates(id) ON UPDATE CASCADE,
FOREIGN KEY (agreementid) REFERENCES public.shipping_agreement(agreementid) ON UPDATE CASCADE
);

INSERT INTO  public.shipping_info (shippingid, vendorid, payment_amount, shipping_plan_datetime, transfer_type_id, shipping_country_id, agreementid)
SELECT DISTINCT s.shippingid,
s.vendorid, 
s.payment_amount, 
shipping_plan_datetime, 
st.id as transfer_type_id,
scr.id as shipping_country_id,
sa.agreementid
FROM public.shipping s
LEFT JOIN public.shipping_transfer st
ON concat_ws(':', st.transfer_type, st.transfer_model) = s.shipping_transfer_description
LEFT JOIN public.shipping_country_rates scr 
on scr.shipping_country=s.shipping_country
LEFT JOIN public.shipping_agreement sa 
on sa.agreementid = (regexp_split_to_array(s.vendor_agreement_description, e'\\:+'))[1]::bigint;


--shipping_status
DROP TABLE IF EXISTS public.shipping_status;

CREATE TABLE public.shipping_status (
shippingid bigint,
status text,
state text,
shipping_start_fact_datetime timestamp,
shipping_end_fact_datetime timestamp
--foreign key (shippingid) references public.shipping_info(shippingid) on update cascade
);

WITH ship_max as (
  SELECT shippingid,
      max(CASE WHEN state = 'booked' THEN state_datetime ELSE NULL END) as shipping_start_fact_datetime,
      max(CASE WHEN state = 'recieved' THEN state_datetime ELSE NULL END) as shipping_end_fact_datetime,
      max(state_datetime) as max_state_datetime
  FROM shipping
  GROUP BY shippingid
)
INSERT INTO public.shipping_status
(shippingid, status,state,shipping_start_fact_datetime,shipping_end_fact_datetime)
SELECT sm.shippingid,
s.status,
s.state,
sm.shipping_start_fact_datetime,
sm.shipping_end_fact_datetime
FROM ship_max as sm
LEFT JOIN shipping as s on sm.shippingid = s.shippingid
            and sm.max_state_datetime = s.state_datetime
ORDER BY shippingid;

--shipping_datamart
DROP TABLE IF EXISTS public.shipping_datamart;

CREATE TABLE public.shipping_datamart (
shippingid bigint,
vendorid bigint,
transfer_type text,
full_day_at_shipping int8,
is_delay int8,
is_shipping_finish int8,
delay_day_at_shipping int8,
payment_amount numeric(14,3),
vat numeric(14,3), 
profit numeric(14,3)
);

INSERT INTO public.shipping_datamart 
(shippingid, vendorid, transfer_type, full_day_at_shipping, is_delay, is_shipping_finish, delay_day_at_shipping, payment_amount, vat, profit)
SELECT DISTINCT si.shippingid,
si.vendorid,
st.transfer_type,
DATE_PART('day', AGE(shipping_end_fact_datetime,shipping_start_fact_datetime)) as full_day_at_shipping,
CASE WHEN shipping_end_fact_datetime > shipping_plan_datetime THEN 1 ELSE 0 END AS is_delay,
CASE WHEN status = 'finished' THEN 1 ELSE 0 END AS is_shipping_finish,
CASE WHEN shipping_end_fact_datetime > shipping_plan_datetime 
THEN DATE_PART('day', AGE(shipping_end_fact_datetime,shipping_plan_datetime)) ELSE 0 END AS delay_day_at_shipping,
si.payment_amount,
si.payment_amount * (shipping_country_base_rate + agreement_rate + shipping_transfer_rate) as vat, 
payment_amount * agreement_commission as profit
FROM shipping_info as si
LEFT JOIN shipping_transfer as st 
on st.id=si.shipping_country_id
LEFT JOIN shipping_status as ss 
on ss.shippingid = si.shippingid
LEFT JOIN shipping_country_rates scr 
on scr.id=si.shipping_country_id 
LEFT JOIN shipping_agreement sa
on sa.agreementid=si.agreementid;
