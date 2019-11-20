create temp table mpwr_abg_unpivot as
with abg AS (
  select
      abg.icustay_id
    , RANK() OVER(PARTITION BY abg.icustay_id ORDER BY abg.charttime) AS abg_seq
    , abg.charttime AS charttime
    , MAX(CASE
        WHEN abg.itemid IN (777, 225698) THEN abg.valuenum
        ELSE NULL
      END) AS arterial_co2
    , MAX(CASE
        WHEN abg.itemid IN (778, 220235) THEN abg.valuenum
        ELSE NULL
      END) AS arterial_paco2
    , MAX(CASE
        WHEN abg.itemid IN (779, 220224) THEN abg.valuenum
        ELSE NULL
      END) AS arterial_pao2
    , MAX(CASE
        WHEN abg.itemid IN (780, 223830) THEN abg.valuenum
        ELSE NULL
      END) AS arterial_ph
  from mpwr_cohort ch
  inner join mpwr_abg abg
    on ch.icustay_id = abg.icustay_id
    and abg.charttime between ch.starttime and ch.endtime
  group by abg.icustay_id, abg.charttime
)
, spo2 AS
(
  select
      abg.icustay_id
    , abg.abg_seq
    , mpwr_o2.valuenum AS spo2
    , datetime_diff(mpwr_o2.charttime, abg.charttime, MINUTE) AS spo2_offset
    , RANK() OVER(
        PARTITION BY abg.icustay_id, abg.charttime
        ORDER BY datetime_diff(mpwr_o2.charttime, abg.charttime, MINUTE) DESC
      ) AS spo2_seq
  from abg
  inner join mpwr_o2
    on abg.icustay_id = mpwr_o2.icustay_id
    and mpwr_o2.charttime between datetime_sub(abg.charttime, INTERVAL 6 HOUR) and abg.charttime
    and mpwr_o2.itemid IN (646, 220277)
)
, fio2 AS
(
  select
      abg.icustay_id
    , abg.abg_seq
    , CASE
        WHEN mpwr_o2.itemid = 190 THEN CAST(mpwr_o2.valuenum*100 AS INT64)
        ELSE mpwr_o2.valuenum
      END AS fio2
    , datetime_diff(mpwr_o2.charttime, abg.charttime, MINUTE) AS fio2_offset
    , RANK() OVER(
        PARTITION BY abg.icustay_id, abg.charttime
        ORDER BY datetime_diff(mpwr_o2.charttime, abg.charttime, MINUTE) DESC
      ) AS fio2_seq 
  from abg
  inner join mpwr_o2
    on abg.icustay_id = mpwr_o2.icustay_id
    and mpwr_o2.charttime between datetime_sub(abg.charttime, INTERVAL 6 HOUR) and abg.charttime
    and mpwr_o2.itemid IN (190, 223835)
)
select
    abg.icustay_id
  , abg.charttime
  , abg.arterial_co2
  , abg.arterial_paco2
  , abg.arterial_pao2
  , abg.arterial_ph
  , spo2.spo2_offset
  , spo2.spo2
  , fio2.fio2_offset
  , fio2.fio2
from abg
left join spo2
  on abg.icustay_id = spo2.icustay_id
  and abg.abg_seq = spo2.abg_seq
  and spo2.spo2_seq = 1
left join fio2
  on abg.icustay_id = fio2.icustay_id
  and abg.abg_seq = fio2.abg_seq
  and fio2.fio2_seq = 1
order by abg.icustay_id, abg.charttime
;