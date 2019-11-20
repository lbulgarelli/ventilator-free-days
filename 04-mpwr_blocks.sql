create temp table mpwr_blocks as
with first_block as -- find the earliest time stamp per patient
(
  select
      co.icustay_id
    , co.starttime
    , min(floor(UNIX_SECONDS(TIMESTAMP(un.charttime))/(6*3600))) as blockn
  from mpwr_vent_unpivot un
  join mpwr_cohort co
    on un.charttime between co.starttime and datetime_add(co.starttime, interval 2 day)
    and un.icustay_id = co.icustay_id
  group by co.icustay_id, co.starttime
  order by co.icustay_id
)
, un_vent as
(
  select
      icustay_id
    , floor(UNIX_SECONDS(TIMESTAMP(charttime))/(6*3600)) as blockn
    , charttime
    , tidal_volume
    , plateau_pressure
    , peep
    , peak_insp_pressure
    , resp_rate_set
    , resp_rate_total
    , vent_mode
  from mpwr_vent_unpivot
)
select
    b1.icustay_id as icustay_id
  , b1.starttime as venttimestart
  , un_vent.blockn
  , un_vent.charttime
  , un_vent.tidal_volume
  , un_vent.plateau_pressure
  , un_vent.peep
  , un_vent.peak_insp_pressure
  , un_vent.resp_rate_set
  , un_vent.resp_rate_total
  , un_vent.vent_mode
  , cast(un_vent.blockn-b1.blockn+1 as int64) as block_order
from first_block b1
inner join un_vent
  on b1.icustay_id = un_vent.icustay_id
  and un_vent.blockn >= b1.blockn and un_vent.blockn <= b1.blockn + 7
order by un_vent.icustay_id, blockn, charttime
;