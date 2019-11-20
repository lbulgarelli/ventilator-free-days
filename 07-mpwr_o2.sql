CREATE temp TABLE mpwr_o2 as
select
    icustay_id
  , charttime
  , itemid
  , value
  , valuenum
  , valueuom
  , storetime
from `physionet-data.mimiciii_clinical.chartevents` ce
where ce.valuenum is not null
-- exclude rows marked as error
and coalesce(error,0)=0
and itemid in
(
    646, 220277 -- spo2
  , 190, 223835 -- fio2
);