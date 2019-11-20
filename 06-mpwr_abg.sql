CREATE temp TABLE mpwr_abg as
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
    777, 225698 -- arterial co2
  , 778, 220235 -- arterial paco2
  , 779, 220224 -- arterial pao2
  , 780, 223830 -- arterial ph
);