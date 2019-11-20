create temp table mpwr_cohort as
with icu as
(
  SELECT 
      ie.subject_id
    , ie.hadm_id
    , ie.icustay_id

    -- patient level factors
    , pat.gender

    -- hospital level factors
    , adm.admittime
    , adm.dischtime
    , ROUND( datetime_diff(adm.dischtime,adm.admittime,MINUTE)/60/24 , 4) AS los_hospital
    , ROUND( datetime_diff(adm.admittime,pat.dob,MINUTE)/60/24/365.242, 4) AS age
    , adm.ethnicity
    , adm.ADMISSION_TYPE
    , adm.hospital_expire_flag
    , DENSE_RANK() OVER (PARTITION BY adm.subject_id ORDER BY adm.admittime) AS hospstay_seq
    , CASE
        WHEN DENSE_RANK() OVER (PARTITION BY adm.subject_id ORDER BY adm.admittime) = 1 THEN 'Y'
        ELSE 'N'
      END AS first_hosp_stay
    , adm.has_chartevents_data

    -- icu level factors
    , ie.intime
    , ie.outtime
    , ROUND( datetime_diff(ie.outtime,ie.intime,MINUTE)/60/24 , 4) AS los_icu
    , DENSE_RANK() OVER (PARTITION BY ie.hadm_id ORDER BY ie.intime) AS icustay_seq

    -- first ICU stay *for the current hospitalization*
    , CASE
        WHEN DENSE_RANK() OVER (PARTITION BY ie.hadm_id ORDER BY ie.intime) = 1 THEN 'Y'
        ELSE 'N' 
      END AS first_icu_stay

  FROM `physionet-data.mimiciii_clinical.icustays` ie
  INNER JOIN `physionet-data.mimiciii_clinical.admissions` adm
      ON ie.hadm_id = adm.hadm_id
  INNER JOIN `physionet-data.mimiciii_clinical.patients` pat
      ON ie.subject_id = pat.subject_id
)
, ventilated AS
(
  select 
      icu.subject_id
    , icu.hadm_id
    , icu.icustay_id
    , icu.intime
    , icu.outtime
    , icu.gender
    , icu.los_hospital
    , icu.age
    , icu.hospstay_seq
    , icu.los_icu
    , icu.icustay_seq
    , vent.ventnum
    , vent.starttime
    , vent.endtime
    , vent.duration_hours/24.0 as duration

    -- exclusions
    , case when icu.age < 16 then 1 else 0 end as exclusion_nonadult
    , case when icu.hospstay_seq>1 or icu.icustay_seq>1 then 1 else 0 end as exclusion_readmission
    , case when tr.trach = 1 then 1 else 0 end as exclusion_trach
    , case when vent.icustay_id is null then 1 else 0 end as exclusion_not_vent
    , case when v.icustay_id is null then 1 else 0 end as exclusion_not_vent_48hr
    , case when has_chartevents_data = 0 then 1 else 0 end as exclusion_bad_data
  from icu
  left join `physionet-data.mimiciii_derived.ventdurations` vent
    on vent.icustay_id = icu.icustay_id
    and vent.ventnum = 1 -- first ventilation and age >= 16
  left join `physionet-data.mimiciii_derived.ventdurations` v
    on v.icustay_id = icu.icustay_id
    and v.ventnum = 1 -- first ventilation and age >= 16
    and v.duration_hours >= 48 -- mv duration >48h
  left join mpwr_trach tr
    on icu.icustay_id = tr.icustay_id
)
select
    subject_id
  , hadm_id
  , icustay_id
  , intime
  , outtime
  , gender
  , los_hospital
  , age
  , hospstay_seq
  , los_icu
  , icustay_seq
  , ventnum
  , starttime
  , endtime
  , duration
from ventilated
where exclusion_nonadult = 0
and exclusion_readmission = 0
and exclusion_trach = 0
and exclusion_not_vent_48hr = 0
and exclusion_bad_data = 0
;