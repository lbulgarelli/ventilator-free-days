create temp table mpwr_blocks_mp as
with vent_mode as
(
  select
      icustay_id
    , block_order
    , vent_mode
    , rank() over(
        partition by icustay_id, block_order, vent_mode
        order by count(*) desc, max(charttime) desc
      ) as vent_mode_seq
  from mpwr_blocks
  where vent_mode is not null
  group by icustay_id, block_order, vent_mode
)
, blocks_mp as
(
  select
      icustay_id
    , block_order
    -- ventilation settings minimum for each block
    , min(tidal_volume) as min_tidal_volume
    , min(plateau_pressure) as min_plateau_pressure
    , min(peep) as min_peep
    , min(peak_insp_pressure) as min_peak_insp_pressure
    , min(resp_rate_set) as min_resp_rate_set
    , min(resp_rate_total) as min_resp_rate_total
    -- ventilation settings maximum for each block
    , max(tidal_volume) as max_tidal_volume
    , max(plateau_pressure) as max_plateau_pressure
    , max(peep) as max_peep
    , max(peak_insp_pressure) as max_peak_insp_pressure
    , max(resp_rate_set) as max_resp_rate_set
    , max(resp_rate_total) as max_resp_rate_total
  from mpwr_blocks
  group by icustay_id, block_order
)
select
    b.*
  -- ventilation mode
  , vm.vent_mode as mode_vent_mode
  -- mechanical power from min values
  , ( 
      0.098 * b.min_resp_rate_total * ( b.min_tidal_volume / 1000 ) *
      ( b.min_peak_insp_pressure - ( b.min_plateau_pressure - b.min_peep ) / 2 )
    )
    as min_mechanical_power
  -- mechanical power from max values
  , ( 
      0.098 * b.max_resp_rate_total * ( b.max_tidal_volume / 1000 ) *
      ( b.max_peak_insp_pressure - ( b.max_plateau_pressure - b.max_peep ) / 2 )
    )
    as max_mechanical_power
from blocks_mp b
left join vent_mode vm
  on b.icustay_id = vm.icustay_id
  and b.block_order = vm.block_order
order by icustay_id, block_order ASC;