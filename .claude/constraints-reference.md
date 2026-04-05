# RDLS Constraint Tables Reference

All constraint tables from `to-rdls/configs/rdls_defaults.yaml`. These are the rules that extractors and validators enforce.

## Vulnerability: function_type_constraints

| function_type | allowed_metrics | default_metric | default_qty |
|---|---|---|---|
| vulnerability | damage_ratio, mean_damage_ratio, casualty_ratio_vulnerability, downtime_vulnerability | damage_ratio | ratio |
| fragility | probability, damage_index, damage_ratio, mean_damage_ratio | probability | ratio |
| damage_to_loss | loss_ratio, mean_loss_ratio, economic_loss_value, insured_loss_value, asset_loss, downtime_loss, casualty_count, displaced_count | loss_ratio | ratio |
| engineering_demand | damage_index, damage_ratio, mean_damage_ratio, probability | damage_index | count |

## Vulnerability: category defaults

| category | typical_function | metric_override | qty_override |
|---|---|---|---|
| buildings | fragility | damage_ratio | ratio |
| infrastructure | vulnerability | downtime_vulnerability | time |
| population | vulnerability | casualty_ratio_vulnerability | ratio |
| agriculture | vulnerability | damage_ratio | ratio |
| natural_environment | vulnerability | damage_ratio | ratio |
| economic_indicator | damage_to_loss | economic_loss_value | monetary |
| development_index | vulnerability | damage_index | count |

## Loss: signal type defaults

| signal_type | asset_category | asset_dimension | impact_type | impact_metric | quantity_kind | loss_type |
|---|---|---|---|---|---|---|
| human_loss | population | population | direct | casualty_count | count | count |
| displacement | population | population | direct | displaced_count | count | count |
| affected_population | population | population | direct | exposure_to_hazard | count | count |
| economic_loss | buildings | structure | direct | economic_loss_value | monetary | ground_up |
| structural_damage | buildings | structure | direct | damage_ratio | ratio | ground_up |
| agricultural_loss | agriculture | product | direct | asset_loss | monetary | ground_up |
| catastrophe_model | buildings | structure | total | loss_annual_average_value | monetary | ground_up |
| general_loss | population | population | direct | asset_loss | count | ground_up |

## Loss: valid asset dimensions per category

| category | valid dimensions |
|---|---|
| agriculture | product, structure, content |
| buildings | structure, content |
| infrastructure | structure, disruption |
| population | population |
| natural_environment | structure, index |
| economic_indicator | product, index |
| development_index | index |

## Loss: type → allowed approaches

| loss_type | allowed approaches |
|---|---|
| net_precat | analytical |
| net_postcat | analytical |
| insured | analytical, empirical, hybrid |
| gross | analytical, empirical, hybrid |
| ground_up | analytical, empirical, hybrid, judgement |
| count | analytical, empirical, hybrid, judgement |

## Impact metric constraints

| metric | quantity_kind | allowed_impact_types |
|---|---|---|
| damage_ratio | ratio | direct |
| mean_damage_ratio | ratio | direct |
| damage_index | count | direct |
| loss_ratio | ratio | direct, indirect, total |
| mean_loss_ratio | ratio | direct, indirect, total |
| probability | ratio | direct, indirect, total |
| downtime_vulnerability | time | indirect |
| casualty_ratio_vulnerability | ratio | direct |
| economic_loss_value | monetary | direct, indirect, total |
| insured_loss_value | monetary | direct, indirect, total |
| loss_annual_average_value | monetary | total |
| loss_probable_maximum_value | monetary | total |
| at_risk_value | monetary | total |
| at_risk_tail_value | monetary | total |
| asset_loss | monetary | direct |
| downtime_loss | time | indirect |
| casualty_count | count | direct |
| casualty_ratio_loss | ratio | direct |
| displaced_count | count | direct |
| exposure_to_hazard | count | direct |

## Component dependencies

```
require_he_for_vl: false
vulnerability: requires_one_of [hazard, exposure], auto_add: exposure
loss: requires_one_of [hazard, exposure], auto_add: exposure
```

## Compound tags

| tag | primary | secondary | rule |
|---|---|---|---|
| earthquake-tsunami | earthquake | tsunami | corroborate_secondary |
| cyclones-hurricanes-typhoons | strong_wind | coastal_flood | corroborate_secondary |
| flood-landslide | flood | landslide | corroborate_secondary |
| drought-food-insecurity | drought | (none) | single |

## Confidence weights (composite scoring)

| component | weight |
|---|---|
| hazard | 0.3 |
| exposure | 0.3 |
| vulnerability | 0.2 |
| loss | 0.2 |
