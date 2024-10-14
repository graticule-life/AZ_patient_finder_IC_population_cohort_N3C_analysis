
-- Filtering Codesets for IC Conditions

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.78efff87-1ecb-4d4e-8c33-e75bc24e075a"),
    concept_set_members=Input(rid="ri.foundry.main.dataset.e670c5ad-42ca-46a2-ae55-e917e3e161b6")
)
SELECT codeset_id,concept_id, concept_name,case 
                                      when codeset_id = 269366633 then 'solid_organ_transplant_procedure'
                                      when codeset_id = 39995760  then 'immunosuppressive_therapies'
                                      when codeset_id = 88684702 then 'car_t_therapy_procedure'
                                      when codeset_id = 961228276 then 'solid_organ_transplant_diagnosis'
                                      when codeset_id = 222300534 then 'Hematopoietic_stem_cell_px_procedure'
                                      when codeset_id = 330926778 then 'dialysis_procedure'
                                      when codeset_id = 715550969 then 'Hematological_malignencies'
                                      when codeset_id = 797180958 then 'B-Cell_depling_therapy'
                                      when codeset_id = 91206032 then 'End_stage_renal_disease_diagnosis'
                                      when codeset_id = 141311032 then 'graft_vs_host_diagnosis'
                                      when codeset_id = 332337787 then 'Hematopoietic_stem_cell_px_diagnosis'
                                      when codeset_id = 338556896 then 'metastic_solid_tumor_diagnosis'
                                      when codeset_id = 800576185 then 'dialysis_diagnosis'
                                      when codeset_id = 554885567 then 'primery_immonodeficiency_diagnosis'
                                      when codeset_id = 117835086 then 'solid_tumor'
                                      when codeset_id = 226846470 then 'any_malignancy_except_malignant'
                                      when codeset_id = 911760988 then 'cancer_therapies'
                                      else 'Others' end as Indication                 

FROM concept_set_members
where codeset_id in(269366633, 39995760, 88684702, 961228276, 222300534, 330926778, 715550969,797180958,91206032, 141311032, 332337787, 338556896, 800576185, 554885567, 117835086, 226846470,911760988)

-- Finding related diagnosis, medications, procedures, observations & measurements

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.1c4becfd-bd18-4ff9-b56d-7c0ba4e53e04"),
    IC_Concept_ids=Input(rid="ri.foundry.main.dataset.78efff87-1ecb-4d4e-8c33-e75bc24e075a"),
    condition_occurrence=Input(rid="ri.foundry.main.dataset.900fa2ad-87ea-4285-be30-c6b5bab60e86")
)
SELECT condition_occurrence.person_id, IC_Concept_ids.concept_id, IC_Concept_ids.concept_name, IC_Concept_ids.Indication, condition_occurrence.condition_start_date, cast('2024-05-31' as date) as index_date
FROM IC_Concept_ids
inner join condition_occurrence
on IC_Concept_ids.concept_id = condition_occurrence.condition_concept_id

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.8399025d-1ab3-46ee-88fc-5ec4fbb2e95b"),
    IC_Concept_ids=Input(rid="ri.foundry.main.dataset.78efff87-1ecb-4d4e-8c33-e75bc24e075a"),
    drug_exposure=Input(rid="ri.foundry.main.dataset.ec252b05-8f82-4f7f-a227-b3bb9bc578ef")
)
SELECT drug_exposure.person_id, IC_Concept_ids.concept_id, IC_Concept_ids.concept_name, IC_Concept_ids.Indication, drug_exposure.drug_exposure_start_date, cast('2024-05-31' as date) as index_date
FROM IC_Concept_ids
inner join drug_exposure
on IC_Concept_ids.concept_id = drug_exposure.drug_concept_id

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.40a73b24-6c6d-4417-9bee-e54f84ae3d98"),
    IC_Concept_ids=Input(rid="ri.foundry.main.dataset.78efff87-1ecb-4d4e-8c33-e75bc24e075a"),
    measurement=Input(rid="ri.foundry.main.dataset.d6054221-ee0c-4858-97de-22292458fa19")
)
SELECT measurement.person_id, IC_Concept_ids.concept_id, IC_Concept_ids.concept_name, IC_Concept_ids.Indication, measurement.measurement_date, cast('2024-05-31' as date) as index_date
FROM IC_Concept_ids
inner join measurement
on IC_Concept_ids.concept_id = measurement.measurement_concept_id

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.a97d2088-67db-43b2-a0f7-893f35ba38fd"),
    IC_Concept_ids=Input(rid="ri.foundry.main.dataset.78efff87-1ecb-4d4e-8c33-e75bc24e075a"),
    observation=Input(rid="ri.foundry.main.dataset.b998b475-b229-471c-800e-9421491409f3")
)
SELECT observation.person_id, IC_Concept_ids.concept_id, IC_Concept_ids.concept_name, IC_Concept_ids.Indication, observation.observation_date, cast('2024-05-31' as date) as index_date
FROM IC_Concept_ids
inner join observation
on IC_Concept_ids.concept_id = observation.observation_concept_id
-- where observation_date >= '2021-02-02' and observation_date <= '2022-02-02'

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.9ab04f4d-eecb-46eb-acdd-7cceea3d3bfd"),
    IC_Concept_ids=Input(rid="ri.foundry.main.dataset.78efff87-1ecb-4d4e-8c33-e75bc24e075a"),
    procedure_occurrence=Input(rid="ri.foundry.main.dataset.f6f0b5e0-a105-403a-a98f-0ee1c78137dc")
)
SELECT procedure_occurrence.person_id, IC_Concept_ids.concept_id, IC_Concept_ids.concept_name, IC_Concept_ids.Indication, procedure_occurrence.procedure_date, cast('2024-05-31' as date) as index_date
from IC_Concept_ids
inner join procedure_occurrence
on IC_Concept_ids.concept_id = procedure_occurrence.procedure_concept_id

-- 1. Solid tumors or hematological malignancies on treatment

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.34e7a927-51fc-4552-a93e-1ac01a402277"),
    IC_diagnosis=Input(rid="ri.foundry.main.dataset.1c4becfd-bd18-4ff9-b56d-7c0ba4e53e04"),
    IC_drug_exposure=Input(rid="ri.foundry.main.dataset.8399025d-1ab3-46ee-88fc-5ec4fbb2e95b")
)
select * from (
    select *, datediff(condition_start_date,index_date) as diff from (
        select person_id, condition_start_date, index_date
        FROM IC_diagnosis
        where Indication in ('any_malignancy_except_malignant','metastic_solid_tumor_diagnosis')))
where diff >= -1460 and diff <= 0

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.39c39d40-4924-4a2b-8e53-f78919c2de7f"),
    IC_drug_exposure=Input(rid="ri.foundry.main.dataset.8399025d-1ab3-46ee-88fc-5ec4fbb2e95b")
)
select * from (
    select *, datediff(drug_exposure_start_date,index_date) as diff from (
        select person_id, drug_exposure_start_date, index_date
        FROM IC_drug_exposure
        where Indication = 'cancer_therapies'))
where diff >= -1460 and diff <= 0

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.376f5055-35f8-47b5-91e6-ac5b3e374cc5"),
    Solid_tumors_or_hematological_malignancies=Input(rid="ri.foundry.main.dataset.34e7a927-51fc-4552-a93e-1ac01a402277"),
    cancer_therapies=Input(rid="ri.foundry.main.dataset.39c39d40-4924-4a2b-8e53-f78919c2de7f")
)
select distinct Solid_tumors_or_hematological_malignancies.person_id, Solid_tumors_or_hematological_malignancies.index_date
from Solid_tumors_or_hematological_malignancies
inner join cancer_therapies
on Solid_tumors_or_hematological_malignancies.person_id = cancer_therapies.person_id

-- 2. Solid tumor and hematological malignancies not on treatment

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.34e7a927-51fc-4552-a93e-1ac01a402277"),
    IC_diagnosis=Input(rid="ri.foundry.main.dataset.1c4becfd-bd18-4ff9-b56d-7c0ba4e53e04"),
    IC_drug_exposure=Input(rid="ri.foundry.main.dataset.8399025d-1ab3-46ee-88fc-5ec4fbb2e95b")
)
select * from (
    select *, datediff(condition_start_date,index_date) as diff from (
        select person_id, condition_start_date, index_date
        FROM IC_diagnosis
        where Indication in ('any_malignancy_except_malignant','metastic_solid_tumor_diagnosis')))
where diff >= -1460 and diff <= 0

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.39c39d40-4924-4a2b-8e53-f78919c2de7f"),
    IC_drug_exposure=Input(rid="ri.foundry.main.dataset.8399025d-1ab3-46ee-88fc-5ec4fbb2e95b")
)
select * from (
    select *, datediff(drug_exposure_start_date,index_date) as diff from (
        select person_id, drug_exposure_start_date, index_date
        FROM IC_drug_exposure
        where Indication = 'cancer_therapies'))
where diff >= -1460 and diff <= 0

-- (Used python code to derive patients not on treatment as SQL code was not working )

-- 3. Receipt of solid organ or islet transplant

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.fe3e2cf4-128c-4d0f-a2ba-d490b39b6e14"),
    IC_diagnosis=Input(rid="ri.foundry.main.dataset.1c4becfd-bd18-4ff9-b56d-7c0ba4e53e04"),
    IC_measurement=Input(rid="ri.foundry.main.dataset.40a73b24-6c6d-4417-9bee-e54f84ae3d98"),
    IC_observation=Input(rid="ri.foundry.main.dataset.a97d2088-67db-43b2-a0f7-893f35ba38fd"),
    IC_procedures=Input(rid="ri.foundry.main.dataset.9ab04f4d-eecb-46eb-acdd-7cceea3d3bfd")
)
select * from (
    select *, datediff(organ_transplant_date,index_date) as diff from (
            SELECT person_id, Indication,procedure_date as organ_transplant_date,index_date
            FROM IC_procedures
            where Indication in ('solid_organ_transplant_diagnosis','solid_organ_transplant_procedure')
            union all
            SELECT person_id, Indication, condition_start_date as organ_transplant_date,index_date
            FROM IC_diagnosis
            where Indication = 'solid_organ_transplant_diagnosis'
            union all
            SELECT person_id, Indication, observation_date as organ_transplant_date,index_date
            FROM IC_observation
            where Indication in ('solid_organ_transplant_diagnosis','solid_organ_transplant_procedure')))
where diff >= -1460 and diff <= 0

-- 5. Receipt of chimeric antigen receptor T-cell (CAR-T) or hematopoietic stem cell transplant (HSCT)

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.4dc8b5fa-6a84-41e7-a896-2d021a446e8e"),
    IC_drug_exposure=Input(rid="ri.foundry.main.dataset.8399025d-1ab3-46ee-88fc-5ec4fbb2e95b"),
    IC_measurement=Input(rid="ri.foundry.main.dataset.40a73b24-6c6d-4417-9bee-e54f84ae3d98"),
    IC_observation=Input(rid="ri.foundry.main.dataset.a97d2088-67db-43b2-a0f7-893f35ba38fd"),
    IC_procedures=Input(rid="ri.foundry.main.dataset.9ab04f4d-eecb-46eb-acdd-7cceea3d3bfd")
)
select * from (
    select *, datediff(car_t_or_hsct_date,index_date) as diff from (
    SELECT person_id, Indication, drug_exposure_start_date as car_t_or_hsct_date, index_date
    FROM IC_drug_exposure
    where Indication in ('car_t_therapy_procedure')
    union
    SELECT person_id, Indication, procedure_date as car_t_or_hsct_date, index_date
    FROM IC_procedures
    where Indication in ('car_t_therapy_procedure','Hematopoietic_stem_cell_px_procedure')
    union
    SELECT person_id, Indication, observation_date as car_t_or_hsct_date, index_date
    FROM IC_observation
    where Indication in ('Hematopoietic_stem_cell_px_procedure')
    union
    SELECT person_id, Indication, measurement_date as car_t_or_hsct_date, index_date
    FROM IC_measurement
    where Indication in ('Hematopoietic_stem_cell_px_procedure')))
where diff >= -1460 and diff <= 0

-- 6. Moderate or severe primary immunodeficiency

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.c2c5f417-1b34-4728-9565-cce6eb3f7918"),
    IC_diagnosis=Input(rid="ri.foundry.main.dataset.1c4becfd-bd18-4ff9-b56d-7c0ba4e53e04")
)
select * from (
    select *, datediff(condition_start_date,index_date) as diff from (
        select person_id, condition_start_date, index_date
        FROM IC_diagnosis
        where Indication = 'primery_immonodeficiency_diagnosis'))
where diff >= -1460 and diff <= 0

-- 7. Advanced or untreated human immunodeficiency virus (HIV) infection

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.211c3333-f520-4b69-b4b9-02ea0c5da4b9"),
    concept_set_members=Input(rid="ri.foundry.main.dataset.e670c5ad-42ca-46a2-ae55-e917e3e161b6"),
    condition_occurrence=Input(rid="ri.foundry.main.dataset.900fa2ad-87ea-4285-be30-c6b5bab60e86")
)
select * from (
select *, datediff(condition_start_date,index_date) as diff from (
    SELECT condition_occurrence.person_id, concept_set_members.concept_id, concept_set_members.concept_name, condition_occurrence.condition_start_date, cast('2024-05-31' as date) as index_date
    FROM concept_set_members
    inner join condition_occurrence
    on concept_set_members.concept_id = condition_occurrence.condition_concept_id
    where concept_set_members.codeset_id = 426219266))
    where diff >= -1460 and diff <= 0

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.e1ebe956-9d9f-40bb-a433-cf9ea49635f9"),
    HIV_AIDS_Diagnosis=Input(rid="ri.foundry.main.dataset.211c3333-f520-4b69-b4b9-02ea0c5da4b9"),
    measurement=Input(rid="ri.foundry.main.dataset.d6054221-ee0c-4858-97de-22292458fa19")
)
select *, 'aids/hiv' as Indication from (
    select *, datediff(measurement_date,index_date) as diff from (
SELECT HIV_AIDS_Diagnosis.person_id, HIV_AIDS_Diagnosis.index_date, measurement.measurement_concept_id, measurement.value_as_number, measurement.unit_source_value,measurement.measurement_date
FROM HIV_AIDS_Diagnosis
inner join measurement
on HIV_AIDS_Diagnosis.person_id = measurement.person_id
where measurement.measurement_concept_id = 40768447 and (measurement.value_as_number < 200 or measurement.value_as_number = '15%') and (measurement.unit_source_value in ('/mm3','/uL'))))
where diff >= -1460 and diff < 0

-- 8. ESRD or dialysis treatment

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.36811b53-c733-4c17-8192-7686f0108997"),
    IC_diagnosis=Input(rid="ri.foundry.main.dataset.1c4becfd-bd18-4ff9-b56d-7c0ba4e53e04"),
    IC_drug_exposure=Input(rid="ri.foundry.main.dataset.8399025d-1ab3-46ee-88fc-5ec4fbb2e95b"),
    IC_observation=Input(rid="ri.foundry.main.dataset.a97d2088-67db-43b2-a0f7-893f35ba38fd"),
    IC_procedures=Input(rid="ri.foundry.main.dataset.9ab04f4d-eecb-46eb-acdd-7cceea3d3bfd")
)
select distinct person_id from (
    select *, datediff(esrd_or_diaysis_date,index_date) as diff from (
SELECT person_id, Indication, condition_start_date as esrd_or_diaysis_date, index_date
FROM IC_diagnosis
where Indication in ('End_stage_renal_disease_diagnosis','dialysis_diagnosis')
union
SELECT person_id, Indication,procedure_date as esrd_or_diaysis_date, index_date
FROM IC_procedures
where Indication in ('dialysis_procedure','dialysis_diagnosis')
union
SELECT person_id, Indication,drug_exposure_start_date as esrd_or_diaysis_date, index_date
FROM IC_drug_exposure
where Indication = 'dialysis_procedure'
union
SELECT person_id, Indication,observation_date as esrd_or_diaysis_date, index_date
FROM IC_observation
where Indication in ('dialysis_procedure','dialysis_diagnosis')))
where diff >= -1460 and diff <= 0

-- 9. Active treatment with high-dose corticosteroids, immunosuppressive or immunomodulatory therapy

-- Immunosuppressive Therapy

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.fac3578b-f9b3-4239-a0b9-a8127ed8973a"),
    IC_drug_exposure=Input(rid="ri.foundry.main.dataset.8399025d-1ab3-46ee-88fc-5ec4fbb2e95b")
)
select * from (
    select *, datediff(drug_exposure_start_date,index_date) as diff from (
        select person_id, drug_exposure_start_date, index_date
        FROM IC_drug_exposure
        where Indication = 'immunosuppressive_therapies'))
where diff >= -1460 and diff <= 0

-- High-Dose Corticosteriod

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.d6cadad6-c6d2-4197-9898-45a9d9154bb5"),
    concept_set_members=Input(rid="ri.foundry.main.dataset.e670c5ad-42ca-46a2-ae55-e917e3e161b6"),
    drug_exposure=Input(rid="ri.foundry.main.dataset.ec252b05-8f82-4f7f-a227-b3bb9bc578ef")
)
-- days_supply column has lots of null values, that's why diff between start and end date is taken
select * from (
    SELECT *, datediff(drug_exposure_end_date,drug_exposure_start_date) as days_supply_updated, datediff(drug_exposure_start_date, index_date) as diff from (
        select drug_exposure.person_id, concept_set_members.concept_id, concept_set_members.concept_name, drug_exposure.drug_concept_name, drug_exposure.quantity, drug_exposure.days_supply, drug_exposure.dose_unit_source_value, drug_exposure.drug_exposure_start_date,  drug_exposure.drug_exposure_end_date, cast('2024-05-31' as date) as index_date
        FROM concept_set_members
        inner join drug_exposure
        on concept_set_members.concept_id = drug_exposure.drug_concept_id
        where (concept_set_members.codeset_id = 148528505)))
where days_supply_updated >= 14 and (diff <= 0 and diff >= -1460)

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.1eec9761-385a-45ad-96a0-1b2c39391185"),
    Corticosteriod_therapies_with_at_least_14_weeks_of_treatment=Input(rid="ri.foundry.main.dataset.d6cadad6-c6d2-4197-9898-45a9d9154bb5")
)
SELECT *
FROM Corticosteriod_therapies_with_at_least_14_weeks_of_treatment
where concept_id in (19129254,19129511,19129513,40225703,1511403,19129290,19133985,37003556,19129512,19129514,40227495,19129398,19135172,19129289,40234801,42629020,40234046,40234038,792429,792427,40234031,40234030,903963,40073529,19034328,19034327,1551123,19121838,19028933,19034014,1551234,19007010,1551192,42873632,42873631,19113806,19034231,1551170,19033622,1551122,42873630,42873629,19113791,19034203,1551101,19031623,1551193,19033380,19024533,1551100,42873628,42873627,1551201,1551099,45776442,45776440,45776444,45776438,43526395,19128781,19061583,19001447,40244441,19007005,19132464,1550560,1551098,19126169,19125148,1551044,19128780,1550720,40226713,19070310,19130001,1551093,1551045,1551042,1551046,1551010,19067795,19122782,1550775,1550557,35606556,42902115,42902114,35606549,42902111,42902110,42902108,42902107,35606532,40060701,19034646,1506315,19034807,19080181,35606540,35606538,19099970,19034805,19034804,1506430,19067555,1506426,19034650,1506314,42901998,42901997,19034645,1506312,19034647,1506479,35606536,19034806,35606533,35606544,35606542,1506513,19104630,1506270,40049686,35604740,975929,19039286,975193,37003069,37003067,35604736,35604734,19006965,19039743,975169,19078350,37003066,37003064,35604730,19063670,19093653,19006967,19040156,975505,19006964,19039383,975168,19083661,37003062,37003060,37003058,37003052,975125,40231785,43526136,43526135,42708926,40241504,19076145,40028260,19030008,1518293,1518609,19030005,19030003,1518292,1518608,40167745,37499312,37497612,1518261,1518610,19030001,19018906,19029973,19076135,1518259,1518606,19095151,19095136,40160931,40160930,19006962,1518258,1518605,1356085,1356080,1518257,19108508,1518851,40173366,19076136,1518254,1592243,19016867,19016866,1592253,1592251,1592245,19018084,19018083,1592261,1592257,1592249,1592247,19086888,1507736,1507735,19075849,1507705,37498554,37498344,37498343,42708202,42708201,939426,939259,19074157,920458,40950844,35746407,41232711,21168390,36275715,36257150,21065413,40898093,43012435,43012434,37499792,37499790,37499791,37499789,40233219,40233205,40234056,740264,42629018,35606555,35606554,35606547,35606545,1719022,1719046,35202020,1719012,702628,1510782)

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.2a126434-ee32-4a7c-99d5-f55783270327"),
    Filtering_required_descendants=Input(rid="ri.foundry.main.dataset.1eec9761-385a-45ad-96a0-1b2c39391185"),
    drug_strength=Input(rid="ri.foundry.main.dataset.3160ed3e-13ff-4b10-9b77-5444b90bfd36")
)
SELECT Filtering_required_descendants.*, drug_strength.amount_value, drug_strength.numerator_value
FROM Filtering_required_descendants
left join drug_strength
on Filtering_required_descendants.concept_id = drug_strength.drug_concept_id

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.74058ec6-9b08-403f-ab81-a90a8a1935ee"),
    Extracting_drug_strength=Input(rid="ri.foundry.main.dataset.2a126434-ee32-4a7c-99d5-f55783270327")
)
SELECT distinct *, case when amount_value is not null then ((amount_value * quantity)/days_supply_updated) else ((numerator_value * quantity)/days_supply_updated) end as daily_dose    
FROM Extracting_drug_strength

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.19c3ea61-72e2-49f5-b75c-5a2e319a6da1"),
    daily_dose_calcaultion=Input(rid="ri.foundry.main.dataset.74058ec6-9b08-403f-ab81-a90a8a1935ee")
)
select *, 'corticosteriods' as Indication from (
SELECT *, case when lower(drug_concept_name) like '%betamethasone%' and daily_dose >= 3 then 1
     when lower(drug_concept_name) like '%budesonide%' and daily_dose >= 1.5 then 1
     when lower(drug_concept_name) like '%cortisone%' and daily_dose >= 100 then 1
     when lower(drug_concept_name) like '%deflazacort%' and daily_dose >= 24 then 1
     when lower(drug_concept_name) like '%dexamethasone%' and daily_dose >= 3 then 1
     when lower(drug_concept_name) like '%hydrocortisone%' and daily_dose >=80 then 1
     when lower(drug_concept_name) like '%methylprednisolone%' and daily_dose >= 16 then 1
     when lower(drug_concept_name) like '%prednisolone%' and daily_dose >= 20 then 1
     when lower(drug_concept_name) like '%prednisone%' and daily_dose >= 20 then 1
     when lower(drug_concept_name) like '%triamcinolone%' and daily_dose >= 16 then 1 else 0 end as qualifying_patients
FROM daily_dose_calcaultion)
where qualifying_patients = 1

-- - Betamethasone ≥3 mg daily dose
-- - Budesonide ≥1.5 mg daily dose
-- - Cortisone ≥100 mg daily dose
-- - Deflazacort ≥24 mg daily dose
-- - Dexamethasone ≥3 mg daily dose
-- - Hydrocortisone ≥80 mg daily dose
-- - Methylprednisolone ≥16 mg daily dose
-- - Prednisolone ≥20 mg daily dose
-- - Prednisone ≥20 mg daily dose
-- - Triamcinolone ≥16 mg daily dose

-- Both (High-Dose Corticosteriod & Immunosuppresive Therapy)

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.3d63495d-1785-4e5f-9736-0355be8d8c6f"),
    immunosuppressive_therepies=Input(rid="ri.foundry.main.dataset.fac3578b-f9b3-4239-a0b9-a8127ed8973a"),
    required_dosages_corticosteriods=Input(rid="ri.foundry.main.dataset.19c3ea61-72e2-49f5-b75c-5a2e319a6da1")
)
select distinct person_id from (
SELECT person_id, index_date, drug_exposure_start_date
FROM required_dosages_corticosteriods
union
SELECT person_id, index_date, drug_exposure_start_date
FROM immunosuppressive_therepies)


-- Total IC Population

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.a299b53f-90e9-453e-844e-5959caf0a58f"),
    Active_treatment_with_high_dose_corticosteroids_immunosuppressive_or_immunomodulatory=Input(rid="ri.foundry.main.dataset.3d63495d-1785-4e5f-9736-0355be8d8c6f"),
    CAR_T_or_HSCT=Input(rid="ri.foundry.main.dataset.4dc8b5fa-6a84-41e7-a896-2d021a446e8e"),
    ESRD_or_dialysis_treatment=Input(rid="ri.foundry.main.dataset.36811b53-c733-4c17-8192-7686f0108997"),
    HIV_lab_values=Input(rid="ri.foundry.main.dataset.e1ebe956-9d9f-40bb-a433-cf9ea49635f9"),
    Moderate_or_severe_primary_immunodeficiency=Input(rid="ri.foundry.main.dataset.c2c5f417-1b34-4728-9565-cce6eb3f7918"),
    Recipients_of_solid_organ_or_islet_transplant=Input(rid="ri.foundry.main.dataset.fe3e2cf4-128c-4d0f-a2ba-d490b39b6e14"),
    Solid_tumors_or_hematological_malignancies_on_treatmenet=Input(rid="ri.foundry.main.dataset.376f5055-35f8-47b5-91e6-ac5b3e374cc5"),
    get_solid_tumors_not_in_cancer_therapies=Input(rid="ri.foundry.main.dataset.87f17db1-27f5-4805-a37a-1ceb275f3943")
)
Select distinct person_id, cast('2024-05-31' as date) as index_date, Indication from (
SELECT  distinct person_id, 'Recipients_of_solid_organ_or_islet_transplant' as Indication
FROM Recipients_of_solid_organ_or_islet_transplant
union all
SELECT distinct person_id, 'CAR_T_or_HSCT' as Indication
FROM CAR_T_or_HSCT
union all
SELECT distinct person_id, 'Moderate_or_severe_primary_immunodeficiency' as Indication
FROM Moderate_or_severe_primary_immunodeficiency
union all
SELECT distinct person_id, 'ESRD_or_dialysis_treatment' as Indication
FROM ESRD_or_dialysis_treatment
union all
SELECT distinct person_id, 'HIV/AIDS' as Indication
FROM HIV_lab_values
union all
SELECT distinct person_id, 'Solid_tumors_or_hematological_malignancies_on_treatmenet' as Indication
FROM Solid_tumors_or_hematological_malignancies_on_treatmenet
union all
SELECT distinct person_id, 'Solid_tumors_or_hematological_malignancies_not_on_treatment' as Indication
FROM get_solid_tumors_not_in_cancer_therapies
union all
SELECT distinct person_id, 'Active_treatment_with_high_dose_corticosteroids_immunosuppressive_or_immunomodulatory' as Indication
FROM Active_treatment_with_high_dose_corticosteroids_immunosuppressive_or_immunomodulatory)

-- Total IC Population Aged 60 or Above

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.23991813-0ec2-42f7-86f6-0c2274ce2e2d"),
    IC_population=Input(rid="ri.foundry.main.dataset.a299b53f-90e9-453e-844e-5959caf0a58f"),
    person=Input(rid="ri.foundry.main.dataset.50cae11a-4afb-457d-99d4-55b4bc2cbe66")
)
SELECT 
    person_id, Indication, index_date, FLOOR(DATEDIFF(CURDATE(), date_of_birth) / 365) AS age
FROM (
    SELECT 
        person.person_id,
        person.year_of_birth,
        person.month_of_birth,
        person.day_of_birth,
        CONCAT(year_of_birth, '-', month_of_birth, '-', day_of_birth) AS date_of_birth, IC_population.index_date, IC_population.Indication
    FROM person
    INNER JOIN IC_population
    on person.person_id = IC_population.person_id
)
HAVING age >= 60


-- Demographic Distribution

-- 1. Gender
@transform_pandas(
    Output(rid="ri.foundry.main.dataset.259071b7-7cc8-49fb-a7d4-4c1e6cbd23c9"),
    IC_population=Input(rid="ri.foundry.main.dataset.a299b53f-90e9-453e-844e-5959caf0a58f"),
    person=Input(rid="ri.foundry.main.dataset.50cae11a-4afb-457d-99d4-55b4bc2cbe66")
)
SELECT 
    P.gender_concept_name,                        
    COUNT(DISTINCT IC.person_id) AS patient_count  
FROM 
    IC_population IC
INNER JOIN 
    person P ON IC.person_id = P.person_id 
GROUP BY 
    P.gender_concept_name                             
ORDER BY 
    patient_count DESC;                

-- 2. Zip code

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.c1e14544-980c-4464-931e-2dc05ed04bc7"),
    IC_population=Input(rid="ri.foundry.main.dataset.a299b53f-90e9-453e-844e-5959caf0a58f"),
    location=Input(rid="ri.foundry.main.dataset.efac41e8-cc64-49bf-9007-d7e22a088318"),
    person=Input(rid="ri.foundry.main.dataset.50cae11a-4afb-457d-99d4-55b4bc2cbe66")
)
SELECT 
    LOC.zip, COUNT(DISTINCT IC.person_id) AS patient_count
FROM 
    IC_population IC
INNER JOIN 
    person P ON IC.person_id = P.person_id
LEFT JOIN 
    location LOC ON P.location_id = LOC.location_id
GROUP BY LOC.zip ORDER BY patient_count DESC; 

-- 3. Age

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.ae47903b-62d3-477c-85f2-158cb0557feb"),
    IC_population=Input(rid="ri.foundry.main.dataset.a299b53f-90e9-453e-844e-5959caf0a58f"),
    person=Input(rid="ri.foundry.main.dataset.50cae11a-4afb-457d-99d4-55b4bc2cbe66")
)
SELECT 
    person_id, Indication, index_date, FLOOR(DATEDIFF(CURDATE(), date_of_birth) / 365) AS age
FROM (
    SELECT 
        person.person_id,
        person.year_of_birth,
        person.month_of_birth,
        person.day_of_birth,
        CONCAT(year_of_birth, '-', month_of_birth, '-', day_of_birth) AS date_of_birth, IC_population.index_date, IC_population.Indication
    FROM person
    INNER JOIN IC_population
    on person.person_id = IC_population.person_id
)

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.564847be-9306-4c49-a854-889fa833cb75"),
    IC_population_age=Input(rid="ri.foundry.main.dataset.ae47903b-62d3-477c-85f2-158cb0557feb")
)
SELECT 
    CASE 
        WHEN age < 18 THEN '<18'
        WHEN age BETWEEN 18 AND 49 THEN '18-49'
        WHEN age BETWEEN 50 AND 64 THEN '50-64'
        WHEN age >= 65 THEN '>=65'
        ELSE 'Missing'
    END AS age_group,
    COUNT(DISTINCT person_id) AS count
FROM IC_population_age
GROUP BY age_group

-- 4. Ethnicity

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.63167003-60a5-4eac-be9f-e26006bd71fb"),
    IC_population=Input(rid="ri.foundry.main.dataset.a299b53f-90e9-453e-844e-5959caf0a58f"),
    person=Input(rid="ri.foundry.main.dataset.50cae11a-4afb-457d-99d4-55b4bc2cbe66")
)
SELECT 
    P.ethnicity_concept_name,                        
    COUNT(DISTINCT IC.person_id) AS patient_count  
FROM 
    IC_population IC
INNER JOIN 
    person P ON IC.person_id = P.person_id 
GROUP BY 
    P.ethnicity_concept_name                             
ORDER BY 
    patient_count DESC;                

-- 5. Race

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.a4360a8c-dda7-4550-8c07-54bfa69e921c"),
    IC_population=Input(rid="ri.foundry.main.dataset.a299b53f-90e9-453e-844e-5959caf0a58f"),
    person=Input(rid="ri.foundry.main.dataset.50cae11a-4afb-457d-99d4-55b4bc2cbe66")
)
SELECT 
    P.race_concept_name,                        
    COUNT(DISTINCT IC.person_id) AS patient_count  
FROM 
    IC_population IC
INNER JOIN 
    person P ON IC.person_id = P.person_id 
GROUP BY 
    P.race_concept_name                             
ORDER BY 
    patient_count DESC;                



