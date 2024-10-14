

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.87f17db1-27f5-4805-a37a-1ceb275f3943"),
    Solid_tumors_or_hematological_malignancies=Input(rid="ri.foundry.main.dataset.34e7a927-51fc-4552-a93e-1ac01a402277"),
    cancer_therapies=Input(rid="ri.foundry.main.dataset.39c39d40-4924-4a2b-8e53-f78919c2de7f")
)
from pyspark.sql import functions as F

def get_solid_tumors_not_in_cancer_therapies(Solid_tumors_or_hematological_malignancies, cancer_therapies):

    solid_tumors_filtered = Solid_tumors_or_hematological_malignancies.select("person_id", "index_date").distinct()

    cancer_therapy_person_ids = cancer_therapies.select("person_id").distinct()

    # Perform the "not in" condition using a left anti join
    result = solid_tumors_filtered.join(cancer_therapy_person_ids, 
                                        on="person_id", 
                                        how="left_anti")
    
    return result

