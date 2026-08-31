from pydantic import BaseModel, Field, model_validator
from typing import List, Optional, Any, Dict

def flatten_yaml_list(v: Any) -> dict:
    """Helper to flatten the list-of-dicts structure used in the YAMLs."""
    if isinstance(v, list) and all(isinstance(i, dict) for i in v):
        return {k: val for d in v for k, val in d.items()}
    return v

class FileIOConfig(BaseModel):
    # Required parameters based on config and README
    dir_save: str
    save_type: str
    save_loc: str
    path_data: str 
    path_hf_gpkg: str
    
    # Optional parameters
    home_dir: str = "~"
    data_source: Optional[str] = None
    dir_base: Optional[str] = None
    dir_std_base: Optional[str] = None
    dir_db_attrs: Optional[str] = None
    path_hf_basins_gpkg: Optional[str] = None
    gage_id_col_gpkg: Optional[str] = None
    gpkg_filename_pattern: Optional[str] = None
    hfatl_id_format: Optional[str] = None
    vpu_mapped: Optional[str] = None
    hf_fp_layer: Optional[str] = None
    hf_fp_id_col: Optional[str] = None
    vpu_id_col: Optional[str] = None
    dataset_name: Optional[str] = None
    
    @model_validator(mode='before')
    @classmethod
    def flatten(cls, values):
        return flatten_yaml_list(values)

class ColSchemaConfig(BaseModel):
    # Required parameters
    gage_id: str
    metric_cols: str
    featureID: str
    featureSource: str
    metric_mappings: str
    val_metrics: str = 'False'

    @model_validator(mode='before')
    @classmethod
    def flatten(cls, values):
        return flatten_yaml_list(values)

class FormulationMetadata(BaseModel):
    # Required parameters
    dataset_name: str
    formulation_base: str
    target_var: str
    start_date: str
    end_date: str
    cal_status: str

    # Optional parameters
    datasets: Optional[List[str]] = None
    formulation_id: Optional[str] = None
    formulation_ver: Optional[str] = None
    temporal_res: Optional[str] = None
    modeled_notes: Optional[str] = None
    start_date_cal: Optional[str] = None
    end_date_cal: Optional[str] = None
    cal_notes: Optional[str] = None

    @model_validator(mode='before')
    @classmethod
    def flatten(cls, values):
        return flatten_yaml_list(values)

class PrepConfig(BaseModel):
    col_schema: ColSchemaConfig
    file_io: FileIOConfig
    formulation_metadata: FormulationMetadata
    references: Optional[Any] = None

class AttrSelectConfig(BaseModel):
    hfatl_id_col: str
    paths_hfatl: List[str]
    hfatl_vars: List[str]

    @model_validator(mode='before')
    @classmethod
    def flatten(cls, values):
        return flatten_yaml_list(values)

class AttrConfig(BaseModel):
    col_schema: Optional[ColSchemaConfig] = None
    file_io: FileIOConfig
    formulation_metadata: FormulationMetadata
    attr_select: AttrSelectConfig