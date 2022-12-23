from dagster import op, Out, In
from dagster_pandas import PandasColumn, create_dagster_pandas_dataframe_type
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
import pandas as pd

#### TRANSFORM IRL_PP ####

Transform_IRL_PPDataFrame = create_dagster_pandas_dataframe_type(
    name="Transform_IRL_PPDataFrame",
    columns=[
        PandasColumn.integer_column("Age",non_nullable=False),
        PandasColumn.string_column("Local_Authority", non_nullable=False),
        PandasColumn.string_column("Scenario", non_nullable=False)
        
    ],
)

    
@op(ins={'start':In(None)},out=Out(Transform_IRL_PPDataFrame))
def transform_extracted_IRL_PP(start) -> Transform_IRL_PPDataFrame:
    T_IRL_PP = pd.read_csv("D:/DAP/ca samples/AUTO/IRP/IRL_PP.csv", sep="\t")
    T_IRL_PP=pd.melt(T_IRL_PP,id_vars=['Local_Authority', 'Scenario', 'Age'],var_name='Date', value_name='Population')
   
    return T_IRL_PP
    

@op(ins={'T_IRL_PP': In(Transform_IRL_PPDataFrame)}, out=Out(None))
def stage_transformed_IRL_PP(T_IRL_PP):
    T_IRL_PP.to_csv("D:/DAP/ca samples/AUTO/IRP/transformed_IRL_PP.csv",sep="\t",index=False)
    
