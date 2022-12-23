from pymongo import MongoClient
from dagster import op, Out, In, DagsterType
from dagster_pandas import PandasColumn, create_dagster_pandas_dataframe_type
from datetime import datetime
import pandas as pd
mongo_connection_string = "mongodb://dap:dap@127.0.0.1"


#IRELAND POPULATION

IRPP_columns = {"Age":"Age",
"Local Authority":"Local_Authority",
"Scenario":"Scenario",
"year_2020":"2020",
"year_2021":"2021",
"year_2022":"2022",
"year_2023":"2023",
"year_2024":"2024",
"year_2025":"2025",
"year_2026":"2026",
"year_2027":"2027",
"year_2028":"2028",
"year_2029":"2029",
"year_2030":"2030",
"year_2031":"2031",
"year_2032":"2032",
"year_2033":"2033",
"year_2034":"2034",
"year_2035":"2035",
"year_2036":"2036",
"year_2037":"2037",
"year_2038":"2038",
"year_2039":"2039",
"year_2040":"2040"
}


IRL_PPDataFrame = create_dagster_pandas_dataframe_type(
    name="IRL_PPDataFrame",
    columns=[
        PandasColumn.integer_column("Age",non_nullable=False),
        PandasColumn.string_column("Local_Authority", non_nullable=False),
        PandasColumn.string_column("Scenario", non_nullable=False),
        PandasColumn.string_column("2020", non_nullable=False),
        PandasColumn.string_column("2021", non_nullable=False),
        PandasColumn.string_column("2022", non_nullable=False),
         PandasColumn.string_column("2023", non_nullable=False),
          PandasColumn.string_column("2024", non_nullable=False),
           PandasColumn.string_column("2025", non_nullable=False),
            PandasColumn.string_column("2026", non_nullable=False),
             PandasColumn.string_column("2027", non_nullable=False),
              PandasColumn.string_column("2028", non_nullable=False),
               PandasColumn.string_column("2029", non_nullable=False),
                PandasColumn.string_column("2030", non_nullable=False),
                 PandasColumn.string_column("2031", non_nullable=False),
                  PandasColumn.string_column("2032", non_nullable=False),
                   PandasColumn.string_column("2033", non_nullable=False),
                    PandasColumn.string_column("2034", non_nullable=False),
                     PandasColumn.string_column("2035", non_nullable=False),
                      PandasColumn.string_column("2036", non_nullable=False),
                       PandasColumn.string_column("2037", non_nullable=False),
                        PandasColumn.string_column("2038", non_nullable=False),
                         PandasColumn.string_column("2039", non_nullable=False) ,
                         PandasColumn.string_column("2040", non_nullable=False)
    ],
)

### EXTRACT IRELAND POPULATION###
print("70")
@op(ins={'start': In(bool)}, out=Out(IRL_PPDataFrame))
def extract_IRL_PP(start) -> IRL_PPDataFrame:
    print("73")
    conn = MongoClient(mongo_connection_string)
    db = conn["DAPGRPM_database"]
    IRL_PP = pd.DataFrame(db.ireland_population_projection.find({}))
    IRL_PP.drop(
        columns=['_id'],
        axis=1,
        inplace=True
    )
    IRL_PP.rename(
        columns=IRPP_columns,
        inplace=True
    )
    conn.close()
    return IRL_PP

@op(ins={'IRL_PP': In(IRL_PPDataFrame)}, out=Out(None))
def stage_extracted_IRL_PP(IRL_PP):
    IRL_PP.to_csv("D:/DAP/ca samples/AUTO/IRP/IRL_PP.csv",index=False,sep="\t")
    
