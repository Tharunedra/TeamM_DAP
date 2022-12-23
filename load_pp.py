from dagster import op, Out, In, get_dagster_logger
from sqlalchemy import create_engine, exc
from sqlalchemy.pool import NullPool
import pandas as pd

postgres_connection_string = "postgresql://dap:dap@127.0.0.1:5432/postgres"

## LOAD IRL_PP TABLE TO POSTGRESQL##
@op(ins={'start': In(None)},out=Out(bool))
def load_IRL_PP_TBL(start):
    logger = get_dagster_logger()
    IRL_PP = pd.read_csv("D:/DAP/ca samples/AUTO/IRP/transformed_IRL_PP.csv", sep="\t")
    try:
        engine = create_engine(postgres_connection_string,poolclass=NullPool)
        rowcount = IRL_PP.to_sql(
            name="IRL_PP",
            schema="public",
            con=engine,
            index=False,
            if_exists="append"
        )
        logger.info("%i records loaded" % rowcount)
        engine.dispose(close=True)
        return rowcount > 0
    except exc.SQLAlchemyError as error:
        logger.error("Error: %s" % error)
        return False

## LOAD EU_SI TABLE TO POSTGRESQL##        
@op(ins={'start': In(None)},out=Out(bool))
def load_EU_SI_TBL(start):
    logger = get_dagster_logger()
    IRL_PP = pd.read_csv("D:/DAP/ca samples/AUTO/IRP/transformed_EU_SI.csv", sep="\t")
    try:
        engine = create_engine(postgres_connection_string,poolclass=NullPool)
        
        rowcount = EU_SI.to_sql(
            name="EU_SI",
            schema="public",
            con=engine,
            index=False,
            if_exists="append"
        )
        logger.info("%i records loaded" % rowcount)
        engine.dispose(close=True)
        return rowcount > 0
    except exc.SQLAlchemyError as error:
        logger.error("Error: %s" % error)
        return False