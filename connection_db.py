import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

# Cargar variables del archivo .env
load_dotenv()

# Conexión usando las variables del .env
engine = create_engine(
    f"mysql+mysqlconnector://{os.getenv('DB_USER')}:{os.getenv('DB_PASS')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
)



df = pd.read_csv("Data/candidates.csv", sep=";", encoding="utf-8-sig")


df = df.rename(columns={
    "First Name": "first_name",
    "Last Name": "last_name",
    "Email": "email",
    "Application Date": "apply_date",
    "Country": "country",
    "YOE": "yoe",
    "Seniority": "seniority",
    "Technology": "technology",
    "Code Challenge Score": "code_score",
    "Technical Interview Score": "interview_score"
})


df["apply_date"] = pd.to_datetime(df["apply_date"], errors="coerce").dt.date
df["code_score"] = pd.to_numeric(df["code_score"], errors="coerce")
df["interview_score"] = pd.to_numeric(df["interview_score"], errors="coerce")
df["yoe"] = pd.to_numeric(df["yoe"], errors="coerce")

df["hired"] = np.where(
    (df["code_score"] >= 7) & (df["interview_score"] >= 7),
    1, 0
)


dim_date = df[["apply_date"]].drop_duplicates().rename(columns={"apply_date": "full_date"})
dim_date["date_sk"] = dim_date["full_date"].apply(lambda d: d.year*10000 + d.month*100 + d.day)
tmp = pd.to_datetime(dim_date["full_date"])
dim_date["year"] = tmp.dt.year
dim_date["month"] = tmp.dt.month
dim_date["day"] = tmp.dt.day
dim_date["quarter"] = tmp.dt.quarter
dim_date["week"] = tmp.dt.isocalendar().week.astype(int)


with engine.begin() as conn:
    dim_date[["date_sk","full_date","year","month","day","quarter","week"]].to_sql(
        "dim_date", conn, if_exists="append", index=False
    )

    df[["first_name","last_name","email"]].drop_duplicates().to_sql(
        "dim_candidate", conn, if_exists="append", index=False
    )

    df[["country"]].drop_duplicates().rename(columns={"country":"name"}).to_sql(
        "dim_country", conn, if_exists="append", index=False
    )

    df[["technology"]].drop_duplicates().rename(columns={"technology":"name"}).to_sql(
        "dim_technology", conn, if_exists="append", index=False
    )

    df[["seniority"]].drop_duplicates().rename(columns={"seniority":"level"}).to_sql(
        "dim_seniority", conn, if_exists="append", index=False
    )


dim_date_db      = pd.read_sql("SELECT date_sk, full_date FROM dim_date", engine)
dim_candidate_db = pd.read_sql("SELECT candidate_sk, email FROM dim_candidate", engine)
dim_country_db   = pd.read_sql("SELECT country_sk, name FROM dim_country", engine)
dim_tech_db      = pd.read_sql("SELECT tech_sk, name FROM dim_technology", engine)
dim_sen_db       = pd.read_sql("SELECT seniority_sk, level FROM dim_seniority", engine)


fact = (df
    .merge(dim_date_db, left_on="apply_date", right_on="full_date", how="left")
    .merge(dim_candidate_db, on="email", how="left")
    .merge(dim_country_db, left_on="country", right_on="name", how="left")
    .merge(dim_tech_db, left_on="technology", right_on="name", how="left")
    .merge(dim_sen_db, left_on="seniority", right_on="level", how="left")
)

fact = fact[[
    "date_sk","candidate_sk","tech_sk","country_sk","seniority_sk",
    "yoe","code_score","interview_score","hired"
]]


with engine.begin() as conn:
    fact.to_sql("fact_application", conn, if_exists="append", index=False)

print("Done: loaded all applications with hired flag.")
