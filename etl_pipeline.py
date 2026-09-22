import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import time


def run_etl_pipeline():

    print("Starting ETL Pipeline...")
    start_time = time.time()

    # =========================================================
    # 1. EXTRACT
    # =========================================================

    file_path = r"E:\project\Raw_data\accepted_2007_to_2018Q4.csv\accepted_2007_to_2018Q4.csv"

    columns_to_keep = [
        'id',
        'loan_amnt',
        'term',
        'int_rate',
        'grade',
        'emp_length',
        'home_ownership',
        'annual_inc',
        'issue_d',
        'loan_status',
        'purpose',
        'dti'
    ]

    print("Extracting data from CSV...")

    df = pd.read_csv(
        file_path,
        usecols=columns_to_keep,
        low_memory=False,
        nrows=500000
    )

    # =========================================================
    # 2. TRANSFORM
    # =========================================================

    print("Cleaning and Transforming data...")

    # -------------------------
    # Missing Value Treatment
    # -------------------------

    df['emp_length'] = df['emp_length'].fillna('Unknown')

    df['annual_inc'] = df['annual_inc'].fillna(
        df['annual_inc'].median()
    )

    df['dti'] = df['dti'].fillna(
        df['dti'].median()
    )

    # -------------------------
    # Loan Status Filtering
    # -------------------------

    valid_statuses = [
        'Fully Paid',
        'Charged Off',
        'Default'
    ]

    df = df[
        df['loan_status'].isin(valid_statuses)
    ].copy()

    # =========================================================
    # RISK SEGMENTATION
    # =========================================================

    print("Creating Risk Categories...")

    conditions = [

        # High Risk
        (
            (df['dti'] > 30) &
            (df['annual_inc'] < 50000)
        ),

        # Medium Risk
        (
            (df['dti'] > 20) |
            (df['annual_inc'] < 50000)
        )
    ]

    choices = [
        'High Risk',
        'Medium Risk'
    ]

    df['Risk_Category'] = np.select(
        conditions,
        choices,
        default='Low Risk'
    )

    # =========================================================
    # 3. LOAD
    # =========================================================

    print("Loading data into MySQL Database...")

    db_user = 'root'
    db_password = '2005'
    db_host = 'localhost'
    db_name = 'credit_risk_db'

    engine = create_engine(
        f"mysql+pymysql://{db_user}:{db_password}@{db_host}/{db_name}"
    )

    df.to_sql(
        name='loan_portfolio',
        con=engine,
        if_exists='replace',
        index=False,
        chunksize=10000
    )

    # =========================================================
    # SUMMARY
    # =========================================================

    end_time = time.time()

    print(
        f"ETL Pipeline Completed Successfully in "
        f"{round(end_time - start_time, 2)} seconds!"
    )

    print(f"Total Rows Loaded: {len(df)}")

    print("\nRisk Category Distribution:")
    print(df['Risk_Category'].value_counts())
