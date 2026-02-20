from pyspark import pipelines as dp
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime, date

@dp.materialized_view(name='`sales_crm_erp`.silver.`crm_cust_info`')
def crm_cust_info():
    df_cust_info = spark.read.table('`sales_crm_erp`.bronze.`crm_cust_info`')
    window_spec = Window.partitionBy("cst_id").orderBy(F.col("cst_create_date").desc())
    df = df_cust_info.\
        filter(F.col('cst_id').isNotNull()).\
        withColumn("rn", F.row_number().over(window_spec))\
                    .filter(F.col("rn") == 1)\
                    .drop("rn").\
        select(
            *[F.col(c) for c in df_cust_info.columns if c not in ["cst_firstname", "cst_lastname", "cst_gndr", "cst_marital_status"]],
            *[F.trim(F.col(c)).alias(c) for c in ["cst_firstname", "cst_lastname"]],
            F.when(F.upper(F.trim(F.col("cst_gndr"))) == 'M', 'Male')\
                .when(F.upper(F.trim(F.col("cst_gndr"))) == 'F', 'Female')\
                .otherwise('n/a').alias('cst_gndr'),
            F.when(F.upper(F.trim(F.col("cst_marital_status"))) == 'M', 'Married')\
                .when(F.upper(F.trim(F.col("cst_marital_status"))) == 'S', 'Single')\
                .otherwise('n/a').alias('cst_marital_status'),
        )
    return df
    
@dp.materialized_view(name='`sales_crm_erp`.silver.`crm_prd_info`')
def crm_prd_info():
    df_prd_info = spark.read.table('`sales_crm_erp`.bronze.`crm_prd_info`')

    window_spec = Window.partitionBy('prd_key').orderBy(F.col('prd_start_dt'))

    df = df_prd_info.withColumn("prd_end_dt", F.lead("prd_start_dt", 1).over(window_spec))\
        .select(
        *[c for c in df_prd_info.columns if c not in ['prd_end_dt', 'prd_key', 'prd_cost', 'prd_line', 'prd_end_dt']],
        F.regexp_replace(F.substring(F.col('prd_key'), 1, 5), '-', '_').alias('cat_id'),
        F.substring(F.col('prd_key'), 7, F.length(F.col('prd_key'))).alias('prd_key'),
        F.coalesce(F.col('prd_cost'), F.lit(0)).alias('prd_cost'),
        F.when(F.upper(F.trim(F.col('prd_line'))) == 'M', 'Mountain')\
                        .when(F.upper(F.trim(F.col("prd_line"))) == 'R', 'Road')\
                        .when(F.upper(F.trim(F.col("prd_line"))) == 'S', 'Other Sales')\
                        .when(F.upper(F.trim(F.col("prd_line"))) == 'T', 'Touring')\
                        .otherwise(F.lit('n/a')).alias('prd_line'),
        F.when(F.col('prd_end_dt').isNotNull(), F.date_sub(F.col('prd_end_dt'), 1)).otherwise(F.lit("9999-12-31")).alias('prd_end_dt')
        )
    return df
    
@dp.materialized_view(name='`sales_crm_erp`.silver.`crm_sales_details`')
def crm_sales_info():
    df_sales_details = spark.read.table('`sales_crm_erp`.bronze.`crm_sales_details`')

    df = df_sales_details.\
        select(
            *[c for c in df_sales_details.columns if c not in ["sls_order_dt", "sls_ship_dt", "sls_due_dt", "sls_sales", "sls_price"]],
            F.to_date(F.col('sls_order_dt').cast("string"), "yyyyMMdd").alias('sls_order_dt'),
            F.to_date(F.col('sls_ship_dt').cast("string"), "yyyyMMdd").alias('sls_ship_dt'),
            F.to_date(F.col('sls_due_dt').cast("string"), "yyyyMMdd").alias('sls_due_dt'),
            F.when(F.col('sls_sales').isNull() | (F.col('sls_sales') <= 0) | (F.col('sls_sales') != F.col('sls_quantity') * F.col('sls_price')), F.col('sls_quantity') * F.abs(F.col('sls_price'))).otherwise(F.col('sls_sales')).alias('sls_sales'),
            F.when(F.col('sls_price').isNull() | (F.col('sls_price') <= 0), F.col('sls_sales') / F.col('sls_quantity')).otherwise(F.abs(F.col('sls_price'))).alias('sls_price')
        )
    return df

@dp.materialized_view(name='`sales_crm_erp`.silver.`erp_cust_az12`')
def erp_cust_az12():
    df_cust_az12 = spark.read.table('`sales_crm_erp`.bronze.`erp_cust_az12`')

    df = df_cust_az12.select(
        F.regexp_replace(F.col('CID'), '^NAS', '').alias('cid'),
        F.when((F.col('BDATE') > datetime.now()) | (F.col('BDATE') < date(1926, 1, 25)), F.lit(None).cast("date")).\
            otherwise(F.col('BDATE')).alias('bdate'),
        F.when(F.upper(F.trim(F.col('GEN'))).isin(['F', 'FEMALE']), "Female")\
            .when(F.upper(F.trim(F.col('GEN'))).isin(['M, MALE']), "Male")\
            .otherwise('n/a').alias('gen')
    )
    return df

@dp.materialized_view(name='`sales_crm_erp`.silver.`erp_loc_a101`')
def erp_loc_a101():
    df_loc_a101 = spark.read.table('`sales_crm_erp`.bronze.`erp_loc_a101`')

    df = df_loc_a101.select(
        F.regexp_replace(F.col('CID'), '-', '').alias('cid'),
        F.when(F.trim(F.col('cntry')).isin('USA', 'US'), 'United States')\
                .when(F.trim(F.col('cntry')) == 'DE', 'Germany')\
                .when((F.trim(F.col('cntry')) == '') | F.col('cntry').isNull(), 'n/a')\
                .otherwise(F.col('cntry')).alias('cntry')
    )
    return df

@dp.materialized_view(name='`sales_crm_erp`.silver.`erp_px_cat_g1v2`')
def erp_px_cat_g1v2():
    df_px_cat_g1v2 = spark.read.table('`sales_crm_erp`.bronze.`erp_px_cat_g1v2`')

    df = df_px_cat_g1v2.select(
        F.col("ID").alias('id'),
        F.col("CAT").alias('cat'),
        F.col("SUBCAT").alias('subcat'),
        F.col("MAINTENANCE").alias('maintenance')
    )
    return df