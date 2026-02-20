from pyspark import pipelines as dp

@dp.materialized_view(name = '`sales_crm_erp`.bronze.`crm_cust_info`')
def crm_cust_info():
    df = spark.read.format('csv')\
               .option('inferSchema', True)\
               .option('multiline', False)\
               .option('header', True)\
               .load('/Volumes/sales_crm_erp/source_data/crm/cust_info.csv')
    return df

@dp.materialized_view(name = '`sales_crm_erp`.bronze.`crm_prd_info`')
def crm_prd_info():
    df = spark.read.format('csv')\
               .option('inferSchema', True)\
               .option('multiline', False)\
               .option('header', True)\
               .load('/Volumes/sales_crm_erp/source_data/crm/prd_info.csv')
    return df
    
@dp.materialized_view(name = '`sales_crm_erp`.bronze.`crm_sales_details`')
def crm_sales_details():
    df = spark.read.format('csv')\
               .option('inferSchema', True)\
               .option('multiline', False)\
               .option('header', True)\
               .load('/Volumes/sales_crm_erp/source_data/crm/sales_details.csv')
    return df

@dp.materialized_view(name = '`sales_crm_erp`.bronze.`erp_cust_az12`')
def crm_cust_info():
    df = spark.read.format('csv')\
               .option('inferSchema', True)\
               .option('multiline', False)\
               .option('header', True)\
               .load('/Volumes/sales_crm_erp/source_data/erp/CUST_AZ12.csv')
    return df

@dp.materialized_view(name = '`sales_crm_erp`.bronze.`erp_loc_a101`')
def crm_cust_info():
    df = spark.read.format('csv')\
               .option('inferSchema', True)\
               .option('multiline', False)\
               .option('header', True)\
               .load('/Volumes/sales_crm_erp/source_data/erp/LOC_A101.csv')
    return df

@dp.materialized_view(name = '`sales_crm_erp`.bronze.`erp_px_cat_g1v2`')
def crm_cust_info():
    df = spark.read.format('csv')\
               .option('inferSchema', True)\
               .option('multiline', False)\
               .option('header', True)\
               .load('/Volumes/sales_crm_erp/source_data/erp/PX_CAT_G1V2.csv')
    return df