from pyspark import pipelines as dp
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import *
# from datetime import datetime, date

@dp.materialized_view(name='`sales_crm_erp`.gold.`dim_customers`')
def dim_customers():
    df_cust_info = spark.read.table('`sales_crm_erp`.silver.`crm_cust_info`')
    df_cust_az12 = spark.read.table('`sales_crm_erp`.silver.`erp_cust_az12`')
    df_loc_a101 = spark.read.table('`sales_crm_erp`.silver.`erp_loc_a101`')
    df_cust = df_cust_info\
            .join(
                df_cust_az12, df_cust_info['cst_key'] == df_cust_az12['cid'], how="left"
            ).join(
                df_loc_a101, df_cust_info['cst_key'] == df_loc_a101['cid'], how="left"
            ).select(
                F.row_number().over(Window.orderBy(df_cust_info['cst_id'])).alias('customer_key'),
                df_cust_info['cst_id'].alias('customer_id'),
                df_cust_info['cst_key'].alias('customer_number'),
                df_cust_info['cst_firstname'].alias('first_name'),
                df_cust_info['cst_lastname'].alias('last_name'),
                df_loc_a101['cntry'].alias('country'),
                F.coalesce(
                  F.when(df_cust_info['cst_gndr'] != "n/a", df_cust_info['cst_gndr']),
                  df_cust_az12['gen'],
                  F.lit("n/a")
                ).alias('gender'),
                df_cust_info['cst_marital_status'].alias('marital_status'),
                df_cust_az12['bdate'].alias('birthdate'),
                df_cust_info['cst_create_date'].alias('create_date')
            )
    return df_cust

@dp.materialized_view(name='`sales_crm_erp`.gold.`dim_products`')
def dim_products():
    df_prd_info = spark.read.table('`sales_crm_erp`.silver.`crm_prd_info`')
    df_px_cat_g1v2 = spark.read.table('`sales_crm_erp`.silver.`erp_px_cat_g1v2`')
    df_product = df_prd_info\
                .join(df_px_cat_g1v2, df_prd_info['cat_id'] == df_px_cat_g1v2['id'], how="left")\
                .select(
                    F.row_number().over(Window.orderBy(df_prd_info['prd_start_dt'], df_prd_info['prd_key'])).alias('product_key'),
                    df_prd_info['prd_id'].alias('product_id'),
                    df_prd_info['prd_key'].alias('product_number'),
                    df_prd_info['prd_nm'].alias('product_name'),
                    df_prd_info['cat_id'].alias('category_id'),
                    df_px_cat_g1v2['cat'].alias('category'),
                    df_px_cat_g1v2['subcat'].alias('subcategory'),
                    df_px_cat_g1v2['maintenance'].alias('maintenance'),
                    df_prd_info['prd_cost'].alias('cost'),
                    df_prd_info['prd_line'].alias('product_line'),
                    df_prd_info['prd_start_dt'].alias('start_date')
                    # df_prd_info['prd_end_dt'] //removed because all the values will be the same
                )\
                .where(df_prd_info['prd_end_dt'] == '9999-12-31')
    return df_product

@dp.materialized_view(name='`sales_crm_erp`.gold.`fact_sales`')
def fact_sales():
    df_sales_details = spark.read.table('`sales_crm_erp`.silver.`crm_sales_details`')
    df_cust = spark.read.table('dim_customers')
    df_product = spark.read.table('dim_products')
    df_sales = df_sales_details\
        .join(df_product, df_sales_details['sls_prd_key'] == df_product['product_number'], how="left")\
        .join(df_cust, df_sales_details['sls_cust_id'] == df_cust['customer_id'], how="left")\
        .select(
            df_sales_details['sls_ord_num'].alias('order_number'),
            df_product['product_key'].alias('product_key'),
            df_cust['customer_key'].alias('customer_key'),
            df_sales_details['sls_order_dt'].alias('order_date'),
            df_sales_details['sls_ship_dt'].alias('shipping_date'),
            df_sales_details['sls_due_dt'].alias('due_date'),
            df_sales_details['sls_sales'].alias('sales_amount'),
            df_sales_details['sls_quantity'].alias('quantity'),
            df_sales_details['sls_price'].alias('price'),
    )
    return df_sales