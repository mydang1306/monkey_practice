from base import getLogger 
from base import execute_select_query, execute_query
from base import get_template, render_template
from tqdm import tqdm

logger = getLogger()
def get_data_feature_from_query(query, source_data="data-team", feature_source='warehouse'):

    logger.debug(f"Query data from & feature: {query}")
    
    template = get_template("query_feature_user")
    if source_data == feature_source:
        render_templated = render_template(template, {"query_select_user": query} )
    else:
        # query from source and make a copy to warehouse 
        temp_tbl = "warehouse.data_mart.thailand_order"
        drop_table_sql = f"DROP TABLE IF EXISTS {temp_tbl};"
        print(drop_table_sql)
        execute_query(drop_table_sql, "warehouse")

        create_tmp_tbl = f"""
        CREATE TABLE warehouse.data_mart.thailand_order as (
            {query}
        )
        """
        execute_query(create_tmp_tbl)
        support_query = "SELECT * FROM warehouse.data_mart.thailand_order"
        render_templated = render_template(template, {"query_select_user": support_query} )
    
    logger.debug(render_templated)
    logger.debug("Load feature & data from warehouse")
    data_feature_user = execute_select_query(render_templated)

    # data_feature_user.to_csv("data.csv", index=False)
    return data_feature_user

def collect_feature_data(tbl_contact:str):
    
    templates_features = [ 'daily_demoraphic','daily_general', 'daily_payment', 
                          'daily_screen', 'daily_sieuviet', 'daily_source', 
                          'daily_study_mj', 'daily_timecontact']
    
    for template in tqdm(templates_features):
        logger.debug(f"Run template: {template}")
        template_form = get_template(template)
        rendered_template = render_template(template_form, {"table_contact": tbl_contact})
        execute_query(rendered_template)
