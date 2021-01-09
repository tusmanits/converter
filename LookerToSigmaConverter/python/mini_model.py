
import looker_sdk
import logging
import json
from pprint import pprint



logging.basicConfig(filename='converter.log',level=logging.INFO, filemode='w', format = '%(asctime)s:%(levelname)s:%(message)s')

sdk = looker_sdk.init31('looker.ini')


models = sdk.all_lookml_models(fields= 'name, label, project_name, allowed_db_connection_names,has_content,can, unlimited_db_connections, explores')
pprint(models)

logging.info(models)

for model in models:
    logging.info(model.name)
    if model.name == 'system__activity':
        continue
    if model.name != 'its_sig':
        continue
        
        #pprint(modelDict)
        #logging.info(modelDict)

    lookml_model = sdk.lookml_model(lookml_model_name = model.name)
    modelDict = {
        "name": lookml_model.name,
        "label": lookml_model.label,
        "project_name": lookml_model.project_name,
        "allowed_db_connection_names": lookml_model.allowed_db_connection_names,
        "has_content": lookml_model.has_content,
        "can": lookml_model.can,
        "unlimited_db_connections": lookml_model.unlimited_db_connections
    }

    #logging.info(lookml_model)

    #logging.info(modelDict)

    for explore in lookml_model.explores:

        if explore.name != 'order_items':
            continue

        exploreObj = sdk.lookml_model_explore(
            lookml_model_name=model.name,
            explore_name=explore.name
        )

        logging.info(exploreObj)

        dim_list = []
        sql = 'INSERT INTO FIELDS(NAME, PRIMARY_KEY, TYPE, VIEW, VIEW_LABEL, DESCRIPTION, SQL, LABEL_SHORT) VALUES '
        for dimension in exploreObj.fields.dimensions:
            dim_def = {
                "name":dimension.name,
                "primary_key": dimension.primary_key,
                "type":dimension.type,
                "view":dimension.view,
                "view_label": dimension.view_label,
                "description": dimension.description,
                "sql": dimension.sql,
                "label_short":dimension.label_short,
            }
            dim_list.append(dim_def)
            if dimension.sql.startswith('${TABLE}'):
                sql = sql + """
                     ('{NAME}', '{PRIMARY_KEY}', '{TYPE}', '{VIEW}', '{VIEW_LABEL}', '{DESCRIPTION}', '{SQL}', '{LABEL_SHORT}'),\n
                    """.format(
                        NAME = dimension.name,
                        PRIMARY_KEY = dimension.primary_key,
                        TYPE = dimension.type,
                        VIEW = dimension.view,
                        VIEW_LABEL = dimension.view_label,
                        DESCRIPTION = ((lambda: '', lambda: dimension.description)[dimension.description != None]()).replace('\n',' '),
                        SQL = ((lambda: '', lambda: dimension.sql)[dimension.sql != None]()).replace('\n',''),
                        LABEL_SHORT = dimension.label_short
                ) 
        logging.info(sql.replace('\n', ''))

        dimensions = json.dumps(dim_list)

        #logging.info(dimensions)

        sql_values = ''

        join_list = []
        for join in exploreObj.joins:
            join_def = {
                "name":join.name,
                "dependent_fields" : join.dependent_fields,
                "fields" : join.fields,
                "foreign_key" : join.foreign_key,
                "from_" : join.from_,
                "outer_only" : join.outer_only,
                "relationship" : join.relationship,
                "required_joins" : join.required_joins,
                "sql_foreign_key" : join.sql_foreign_key,
                "sql_on" : ((lambda: '', lambda: join.sql_on)[join.sql_on != None]()).replace('\n',' '),
                "sql_table_name" : join.sql_table_name,
                "type": join.type,
                "view_label": join.view_label
            }
            join_list.append(join_def)
            sql_values  = sql_values + """
            (
                '{NAME}', '{DEPENDENT_FIELDS}', '{FIELDS}', '{FOREIGN_KEY}', '{FROM_}', '{OUTER_ONLY}', '{RELATIONSHIP}', '{REQUIRED_JOINS}', '{SQL_FOREIGN_kEY}', '{SQL_ON}', '{SQL_TABLE_NAME}', '{TYPE}', '{VIEW_LABEL}'
            ),
            """.format(
                NAME  = join.name,
                DEPENDENT_FIELDS = ','.join(join.dependent_fields).replace("'","''"),
                FIELDS = join.fields,
                FOREIGN_KEY = join.foreign_key,
                FROM_ = join.from_,
                OUTER_ONLY = join.outer_only,
                RELATIONSHIP = join.relationship,
                REQUIRED_JOINS = join.required_joins,
                SQL_FOREIGN_kEY = join.sql_foreign_key,
                SQL_ON = ((lambda: '', lambda: join.sql_on)[join.sql_on != None]()).replace('\n',' ').replace("'","''"),
                SQL_TABLE_NAME = join.sql_table_name,
                TYPE = join.type,
                VIEW_LABEL = join.view_label
            )
        #joins = json.dumps(join_list)

        sql = """
        INSERT INTO PUBLIC.JOINS(NAME, DEPENDENT_FIELDS, FIELDS, FOREIGN_KEY, FROM_, OUTER_ONLY, RELATIONSHIP, REQUIRED_JOINS, SQL_FOREIGN_kEY, SQL_ON, SQL_TABLE_NAME, TYPE, VIEW_LABEL)
        VALUES
        {SQL_VALUES}
        """.format(SQL_VALUES = sql_values)


        logging.info(sql)

        #logging.info(joins)

        #logging.info(sql)
        




'''
        for explore in model.explores:
            if explore["name"] == 'look':
                print(explore)
                logging.info(explore)

'''


#look = sdk.look(2)

#pprint(look)

#lookSQL = sdk.run_look(look_id = 2, result_format = 'sql', server_table_calcs = True)
#pprint(lookSQL)
#logging.info(lookSQL)

#dashboard = next(iter(sdk.search_dashboards(title='web analytics data tool')), None)

#pprint(dashboard)


#pprint(look)

#pprint(look.query)



'''
model = sdk.lookml_model(lookml_model_name = 'training')
pprint(model)
'''

'''
explore = sdk.lookml_model_explore(
        lookml_model_name='training',
        explore_name='order_items',
        fields="id, name, description, fields,joins",
    )
    
pprint(explore)

explore_def = {
    "id": explore.id,
    "name": explore.name,
    "description": explore.description
}

pprint(explore_def)


my_joins = []

for join in explore.joins:
    join_def = {
        "name":join.name,
        "dependent_fields" : join.dependent_fields,
        "fields" : join.fields,
        "foreign_key" : join.foreign_key,
        "from_" : join.from_,
        "outer_only" : join.outer_only,
        "relationship" : join.relationship,
        "required_joins" : join.required_joins,
        "sql_foreign_key" : join.sql_foreign_key,
        "sql_on" : join.sql_on,
        "sql_table_name" : join.sql_table_name,
        "type": join.type,
        "view_label": join.view_label
    }
    my_joins.append(join_def)
pprint(my_joins)
my_fields = []

# Iterate through the field definitions and pull in the description, sql,
# and other looker tags you might want to include in  your data dictionary.
if explore.fields and explore.fields.dimensions:
    for dimension in explore.fields.dimensions:
        dim_def = {
            "field_type": "Dimension",
            "view_name": dimension.view_label,
            "field_name": dimension.label_short,
            "type": dimension.type,
            "description": dimension.description,
            "sql": dimension.sql,
        }
        my_fields.append(dim_def)
if explore.fields and explore.fields.measures:
    for measure in explore.fields.measures:
        mes_def = {
            "field_type": "Measure",
            "view_name": measure.view_label,
            "field_name": measure.label_short,
            "type": measure.type,
            "description": measure.description,
            "sql": measure.sql,
        }
        my_fields.append(mes_def)
pprint(my_fields)
'''