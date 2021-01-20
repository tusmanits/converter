import lkml

from dimension import Dimension


class View:
    def __init__(self):
        self.name = ''
        self.sql = ''
        self.databaseName = ''
        self.schemaName = ''
        self.dimenions = []
        self.targetSchema = ''
        self.viewType = ''
        self.persistedType = ''
        self.persistedSQL = ''

    def setView(self, view):

        if 'derived_table' in view:
            self.is_derived_table = True

            if 'sql' in view['derived_table']:
                self.sql = view['derived_table']['sql']
                self.sql = self.sql.replace('"','\"')

            if 'persist_for' in view['derived_table']:
                self.persistedSQL = view['derived_table']['persist_for']
                self.persistedSQL = self.persistedSQL.replace('"', "\"")
                self.persistedType = 'PERSIST_FOR'

            if 'sql_trigger_value' in view['derived_table']:
                self.persistedSQL = view['derived_table']['sql_trigger_value']
                self.persistedSQL = self.persistedSQL.replace('"', "\"")
                self.persistedType = 'SQL_TRIGGER_VALUE'

            self.viewType = 'PDT'

        if 'name' in view:
            self.name = view['name']

        if 'dimensions' in view:

            dimensions_ = []
            
            for dimensionRow in view['dimensions']:
                dimensionObj = Dimension()
                dimensionObj.setDimension(dimensionRow)
                dimensions_.append(dimensionObj)

            self.dimensions = Dimension().getProcessedSubstituteDimensions(dimensions_)



    def __str__(self):
        return """
            View: ---------------------------------------------------------------------------------------------------------------
            View Name       :     {name}
            Persisted Type  :     {persistedType}
            Persisted SQL   :     {persistedSQL}
            SQL             :     {sql}
            """.format(name = self.name, persistedType = self.persistedType, sql = self.sql, persistedSQL = self.persistedSQL)

    
    def getViewInfomationFromFile(self, fileName):

        views = []

        with open(fileName, 'r') as file:
            parsed = lkml.load(file)
            #logging.info(parsed)

            for view in parsed['views']:

                viewObj = View()

                viewObj.setView(view)
                views.append(viewObj)

        return views



    def writedbtModel(self):


        f = open("pdt_placeholder.ddl", "r")
        placeholder = f.read()
        dbtModelName = self.targetSchema.lower().strip().replace(' ', '_') + '_' + self.name.lower().strip().replace(' ', '_')

        fileName =  dbtModelName + '.sql'

        filePath = "../models/" + fileName

        dbtrunModelsPath = "run_models.sh" 
        dbtrunPresistedModelsPath = "run_presisted_models.sh"

        sql = self.getViewSQL()

        content = placeholder \
                    .replace("@@SCHEMA@@",self.targetSchema.lower().strip()) \
                    .replace("@@ALIAS@@", self.name.lower().strip()) \
                    .replace("@@SQL@@", self.sql) \
                    .replace("@@PERSISTED_TYPE@@", self.persistedType) \
                    .replace("@@PERSISTED_SQL@@", self.persistedSQL) 

        with open(filePath, 'w') as file:
            file.write(content)

        if self.viewType == 'PDT':
            content = 'dbt run --models {}\n'.format(dbtModelName)
            with open(dbtrunPresistedModelsPath, 'a') as file:
                file.write(content)

        content = 'dbt run --models {}\n'.format(dbtModelName)
        with open(dbtrunModelsPath, 'a') as file:
            file.write(content)

    def writedbtModel1(self):

        fileName = self.name.lower().strip() + '.sql'

        filePath = "../models/" + fileName

        dbtrunModelsPath = "run_models.sh" 
        dbtrunPresistedModelsPath = "run_presisted_models.sh"

        sql = self.getViewSQL()

        content = """
        {{{{ config(materialized = "{materialized}") }}}}

        {{{{ config(schema = "{schema}") }}}}

        {sql}

        """.format(schema = self.targetSchema, materialized = self.materialized, sql = sql)

        with open(filePath, 'w') as file:
            file.write(content)

        if self.materialized == 'table':
            content = 'dbt run --models {}\n'.format(self.name.lower().strip())
            with open(dbtrunPresistedModelsPath, 'a') as file:
                file.write(content)

        content = 'dbt run --models {}\n'.format(self.name.lower().strip())
        with open(dbtrunModelsPath, 'a') as file:
            file.write(content)

    def getViewSQL(self):

        viewSQL = ''

        dimList = []

        for dimension in self.dimensions:

            if dimension.name.upper().strip() != dimension.sql.upper().strip():
                row = "{} AS {}".format(dimension.sql.strip(), dimension.name.upper().strip())
                dimList.append(row)

            else:
                row = "{}".format(dimension.name.upper().strip())
                dimList.append(row)

        cols =  ',\n'.join(dimList)

        if self.is_derived_table:

            viewSQL = """
            SELECT
            {cols}
            FROM ({sql})
            """.format(cols = cols, sql = self.sql)

        return viewSQL




