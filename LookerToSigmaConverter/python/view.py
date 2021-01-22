import lkml
import re

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
        self.dependencies = ''
        self.dbtModelName = ''
        self.sql_table_name = None

    def setDBTModelName(self):
        self.dbtModelName = self.targetSchema.lower().strip().replace(' ', '_') + '_' + self.name.lower().strip().replace(' ', '_')

    def getDBTModelName(self):
        return self.dbtModelName

    def setView(self, view):

        if 'derived_table' in view:
            
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

        elif 'sql_table_name' in view:
            self.viewType = 'VIEW'
            self.sql_table_name = view['sql_table_name']

        if 'name' in view:
            self.name = view['name']

        dimensions_ = []
        if 'dimensions' in view:
            
            for dimensionRow in view['dimensions']:
                dimensionObj = Dimension()
                dimensionObj.setDimension(dimensionRow)
                dimensions_.append(dimensionObj)

        
        allDimensions = Dimension().getProcessedSubstituteDimensions(dimensions_)

        validDimensions = []

        for dimension_ in allDimensions:
            if not dimension_.isExcluded:
                validDimensions.append(dimension_) 

        self.dimensions = validDimensions

    def checkKeyExists(self, key, dictionary):
        found = False

        if key in dictionary:
            found = True

        return found

    def getKeyValue(self, key, dictionary):
        val = {}

        for item in dictionary:
            if key in item:
                val = item
        return val

    def injectViewSchema(self):

        if self.sql is not None and self.sql != '':

            processedSQL = re.sub(r'\s+',' ', self.sql.replace('\n', ' ').replace('\t', ' '))
            print("Source SQL: {}".format(processedSQL))
            dependencies = []

            keywords = ['lateral']

            rx = re.compile(r'FROM\s+(\w+\s*\w*,\s*\w+\s*\w*)\s+',re.IGNORECASE)

            for match in rx.finditer(processedSQL):
                group = match.group(1)
                found = False
                for keyword in keywords:
                    if keyword in group:
                        found = True

                if not found:
                    from_ = 'FROM {}'.format(group)
                    list_ = group.split(',')
                    transformedList = []
                    dictList = []
                    for item in list_:
                        itemStripped = item.strip()
                        if '.' not in itemStripped:
                            transformItem = "{}.{}".format(self.schemaName, itemStripped)
                            dictObj = {itemStripped:transformItem}
                            check = self.checkKeyExists(itemStripped, dictList) 
                            if not check:
                                dictList.append(dictObj)

                        else:
                            dictObj = {itemStripped:itemStripped}
                            found = self.checkKeyExists(itemStripped, dictList)
                            if not check:
                                dictList.append(dictObj)

                    schemaConcatenatedList = []

                    for item in list_:
                        key = item.strip()
                        schemaConcatenatedItem = self.getKeyValue(key, dictList)
                        value = schemaConcatenatedItem[key]
                        schemaConcatenatedList.append(value)

                    to_ = "FROM {}".format(' , '.join(schemaConcatenatedList))

                    processedSQL = re.sub(r'{}'.format(from_), to_, processedSQL, flags=re.I)            


            processedSQL = re.sub(r'\s+',' ', processedSQL)
            
            rx = re.compile(r'FROM\s+(\w+)', re.IGNORECASE)
            substitued = []
            for match in rx.finditer(processedSQL):
                group = match.group(1)
                
                if group not in substitued:
                    itemStripped = group.strip()
                    from_ = 'FROM {}'.format(group)
                    schemaConcatenatedValue ='{}.{}'.format(self.schemaName, itemStripped)
                    to_ = 'FROM {}'.format(schemaConcatenatedValue)
                    processedSQL = re.sub(from_, to_, processedSQL, flags=re.I)

                    substitued.append(itemStripped)

            processedSQL = re.sub(r'\s+',' ', processedSQL)


            rx = re.compile(r'JOIN\s+(\w+)', re.IGNORECASE)
            substitued = []
            for match in rx.finditer(processedSQL):
                group = match.group(1)
                
                if group not in substitued:
                    itemStripped = group.strip()
                    from_ = 'JOIN {}'.format(group)
                    schemaConcatenatedValue ='{}.{}'.format(self.schemaName, itemStripped)
                    to_ = 'JOIN {}'.format(schemaConcatenatedValue)
                    processedSQL = re.sub(from_, to_, processedSQL, flags=re.I)

                    substitued.append(itemStripped)

            processedSQL = re.sub(r'\s+',' ', processedSQL)

            self.sql = processedSQL

            print(self.sql)

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

    def injectSqlTableName(self, views):
        rx = re.compile(r'\$\{(\w+)\.SQL_TABLE_NAME\}',re.IGNORECASE)
        for match in rx.finditer(self.sql):
            group = match.group(1)
            
            view = self.getViewByName(group.lower().strip(), views)
            dbtModelName = view.getDBTModelName() 

            ref = r"{{ref('" + dbtModelName +r"')}}"

            processedSQL = re.sub(r'\$\{\w+\.SQL_TABLE_NAME\}',ref, self.sql)

            self.sql = processedSQL

    def injectSqlTableNameInSQLTriggerValue(self, views):
        rx = re.compile(r'\$\{(\w+)\.SQL_TABLE_NAME\}',re.IGNORECASE)
        for match in rx.finditer(self.persistedSQL):
            group = match.group(1)
            
            view = self.getViewByName(group.lower().strip(), views)
            dbtModelName = view.getDBTModelName() 

            #ref = r"{{ref('" + dbtModelName +r"')}}"

            ref = "{}.{}".format(view.targetSchema, view.name) 

            processedSQL = re.sub(r'\$\{\w+\.SQL_TABLE_NAME\}',ref, self.persistedSQL)

            self.persistedSQL = processedSQL


    
    def getViewByName(self, name, views):
        view = None

        for view_ in views:
            if view_.name == name:
                view = view_
        return view

    def writedbtModel(self):

        if self.viewType == 'PDT':
            placeholder = 'pdt_placeholder.ddl'
        else:
            placeholder = 'view_placeholder.ddl'

        f = open(placeholder, "r")
        placeholder = f.read()
        dbtModelName = self.dbtModelName

        fileName =  dbtModelName + '.sql'

        filePath = "../models/" + fileName

        dbtrunModelsPath = "run_models.sh" 
        dbtrunPresistedModelsPath = "run_presisted_models.sh"

        sql = self.sql

        content = placeholder \
                    .replace("@@SCHEMA@@",self.targetSchema.lower().strip()) \
                    .replace("@@ALIAS@@", self.name.lower().strip()) \
                    .replace("@@SQL@@", sql) \
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

        if cols == None or cols.strip() == '':
            cols = '*'

        if self.viewType == 'PDT':
            viewSQL = """
            SELECT
            {cols}
            FROM ({sql})
            """.format(cols = cols, sql = self.sql)
        elif self.viewType == 'VIEW':
            viewSQL = """
            SELECT
            {cols}
            FROM {sql}
            """.format(cols = cols, sql = self.sql_table_name)
        else:
            viewSQL = ''

        self.sql = viewSQL    
        return viewSQL




