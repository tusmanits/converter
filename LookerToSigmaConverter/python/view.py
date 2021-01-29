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
        self.allDimensions = []
        self.validDimensions = []
        self.excludedDimensions = []

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

        dimensionGroupList = []
        if 'dimension_groups' in view:

            for dimension_groupRow in view['dimension_groups']:
                print('Dimension Group Row-----------------'+str(dimension_groupRow))

                baseName = None

                if '${TABLE}.' in dimension_groupRow['sql']:
                    baseName = dimension_groupRow['sql'].replace('${TABLE}.', '')
                dimensionGroupName = dimension_groupRow['name']
                print("baseName:" + baseName)

                if 'timeframes' in dimension_groupRow:
                    name = None
                    type = None
                    sql = None

                    for timeframe in dimension_groupRow['timeframes']:
                        name = '{}_{}'.format(dimensionGroupName, timeframe)
                        if timeframe == 'raw':
                            type = 'date'
                            sql="{}".format(baseName)

                        elif timeframe == 'date':
                            type = 'string'
                            sql = "TO_CHAR(TO_DATE({}), 'YYYY-MM-DD')".format(baseName)

                            dimensionObj = Dimension()
                            dimensionObj.setDimension(
                                {
                                    "name": name,
                                    "sql" : sql,
                                    "type": type
                                }
                                )
                            print(dimensionObj)

                        elif timeframe == 'week':
                            type = 'string'
                            sql="TO_CHAR(DATE_TRUNC('week', {} ), 'YYYY-MM-DD')".format(baseName)

                            dimensionObj = Dimension()
                            dimensionObj.setDimension(
                                {
                                    "name": name,
                                    "sql" : sql,
                                    "type": type
                                }
                                )
                            print(dimensionObj)
                        elif timeframe == 'month':
                            type = 'string'
                            sql="TO_CHAR(DATE_TRUNC('month',{}), 'YYYY-MM')".format(baseName)
                            
                            dimensionObj = Dimension()
                            dimensionObj.setDimension(
                                {
                                    "name": name,
                                    "sql" : sql,
                                    "type": type
                                }
                                )
                            print(dimensionObj)
                        elif timeframe == 'time':
                            type = 'string'
                            sql="TO_CHAR(DATE_TRUNC('second',{}), 'YYYY-MM-DD HH24:MI:SS')".format(baseName)
                            
                            dimensionObj = Dimension()
                            dimensionObj.setDimension(
                                {
                                    "name": name,
                                    "sql" : sql,
                                    "type": type
                                }
                                )
                            print(dimensionObj)
                        elif timeframe == 'yesno':
                            type = 'string'
                            sql="CASE WHEN {} IS NOT NULL THEN 'YES' ELSE 'NO' END".format(baseName)
                            
                            dimensionObj = Dimension()
                            dimensionObj.setDimension(
                                {
                                    "name": name,
                                    "sql" : sql,
                                    "type": type
                                }
                                )
                            print(dimensionObj)
                        elif timeframe == 'day_of_month':
                            type = 'string'
                            sql="EXTRACT(DAY FROM {} )::integer".format(baseName)
                            
                            dimensionObj = Dimension()
                            dimensionObj.setDimension(
                                {
                                    "name": name,
                                    "sql" : sql,
                                    "type": type
                                }
                                )
                            print(dimensionObj)
                        elif timeframe == 'day_of_year':
                            type = 'string'
                            sql="EXTRACT(DOY FROM {} )::integer".format(baseName)
                            
                            dimensionObj = Dimension()
                            dimensionObj.setDimension(
                                {
                                    "name": name,
                                    "sql" : sql,
                                    "type": type
                                }
                                )
                            print(dimensionObj)
                        elif timeframe == 'hour_of_day':
                            type = 'string'
                            sql="CAST(EXTRACT(HOUR FROM CAST({}  AS TIMESTAMP)) AS INT)".format(baseName)
                            
                            dimensionObj = Dimension()
                            dimensionObj.setDimension(
                                {
                                    "name": name,
                                    "sql" : sql,
                                    "type": type
                                }
                                )
                            print(dimensionObj)
                        elif timeframe == 'time_of_day':
                            type = 'string'
                            sql="TO_CHAR({} , 'HH24:MI')".format(baseName)
                            
                            dimensionObj = Dimension()
                            dimensionObj.setDimension(
                                {
                                    "name": name,
                                    "sql" : sql,
                                    "type": type
                                }
                                )
                            print(dimensionObj)
                        elif timeframe == 'week_of_year':
                            type = 'string'
                            sql="EXTRACT(WEEK FROM {} )::int".format(baseName)
                            
                            dimensionObj = Dimension()
                            dimensionObj.setDimension(
                                {
                                    "name": name,
                                    "sql" : sql,
                                    "type": type
                                }
                                )
                            print(dimensionObj)
                        elif timeframe == 'fiscal_quarter_of_year':
                            type = 'string'
                            sql="(CAST('Q' AS VARCHAR) || CAST(CEIL(EXTRACT(MONTH FROM {} )::integer / 3) AS VARCHAR))".format(baseName)
                            
                            dimensionObj = Dimension()
                            dimensionObj.setDimension(
                                {
                                    "name": name,
                                    "sql" : sql,
                                    "type": type
                                }
                                )
                            print(dimensionObj)
                        elif timeframe == 'quarter_of_year':
                            type = 'string'
                            sql="(CAST('Q' AS VARCHAR) || CAST(CEIL(EXTRACT(MONTH FROM {} )::integer / 3) AS VARCHAR))".format(baseName)
                            
                            dimensionObj = Dimension()
                            dimensionObj.setDimension(
                                {
                                    "name": name,
                                    "sql" : sql,
                                    "type": type
                                }
                                )
                            print(dimensionObj)
                        elif timeframe == 'day_of_week_index':
                            type = 'string'
                            sql="MOD(EXTRACT(DOW FROM {} )::integer - 1 + 7, 7)".format(baseName)
                            
                            dimensionObj = Dimension()
                            dimensionObj.setDimension(
                                {
                                    "name": name,
                                    "sql" : sql,
                                    "type": type
                                }
                                )
                            print(dimensionObj)
                        elif timeframe == 'fiscal_month_num':
                            type = 'string'
                            sql="EXTRACT(MONTH FROM {} )::integer".format(baseName)
                            
                            dimensionObj = Dimension()
                            dimensionObj.setDimension(
                                {
                                    "name": name,
                                    "sql" : sql,
                                    "type": type
                                }
                                )
                            print(dimensionObj)
                        elif timeframe == 'fiscal_quarter':
                            type = 'string'
                            sql="TO_CHAR(DATE_TRUNC('month', CAST(DATE_TRUNC('quarter', {} ) AS DATE)), 'YYYY-MM')".format(baseName)
                            
                            dimensionObj = Dimension()
                            dimensionObj.setDimension(
                                {
                                    "name": name,
                                    "sql" : sql,
                                    "type": type
                                }
                                )
                            print(dimensionObj)
                        elif timeframe == 'fiscal_year':
                            type = 'string'
                            sql="EXTRACT(YEAR FROM {} )::integer".format(baseName)
                            
                            dimensionObj = Dimension()
                            dimensionObj.setDimension(
                                {
                                    "name": name,
                                    "sql" : sql,
                                    "type": type
                                }
                                )
                            print(dimensionObj)
                        elif timeframe == 'hour':
                            type = 'string'
                            sql="TO_CHAR(DATE_TRUNC('hour', {} ), 'YYYY-MM-DD HH24')".format(baseName)
                            
                            dimensionObj = Dimension()
                            dimensionObj.setDimension(
                                {
                                    "name": name,
                                    "sql" : sql,
                                    "type": type
                                }
                                )
                            print(dimensionObj)
                        elif timeframe == 'microsecond':
                            type = 'string'
                            sql="LEFT(TO_CHAR({} , 'YYYY-MM-DD HH24:MI:SS.FF'), 26)".format(baseName)
                            
                            dimensionObj = Dimension()
                            dimensionObj.setDimension(
                                {
                                    "name": name,
                                    "sql" : sql,
                                    "type": type
                                }
                                )
                            print(dimensionObj) 
                        elif timeframe == 'millisecond':
                            type = 'string'
                            sql="LEFT(TO_CHAR({} , 'YYYY-MM-DD HH24:MI:SS.FF'), 23)".format(baseName)
                            
                            dimensionObj = Dimension()
                            dimensionObj.setDimension(
                                {
                                    "name": name,
                                    "sql" : sql,
                                    "type": type
                                }
                                )
                            print(dimensionObj)
                        elif timeframe == 'minute':
                            type = 'string'
                            sql="TO_CHAR(DATE_TRUNC('minute', {} ), 'YYYY-MM-DD HH24:MI')".format(baseName)
                            
                            dimensionObj = Dimension()
                            dimensionObj.setDimension(
                                {
                                    "name": name,
                                    "sql" : sql,
                                    "type": type
                                }
                                )
                            print(dimensionObj)
                        elif timeframe == 'month_num':
                            type = 'string'
                            sql="EXTRACT(MONTH FROM {} )::integer".format(baseName)
                            
                            dimensionObj = Dimension()
                            dimensionObj.setDimension(
                                {
                                    "name": name,
                                    "sql" : sql,
                                    "type": type
                                }
                                )
                            print(dimensionObj)
                        elif timeframe == 'quarter':
                            type = 'string'
                            sql="TO_CHAR(DATE_TRUNC('month', CAST(DATE_TRUNC('quarter', {} ) AS DATE)), 'YYYY-MM')".format(baseName)
                            
                            dimensionObj = Dimension()
                            dimensionObj.setDimension(
                                {
                                    "name": name,
                                    "sql" : sql,
                                    "type": type
                                }
                                )
                            print(dimensionObj) 
                        elif timeframe == 'second':
                            type = 'string'
                            sql="TO_CHAR(DATE_TRUNC('second', {} ), 'YYYY-MM-DD HH24:MI:SS')".format(baseName)
                            
                            dimensionObj = Dimension()
                            dimensionObj.setDimension(
                                {
                                    "name": name,
                                    "sql" : sql,
                                    "type": type
                                }
                                )
                            print(dimensionObj)  
                        elif timeframe == 'day_of_week':
                            type = 'string'
                            sql="CASE TO_CHAR({} , 'DY') WHEN 'Tue' THEN 'Tuesday' WHEN 'Wed' THEN 'Wednesday' WHEN 'Thu' THEN 'Thursday' WHEN 'Sat' THEN 'Saturday' ELSE TO_CHAR({} , 'DY') || 'day' END ".format(baseName,baseName)                          
                            dimensionObj = Dimension()
                            dimensionObj.setDimension(
                                {
                                    "name": name,
                                    "sql" : sql,
                                    "type": type
                                }
                                )
                            print(dimensionObj)

                        elif timeframe == 'month_name':
                            type = 'string'
                            sql="DECODE(EXTRACT('month', {} ), 1, 'January', 2, 'February', 3, 'March', 4, 'April', 5, 'May', 6, 'June', 7, 'July', 8, 'August', 9, 'September', 10, 'October', 11, 'November', 12, 'December')".format(baseName)
                            
                            dimensionObj = Dimension()
                            dimensionObj.setDimension(
                                {
                                    "name": name,
                                    "sql" : sql,
                                    "type": type
                                }
                                )
                            print(dimensionObj)
                        
                        elif re.search(r'hour\d+', timeframe):
                            rx = re.compile(r'hour(\d+)')
                            matches_raw = [match.group(1) for match in rx.finditer(timeframe)]
                            hourValue = None
                            for item in matches_raw:
                                hourValue = item
                            type = 'string'
                            sql="TO_CHAR(DATE_TRUNC('hour', DATE_TRUNC('hour', DATEADD('HOURS', -1 * (CAST(DATE_PART('HOUR', CAST({}  AS TIMESTAMP)) AS INT) % {}), {} ))), 'YYYY-MM-DD HH24')".format(baseName,hourValue,baseName)
                            dimensionObj = Dimension()
                            dimensionObj.setDimension(
                                {
                                    "name": name,
                                    "sql" : sql,
                                    "type": type
                                }
                                )
                            print(dimensionObj)
                        elif re.search(r'millisecond\d+', timeframe):
                            rx = re.compile(r'millisecond(\d+)')
                            matches_raw = [match.group(1) for match in rx.finditer(timeframe)]
                            hourValue = None
                            for item in matches_raw:
                                hourValue = item
                            type = 'string'
                            sql="LEFT(TO_CHAR(TO_TIMESTAMP(LEFT(TO_CHAR(DATEADD('NANOSECOND', -1 * (CAST(DATE_PART('NANOSECOND', CAST({}  AS TIMESTAMP)) AS INT) % ({} * 1000000)), {} ), 'YYYY-MM-DD HH24:MI:SS.FF'), 23)), 'YYYY-MM-DD HH24:MI:SS.FF'), 23)".format(baseName,hourValue,baseName)
                            dimensionObj = Dimension()
                            dimensionObj.setDimension(
                                {
                                    "name": name,
                                    "sql" : sql,
                                    "type": type
                                }
                                )
                            print(dimensionObj)
                        elif re.search(r'minute\d+', timeframe):
                            rx = re.compile(r'minute(\d+)')
                            matches_raw = [match.group(1) for match in rx.finditer(timeframe)]
                            hourValue = None
                            for item in matches_raw:
                                hourValue = item
                            type = 'string'
                            sql="TO_CHAR(DATE_TRUNC('minute', DATE_TRUNC('minute', TO_TIMESTAMP((EXTRACT('EPOCH', CAST({}  AS TIMESTAMP_TZ)) - (EXTRACT('EPOCH', CAST({}  AS TIMESTAMP_TZ)) % (60*{})))))), 'YYYY-MM-DD HH24:MI')".format(baseName,baseName,hourValue)
                            dimensionObj = Dimension()
                            dimensionObj.setDimension(
                                {
                                    "name": name,
                                    "sql" : sql,
                                    "type": type
                                }
                                )
                            print(dimensionObj)
        
                        
                        print('Time Frames Name-----------------'+name)


                        dimension_ = Dimension()
                        dimension_.setDimension(
                            {
                                "name": name,
                                "sql" : sql,
                                "type": type
                            }
                        )

                        dimensionGroupList.append(dimension_)

                        #print('Time Frames-----------------'+timeframesRow)
                        #dimensiongroupObj = dimensiongroup()
                        #dimensiongroupObj.setdimensiongroup(timeframesRow)



        
        for dimensionItem in dimensionGroupList:
            dimensions_.append(dimensionItem)     

        allDimensions = Dimension().getProcessedSubstituteDimensions(dimensions_)

        validDimensions = []
        excludedDimensions = []

        for dimension_ in allDimensions:
            if not dimension_.isExcluded:
                validDimensions.append(dimension_)
            else:
                excludedDimensions.append(dimension_)

        self.allDimensions = allDimensions
        self.dimensions = validDimensions
        self.validDimensions = validDimensions
        self.excludedDimensions = excludedDimensions

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
                print("-------------------------------------")
                print(group)
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
            
            rx = re.compile(r'(\w+\(*\w+\s+)FROM\s+(\w+)', re.IGNORECASE)
            substitued = []
            for match in rx.finditer(processedSQL):
                rxExtract = re.compile(r'EXTRACT', re.IGNORECASE)
                group1 = match.group(1)
                extractFound = rxExtract.search(group1)
                if extractFound:
                    print("Skipping Extract: {}".format(group1))
                else:
                    group = match.group(2)
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
            print(parsed)
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




