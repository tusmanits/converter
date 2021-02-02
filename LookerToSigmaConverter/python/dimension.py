import lkml
import re

class Dimension:
    def __init__(self):
        self.name = ''
        self.sql_raw = ''
        self.sql = ''
        self.type = ''
        self.hidden = ''
        self.primaryKey = ''
        self.dependencies= []
        self.column = ''
        self.isSubstituded = False
        self.dependenciesByPath = []
        self.isExcluded = False
        self.excludedReason = None

    def setExcludedDimension(self):
        found = re.search(r'\$\{\s*\w+\s*\.\s*\w+\s*\}', self.sql)
        if found:
            self.isExcluded = True
            self.excludedReason = "Using other view's dimensions"

    def getDependencies(self):

        rx = re.compile(r'\$\{(\w+)\}')

        matches_raw = [match.group(1) for match in rx.finditer(self.sql)]

        dependencies = []

        for item in matches_raw:
            if item not in dependencies:
                if item != 'TABLE':
                    dependencies.append(item)

        return dependencies

    def transformLocationDimension(self, sqlLogitute, sqlLatitute):
        return "{}||','||{}".format(sqlLogitute, sqlLatitute)

    def transformYesNoDiemension(self):
        self.sql = """
        (CASE 
            WHEN {} THEN TRUE 
            ELSE FALSE 
        END)""".format(self.sql)

    def transformTableDimensions(self):
        if '${TABLE}.' in self.sql:
            self.sql = self.sql.replace('${TABLE}.', '') 

    def transformZipCodeDimension(self):
        self.transformTableDimensions()
   
   
    def transformTierDimension(self, tiers ,style):
        print('----------------------------------------------')
        print('----------------------------------------------')
        print(tiers)
        print(style)
        print(self.sql)
        if tiers is not None:
            difference=int(tiers[1])-int(tiers[0])
            print(difference)
            finalquery=''
            i=00
            for value in tiers:
                print(value)
                if style=='integer':
                    diff_from_value=int(value)-int(difference)
                    minus_one_value=int(value)-1
                    if int(value)==0:
                        query="CASE WHEN {}  < {} THEN 'Below {}'".format(self.sql,value,value)
                        finalquery = '{} {}'.format(finalquery, query)
                        #queryF=query
                    if int(value)>0:
                        query="WHEN {}  >= {} AND {}  < {} THEN '{} to {}'".format(self.sql,diff_from_value,self.sql,value,diff_from_value,minus_one_value)
                        print(query)
                        finalquery = '{} {}'.format(finalquery, query)
                        #queryF=queryF
                    print(value)
                    
                    
                if style=='relational':
                    diff_from_value=float(value)-float(difference)
                    minus_one_value=float(value)
                    if int(value)==0:
                        query="CASE WHEN {}  < {} THEN '< {}'".format(self.sql,float(value),float(value))
                        finalquery = '{} {}'.format(finalquery, query)
                        #queryF=query
                    if int(value)>0:
                        query="WHEN {}  >= {} AND {}  < {} THEN '>={} and <{}'".format(self.sql,float(diff_from_value),self.sql,float(value),float(diff_from_value),float(minus_one_value))
                        print(query)
                        finalquery = '{} {}'.format(finalquery, query)
                        #queryF=queryF
                    print(value)

                
                if style=='classic':
                    
                    diff_from_value=float(value)-float(difference)
                    minus_one_value=float(value)
                    if int(value)==0:
                        query="CASE WHEN {}  < {} THEN 'T{:02d} (-inf,{})'".format(self.sql,float(value),i,float(value))
                        finalquery = '{} {}'.format(finalquery, query)
                        #queryF=query
                        print(query)
                    if int(value)>0:
                        query="WHEN {}  >= {} AND {}  < {} THEN 'T{:02d} [{},{})'".format(self.sql,float(diff_from_value),self.sql,float(value),i,float(diff_from_value),float(minus_one_value))
                        print(query)
                        finalquery = '{} {}'.format(finalquery, query)
                        #queryF=queryF
                    
                    #print(i)
                    print(value)

                if style=='interval':
                    
                    diff_from_value=float(value)-float(difference)
                    minus_one_value=float(value)
                    if int(value)==0:
                        query="CASE WHEN {}  < {} THEN '(-inf,{})'".format(self.sql,float(value),float(value))
                        finalquery = '{} {}'.format(finalquery, query)
                        #queryF=query
                        print(query)
                    if int(value)>0:
                        query="WHEN {}  >= {} AND {}  < {} THEN '[{},{})'".format(self.sql,float(diff_from_value),self.sql,float(value),float(diff_from_value),float(minus_one_value))
                        print(query)
                        finalquery = '{} {}'.format(finalquery, query)

                i=i+1
            #queryF=queryF
            if style == 'integer':
                query="WHEN {}  >= {} THEN '{} or Above' ELSE 'Undefined' END".format(self.sql,value,value)
                finalquery = '{} {}'.format(finalquery, query)
            elif style == 'relational':
                query="WHEN {}  >= {} THEN '>={}' ELSE 'Undefined' END".format(self.sql,float(value),float(value))
                finalquery = '{} {}'.format(finalquery, query)
            elif style == 'classic':
                query="WHEN {}  >= {} THEN 'T{:02d} [{},inf)' ELSE 'TXX Undefined' END".format(self.sql,float(value),i,float(value))
                finalquery = '{} {}'.format(finalquery, query)
            elif style == 'interval':
                query="WHEN {}  >= {} THEN '[{},inf)' ELSE 'Undefined' END".format(self.sql,float(value),float(value))
                finalquery = '{} {}'.format(finalquery, query)
        #print(str(finalquery))
        print(finalquery)


    def duration_day(self, sql_start, sql_end):
        sql_start=sql_start
        sql_end=sql_end
        #print("Duration Day Function"+sql_start+sql_end)
        type = 'string'
        query = "(TIMESTAMPDIFF(DAY, {} , {}) + CASE WHEN TIMESTAMPDIFF(SECOND, TO_DATE({}), {}) = TIMESTAMPDIFF(SECOND, TO_DATE({} ), {} ) THEN 0 WHEN TIMESTAMPDIFF(SECOND, TO_DATE({}), {}) < TIMESTAMPDIFF(SECOND, TO_DATE({} ), {} ) THEN CASE WHEN {}  < {} THEN -1 ELSE 0 END ELSE CASE WHEN {}  > {} THEN 1 ELSE 0 END END)".format(sql_start,sql_end,sql_end,sql_end,sql_start,sql_start,sql_end,sql_end,sql_start,sql_start,sql_start,sql_end,sql_start,sql_end)
        print(query)
        self.sql = query
    def duration_hour(self, sql_start, sql_end):
        sql_start=sql_start
        sql_end=sql_end
        #print("Duration Day Function"+sql_start+sql_end)
        type = 'string'
        query = "CASE WHEN TIMESTAMPDIFF(SECOND, {created_at} , {delivered_at}) / (60*60) < 0 THEN CEIL(TIMESTAMPDIFF(SECOND, {created_at} , {delivered_at}) / (60*60)) ELSE FLOOR(TIMESTAMPDIFF(SECOND, {created_at} , {delivered_at}) / (60*60)) END".format(created_at=sql_start,delivered_at=sql_end)
        print(query)
        self.sql = query
    def duration_second(self, sql_start, sql_end):
        sql_start=sql_start
        sql_end=sql_end
        #print("Duration Day Function"+sql_start+sql_end)
        type = 'string'
        query = "TIMESTAMPDIFF(SECOND, {created_at} , {delivered_at})".format(created_at=sql_start,delivered_at=sql_end)
        print(query)
        self.sql = query
    def duration_minute(self, sql_start, sql_end):
        sql_start=sql_start
        sql_end=sql_end
        #print("Duration Day Function"+sql_start+sql_end)
        type = 'string'
        query = "CASE WHEN TIMESTAMPDIFF(SECOND, {created_at} , {delivered_at}) / 60 < 0 THEN CEIL(TIMESTAMPDIFF(SECOND, {created_at} , {delivered_at}) / 60) ELSE FLOOR(TIMESTAMPDIFF(SECOND, {created_at} , {delivered_at}) / 60) END".format(created_at=sql_start,delivered_at=sql_end)
        print(query)
        self.sql = query
    def duration_month(self, sql_start, sql_end):
        sql_start=sql_start
        sql_end=sql_end
        #print("Duration Day Function"+sql_start+sql_end)
        type = 'string'
        query = "(TIMESTAMPDIFF(MONTH, {created_at} , {delivered_at}) + CASE WHEN TIMESTAMPDIFF(SECOND, DATE_TRUNC('month', {delivered_at}), {delivered_at}) = TIMESTAMPDIFF(SECOND, DATE_TRUNC('month', {created_at} ), {created_at} ) THEN 0 WHEN TIMESTAMPDIFF(SECOND, DATE_TRUNC('month', {delivered_at}), {delivered_at}) < TIMESTAMPDIFF(SECOND, DATE_TRUNC('month', {created_at} ), {created_at} ) THEN CASE WHEN {created_at}  < {delivered_at} THEN -1 ELSE 0 END ELSE CASE WHEN {created_at}  > {delivered_at} THEN 1 ELSE 0 END END)".format(created_at=sql_start,delivered_at=sql_end)
        print(query)
        self.sql = query
    def duration_quarter(self, sql_start, sql_end):
        sql_start=sql_start
        sql_end=sql_end
        #print("Duration Day Function"+sql_start+sql_end)
        type = 'string'
        query = "CASE WHEN (TIMESTAMPDIFF(MONTH, {created_at} , {delivered_at}) + CASE WHEN TIMESTAMPDIFF(SECOND, DATE_TRUNC('month', {delivered_at}), {delivered_at}) = TIMESTAMPDIFF(SECOND, DATE_TRUNC('month', {created_at} ), {created_at} ) THEN 0 WHEN TIMESTAMPDIFF(SECOND, DATE_TRUNC('month', {delivered_at}), {delivered_at}) < TIMESTAMPDIFF(SECOND, DATE_TRUNC('month', {created_at} ), {created_at} ) THEN CASE WHEN {created_at}  < {delivered_at} THEN -1 ELSE 0 END ELSE CASE WHEN {created_at}  > {delivered_at} THEN 1 ELSE 0 END END) / 3 < 0 THEN CEIL((TIMESTAMPDIFF(MONTH, {created_at} , {delivered_at}) + CASE WHEN TIMESTAMPDIFF(SECOND, DATE_TRUNC('month', {delivered_at}), {delivered_at}) = TIMESTAMPDIFF(SECOND, DATE_TRUNC('month', {created_at} ), {created_at} ) THEN 0 WHEN TIMESTAMPDIFF(SECOND, DATE_TRUNC('month', {delivered_at}), {delivered_at}) < TIMESTAMPDIFF(SECOND, DATE_TRUNC('month', {created_at} ), {created_at} ) THEN CASE WHEN {created_at}  < {delivered_at} THEN -1 ELSE 0 END ELSE CASE WHEN {created_at}  > {delivered_at} THEN 1 ELSE 0 END END) / 3) ELSE FLOOR((TIMESTAMPDIFF(MONTH, {created_at} , {delivered_at}) + CASE WHEN TIMESTAMPDIFF(SECOND, DATE_TRUNC('month', {delivered_at}), {delivered_at}) = TIMESTAMPDIFF(SECOND, DATE_TRUNC('month', {created_at} ), {created_at} ) THEN 0 WHEN TIMESTAMPDIFF(SECOND, DATE_TRUNC('month', {delivered_at}), {delivered_at}) < TIMESTAMPDIFF(SECOND, DATE_TRUNC('month', {created_at} ), {created_at} ) THEN CASE WHEN {created_at}  < {delivered_at} THEN -1 ELSE 0 END ELSE CASE WHEN {created_at}  > {delivered_at} THEN 1 ELSE 0 END END) / 3) END".format(created_at=sql_start,delivered_at=sql_end)
        print(query)
        self.sql = query
    def duration_weeks(self, sql_start, sql_end):
        sql_start=sql_start
        sql_end=sql_end
        #print("Duration Day Function"+sql_start+sql_end)
        type = 'string'
        query = "CASE WHEN (TIMESTAMPDIFF(DAY, {created_at} , {delivered_at}) + CASE WHEN TIMESTAMPDIFF(SECOND, TO_DATE({delivered_at}), {delivered_at}) = TIMESTAMPDIFF(SECOND, TO_DATE({created_at} ), {created_at} ) THEN 0 WHEN TIMESTAMPDIFF(SECOND, TO_DATE({delivered_at}), {delivered_at}) < TIMESTAMPDIFF(SECOND, TO_DATE({created_at} ), {created_at} ) THEN CASE WHEN {created_at}  < {delivered_at} THEN -1 ELSE 0 END ELSE CASE WHEN {created_at}  > {delivered_at} THEN 1 ELSE 0 END END) / 7 < 0 THEN CEIL((TIMESTAMPDIFF(DAY, {created_at} , {delivered_at}) + CASE WHEN TIMESTAMPDIFF(SECOND, TO_DATE({delivered_at}), {delivered_at}) = TIMESTAMPDIFF(SECOND, TO_DATE({created_at} ), {created_at} ) THEN 0 WHEN TIMESTAMPDIFF(SECOND, TO_DATE({delivered_at}), {delivered_at}) < TIMESTAMPDIFF(SECOND, TO_DATE({created_at} ), {created_at} ) THEN CASE WHEN {created_at}  < {delivered_at} THEN -1 ELSE 0 END ELSE CASE WHEN {created_at}  > {delivered_at} THEN 1 ELSE 0 END END) / 7) ELSE FLOOR((TIMESTAMPDIFF(DAY, {created_at} , {delivered_at}) + CASE WHEN TIMESTAMPDIFF(SECOND, TO_DATE({delivered_at}), {delivered_at}) = TIMESTAMPDIFF(SECOND, TO_DATE({created_at} ), {created_at} ) THEN 0 WHEN TIMESTAMPDIFF(SECOND, TO_DATE({delivered_at}), {delivered_at}) < TIMESTAMPDIFF(SECOND, TO_DATE({created_at} ), {created_at} ) THEN CASE WHEN {created_at}  < {delivered_at} THEN -1 ELSE 0 END ELSE CASE WHEN {created_at}  > {delivered_at} THEN 1 ELSE 0 END END) / 7) END".format(created_at=sql_start,delivered_at=sql_end)
        print(query)
        self.sql = query
    def duration_years(self, sql_start, sql_end):
        sql_start=sql_start
        sql_end=sql_end
        #print("Duration Day Function"+sql_start+sql_end)
        type = 'string'
        query = "CASE WHEN (TIMESTAMPDIFF(MONTH, {created_at} , {delivered_at}) + CASE WHEN TIMESTAMPDIFF(SECOND, DATE_TRUNC('month', {delivered_at}), {delivered_at}) = TIMESTAMPDIFF(SECOND, DATE_TRUNC('month', {created_at} ), {created_at} ) THEN 0 WHEN TIMESTAMPDIFF(SECOND, DATE_TRUNC('month', {delivered_at}), {delivered_at}) < TIMESTAMPDIFF(SECOND, DATE_TRUNC('month', {created_at} ), {created_at} ) THEN CASE WHEN {created_at}  < {delivered_at} THEN -1 ELSE 0 END ELSE CASE WHEN {created_at}  > {delivered_at} THEN 1 ELSE 0 END END) / 12 < 0 THEN CEIL((TIMESTAMPDIFF(MONTH, {created_at} , {delivered_at}) + CASE WHEN TIMESTAMPDIFF(SECOND, DATE_TRUNC('month', {delivered_at}), {delivered_at}) = TIMESTAMPDIFF(SECOND, DATE_TRUNC('month', {created_at} ), {created_at} ) THEN 0 WHEN TIMESTAMPDIFF(SECOND, DATE_TRUNC('month', {delivered_at}), {delivered_at}) < TIMESTAMPDIFF(SECOND, DATE_TRUNC('month', {created_at} ), {created_at} ) THEN CASE WHEN {created_at}  < {delivered_at} THEN -1 ELSE 0 END ELSE CASE WHEN {created_at}  > {delivered_at} THEN 1 ELSE 0 END END) / 12) ELSE FLOOR((TIMESTAMPDIFF(MONTH, {created_at} , {delivered_at}) + CASE WHEN TIMESTAMPDIFF(SECOND, DATE_TRUNC('month', {delivered_at}), {delivered_at}) = TIMESTAMPDIFF(SECOND, DATE_TRUNC('month', {created_at} ), {created_at} ) THEN 0 WHEN TIMESTAMPDIFF(SECOND, DATE_TRUNC('month', {delivered_at}), {delivered_at}) < TIMESTAMPDIFF(SECOND, DATE_TRUNC('month', {created_at} ), {created_at} ) THEN CASE WHEN {created_at}  < {delivered_at} THEN -1 ELSE 0 END ELSE CASE WHEN {created_at}  > {delivered_at} THEN 1 ELSE 0 END END) / 12) END".format(created_at=sql_start,delivered_at=sql_end)
        print(query)
        self.sql = query


    def setDimension(self, dimension):

        if 'name' in dimension:
            self.name = dimension['name']

        if 'sql' in dimension:
            self.sql_raw = dimension['sql']

        if 'type' in dimension:
            self.type = dimension['type']

        if 'hidden' in dimension:
            self.hidden = dimension['hidden']

        if 'primary_key' in dimension:
            self.primary_key = dimension['primary_key']

        self.sql = self.sql_raw

        if self.type == 'location':
            if 'sql_latitude' in dimension:
                sqlLatitude = dimension['sql_latitude']
            if 'sql_longitude' in dimension:
                sqlLongitude = dimension['sql_longitude']
            self.sql = self.transformLocationDimension(sqlLongitude, sqlLatitude)
            

        self.transformTableDimensions()

        if self.type == 'zipcode':
            self.transformZipCodeDimension()
        if self.type == 'tier':
            tiers = None
            if 'tiers' in dimension:
                tiers = dimension['tiers']
                style = dimension['style']
                self.transformTierDimension(tiers,style)

        if self.type == 'yesno':
            self.transformYesNoDiemension()

        if 'duration' in self.type:
            print("Duration----------------------------------------------------------------------------------------"+self.type)
            if 'day' in self.type:
                sql_start = dimension['sql_start']
                sql_end = dimension['sql_end']
                #print("&&&&&&&&&&&&&&&&&sql_start:"+sql_start+", sql_end:"+sql_end)
                self.duration_day(sql_start,sql_end)
            elif 'hour' in self.type:
                sql_start = dimension['sql_start']
                sql_end = dimension['sql_end']
                #print("&&&&&&&&&&&&&&&&&sql_start:"+sql_start+", sql_end:"+sql_end)
                self.duration_hour(sql_start,sql_end)
            elif 'minute' in self.type:
                sql_start = dimension['sql_start']
                sql_end = dimension['sql_end']
                #print("&&&&&&&&&&&&&&&&&sql_start:"+sql_start+", sql_end:"+sql_end)
                self.duration_minute(sql_start,sql_end)
            elif 'month' in self.type:
                sql_start = dimension['sql_start']
                sql_end = dimension['sql_end']
                #print("&&&&&&&&&&&&&&&&&sql_start:"+sql_start+", sql_end:"+sql_end)
                self.duration_month(sql_start,sql_end)
            elif 'quarter' in self.type:
                sql_start = dimension['sql_start']
                sql_end = dimension['sql_end']
                #print("&&&&&&&&&&&&&&&&&sql_start:"+sql_start+", sql_end:"+sql_end)
                self.duration_quarter(sql_start,sql_end)
            elif 'week' in self.type:
                sql_start = dimension['sql_start']
                sql_end = dimension['sql_end']
                #print("&&&&&&&&&&&&&&&&&sql_start:"+sql_start+", sql_end:"+sql_end)
                self.duration_weeks(sql_start,sql_end)
            elif 'year' in self.type:
                sql_start = dimension['sql_start']
                sql_end = dimension['sql_end']
                #print("&&&&&&&&&&&&&&&&&sql_start:"+sql_start+", sql_end:"+sql_end)
                self.duration_years(sql_start,sql_end)
            elif 'second' in self.type:
                sql_start = dimension['sql_start']
                sql_end = dimension['sql_end']
                #print("&&&&&&&&&&&&&&&&&sql_start:"+sql_start+", sql_end:"+sql_end)
                self.duration_second(sql_start,sql_end)
                

                

        self.dependencies = self.getDependencies()
        

    def getIndex(self, dimensions):
        
        index = 0
        
        for dimension_ in dimensions:
            if self.getDimensionName() == dimension_.getDimensionName():
                break
            else:
                index = index + 1
        return index

    def updateDimensionAtIndex(self, dimension, dimensions, index):

        dimensions_ = dimensions
        
        if index >= 0 and index < len(dimensions):
            dimensions_[index] = dimension

        return dimensions_


    def __str__(self):
        return """
            Dimension: --------------------------------------------------------------------------------------------------------
            Name:               {name}
            Type:               {type}
            Hidden:             {hidden}
            Primary Key:        {primary_key}
            Dependencies:       {dependencies}
            DependenciesByPath: {dependenciesByPath}
            SQL RAW:            {sql_raw}
            SQL:                {sql}
            IsExcluded :        {isExcluded}
            ExcludedReason :    {excludedReason}
            """.format(name = self.name, type = self.type, sql = self.sql, primary_key = self.primaryKey, hidden = self.hidden, dependencies = self.dependencies, sql_raw = self.sql_raw, dependenciesByPath = self.dependenciesByPath, isExcluded = self.isExcluded, excludedReason = self.excludedReason)

    def getDimensionName(self):
        return self.name

    def getDimensionByName(self, name, dimensions):

        dimension_ = None
        for dimension in dimensions:
            if dimension.getDimensionName() == name:
                dimension_ = dimension
        return dimension_

    def setDependenciesByPath(self, dependenciesByPath):

        self.dependenciesByPath = dependenciesByPath

    def getDependenciesByPath(self):
        return self.dependenciesByPath

    def getSQL(self):
        return self.sql

    def substituteDimension(self, sourceDimension, dimensions):
        sourceName = sourceDimension.getDimensionName()
        sourceSQL = sourceDimension.getSQL()

        sourceDimensionPlaceHolder = r'${' + sourceName +'}'


        self.sql = self.sql.replace(sourceDimensionPlaceHolder, sourceSQL)

    def getProcessedSubstituteDimensions(self, dimenions):
        graph = dict()

        dimensionsObjs = dimenions

        for dimension_ in dimensionsObjs:
            dimName_ = dimension_.getDimensionName()
            dimDep_ = dimension_.getDependencies()

            item = {
                dimName_ : dimDep_
            }

            graph.update(item)


        for dimension_ in dimensionsObjs:
            dimName_ = dimension_.getDimensionName()
            path = findPath(graph, dimName_)

            path.reverse()

            dimension_.setDependenciesByPath(path)

        for i in range(0, len(dimensionsObjs)):
            dimension_ = dimensionsObjs[i]
            dimName = dimension_.getDimensionName()
            dependenciesByPath = dimension_.getDependenciesByPath()

            if len(dependenciesByPath) == 1 and dimName == dependenciesByPath[0]:
                #logging.info("Only I dimension")
                continue
            else:
                #logging.info("All Dimensions")
                dimension_.getDependenciesByPath()
                for j in range(0, len(dependenciesByPath) - 2):
                    #logging.info("Sub Dimenions")
                    sourceDimensionName = dependenciesByPath[j]
                    targetDimensionName = dependenciesByPath[j + 1]

                    targetDimension = dimension_.getDimensionByName(targetDimensionName, dimensionsObjs)
                    sourceDimension = dimension_.getDimensionByName(sourceDimensionName, dimensionsObjs)

                    targetDimension.substituteDimension(sourceDimension, dimensionsObjs)
                    index = targetDimension.getIndex(dimensionsObjs)
                    dimensionsObjs[index] = targetDimension



                    #logging.info("substituteDimension: {}".format(targetDimension))

                #REPLACE ALL IN TARGET DIMENSION

                for name in dimension_.getDependencies():
                    sourceDimension = dimension_.getDimensionByName(name, dimensionsObjs)

                    dimension_.substituteDimension(sourceDimension, dimensionsObjs)
                    index = dimension_.getIndex(dimensionsObjs)
                    dimensionsObjs[index] = dimension_


        for dimension in dimensionsObjs:
            dimension.setExcludedDimension()

        for dimension in dimensionsObjs:
            dependencies = dimension.getDependencies()
            for dependencyItem in dependencies:
                dimension_ = dimension.getDimensionByName(dependencyItem, dimensionsObjs)
                if dimension_.isExcluded:
                    dimension.isExcluded = True
                    dimension.excludedReason = "Referencing to a excluded dimension."

        return dimensionsObjs





def findPath(graph, start, path=[]): 
    path = path + [start]

    if len(graph[start]) == 0: 
        return path
    else: 
        for node in graph[start]: 
            if node not in path: 
                newpath = findPath(graph, node, path) 
                if newpath: 
                    return newpath