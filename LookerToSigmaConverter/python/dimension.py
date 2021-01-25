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

        if self.type == 'yesno':
            self.transformYesNoDiemension()

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