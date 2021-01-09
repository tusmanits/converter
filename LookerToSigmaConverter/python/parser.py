import lkml

import logging

import re

from collections import defaultdict 


class View:
    def __init__(self):
        self.name = ''
        self.sql = ''
        self.is_derived_table = False
        self.is_native_derived_table = False
        self.persist_for = ''
        self.sql_trigger_name = ''

    def setView(self, view):

        if 'derived_table' in view:
            self.is_derived_table = True

            if 'sql' in view['derived_table']:
                self.sql = view['derived_table']['sql']

            if 'persist_for' in view['derived_table']:
                self.persist_for = view['derived_table']['persist_for']

        if 'name' in view:
            self.name = view['name']

    def __str__(self):
        return """
            View: ---------------------------------------------------------------------------------------------------------------
            View Name:            {name}
            Is Derived Table:     {is_derived_table}
            Presist For:          {persist_for}
            SQL:                  {sql}
            """.format(name = self.name, is_derived_table = self.is_derived_table, sql = self.sql, persist_for = self.persist_for)


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

    def getDependencies(self):

        rx = re.compile(r'\$\{(\w+)\}')

        matches_raw = [match.group(1) for match in rx.finditer(self.sql)]

        dependencies = []

        for item in matches_raw:
            if item not in dependencies:
                if item != 'TABLE':
                    dependencies.append(item)

        return dependencies

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
            
            """.format(name = self.name, type = self.type, sql = self.sql, primary_key = self.primaryKey, hidden = self.hidden, dependencies = self.dependencies, sql_raw = self.sql_raw, dependenciesByPath = self.dependenciesByPath)

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




logging.basicConfig(filename='parser.log',level=logging.INFO, filemode='w', format = '%(asctime)s:%(levelname)s:%(message)s')


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


with open('./its_sig/repeat_purchase_fact.view.lkml', 'r') as file:
    parsed = lkml.load(file)
    logging.info(parsed)

    for view in parsed['views']:

        viewObj = View()

        viewObj.setView(view)

        logging.info(viewObj)

        

        if 'dimensions' in view:

            dimensionsObjs = []
            
            for dimensionRow in view['dimensions']:
                dimensionObj = Dimension()
                dimensionObj.setDimension(dimensionRow)

                logging.info(dimensionObj) 

                dimensionsObjs.append(dimensionObj)



        graph = dict()

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
                logging.info("Only I dimension")
                continue
            else:
                logging.info("All Dimensions")
                dimension_.getDependenciesByPath()
                for j in range(0, len(dependenciesByPath) - 2):
                    logging.info("Sub Dimenions")
                    sourceDimensionName = dependenciesByPath[j]
                    targetDimensionName = dependenciesByPath[j + 1]

                    targetDimension = dimension_.getDimensionByName(targetDimensionName, dimensionsObjs)
                    sourceDimension = dimension_.getDimensionByName(sourceDimensionName, dimensionsObjs)

                    targetDimension.substituteDimension(sourceDimension, dimensionsObjs)
                    index = targetDimension.getIndex(dimensionsObjs)
                    dimensionsObjs[index] = targetDimension



                    logging.info("substituteDimension: {}".format(targetDimension))

                #REPLACE ALL IN TARGET DIMENSION

                for name in dimension_.getDependencies():
                    sourceDimension = dimension_.getDimensionByName(name, dimensionsObjs)

                    dimension_.substituteDimension(sourceDimension, dimensionsObjs)
                    index = dimension_.getIndex(dimensionsObjs)
                    dimensionsObjs[index] = dimension_

















'''
        if view['derived_table']:

            sql = view['derived_table']['sql']
            logging.info(view)

            viewName = ''

            if 'name' in view:
                viewName = view['name']

            if 'sql' in view:
                viewSQL = view['sql']

            if 'presist_for' in view:
                presistFor = view['presist_for']

            



            if 'name' in view:
                viewName = view['name']                                
            if 'name' in view:
                viewName = view['name']                                
            if 'name' in view:
                viewName = view['name']                                
            if 'name' in view:
                viewName = view['name']                                

            View = v



        dimensions = view['dimensions']

        tree = Tree()
        tree.create_node("Dimensions", 0)

        for dimension in dimensions:
            name = ''
            type = ''
            primary_key = ''
            hidden = ''
            sql = ''
            rank = ''

            if 'name' in dimension:
                name = dimension['name']

            if 'type' in dimension:
                type = dimension['type']

            if 'primary_key' in dimension:
                primary_key = dimension['primary_key']

            if 'hidden' in dimension:
                hidden = dimension['hidden']

            if 'sql' in dimension:
                sql = dimension['sql']
                if sql.startswith('${TABLE}.'):
                    rank = '0'
                else:
                    rank = '1'

            rx = re.compile(r'\$\{(\w+)\}')
 
            matches_raw = [match.group(1) for match in rx.finditer(sql)]

            matches = []

            for item in matches_raw:
                if item not in matches:
                    matches.append(item)

            logging.info('Name: {name}, Children: {matches}'.format(name = name, matches = matches))

            #logging.info('Name: {name}, Type: {type}, Primary Key: {primary_key}, Hidden: {hidden}, SQL: {sql}, Rank: {rank}, Children: {f}'.format(
            #    name = name, type = type, primary_key = primary_key, hidden = hidden, sql = sql, rank = rank, f = matches
            #    ))

 '''    
        #tree.show()

 





                
