import lkml
from connection import Connection
from logger import Logger
from view import View
import os
import re

class Model:
    def __init__(self):
        self.name = None
        self.connectionName = None
        self.connection = None
        self.label = None
        self.includes = None

    def setModel(self, model):

        if 'label' in model:
            self.label = model['label']
            self.name = "SIGMA_" + self.label.replace(' ', '_')

        if 'includes' in model:
            self.includes = model['includes']

        if 'connection' in model:
            self.connectionName = model['connection']

            self.connection = Connection(self.connectionName)

    def __str__(self):
        return """
            Model: ---------------------------------------------------------------------------------------------------------------
            Label               :        {label}
            Name                :        {name}
            Connection Name     :        {connectionName}
            Database Name       :        {databaseName}
            SchemaName          :        {schemaName}
            includes            :        {includes}
            """.format(label = self.label, connectionName = self.connectionName, databaseName = self.connection.getDatabaseName(), schemaName = self.connection.getSchemaName(), name = self.name, includes = self.includes)

logging = Logger().getLogger()


def getFiles(dir, filesIncluded):
    filesList = []
    for dirName, subdirList, fileList in os.walk(dir):
        for fname in fileList:
            for fileIncluded in filesIncluded:
                fileIncluded = fileIncluded.replace('*.', '.*.')
                rx = re.compile(fileIncluded)
                filePath = '{}{}'.format(dirName, fname)
                if rx.match(filePath):    
                    modelFilesDict = {
                        "FileName" : fname,
                        "DirName"  : dirName,
                    }

                    filesList.append(modelFilesDict)

    return filesList


def main():
    rootDir = '../data/its_sig/'
    filesIncluded = ['*.model.lkml']
    modelFilesList = getFiles(rootDir, filesIncluded)

    logging.info(modelFilesList)

    for modelFileItem in modelFilesList:
        with open('{}{}'.format(modelFileItem["DirName"], modelFileItem["FileName"]), 'r') as modelFile:
            modelParsed = lkml.load(modelFile)
            
            model = Model()
            model.setModel(modelParsed)
            logging.info(model)
            #print(model)


            viewList = []

            rootDir = modelFileItem["DirName"]
            filesIncluded = model.includes
            viewFilesList = getFiles(rootDir, filesIncluded)

            logging.info('View----------------------------------')
            logging.info(viewFilesList)
            for viewFileItem in viewFilesList:

                if viewFileItem['FileName'] != 'inventory_items.view.lkml':
                    continue

                viewFile = '{}{}'.format(viewFileItem["DirName"], viewFileItem["FileName"])
                msg = "Parsing: {}".format(viewFile)
                logging.info(msg)
                print(msg)
                
                viewObj = View()
                views = viewObj.getViewInfomationFromFile(viewFile)

                for view in views:
                    logging.info("Viewinfo")
                    logging.info(view)
                    
                    view.schemaName = model.connection.schemaName
                    view.databaseName = model.connection.databaseName
                    view.targetSchema = model.name

                    logging.info("-------------------------All Dimensions---------------------------------------------")

                    for dimension_ in view.allDimensions:
                        logging.info(dimension_)

                    logging.info("-------------------------Valid Dimensions---------------------------------------------")

                    for dimension_ in view.validDimensions:
                        logging.info(dimension_)
                    
                    logging.info("-------------------------Invalid Dimensions---------------------------------------------")
                    for dimension_ in view.excludedDimensions:
                        logging.info(dimension_)
                    
                    view.getViewSQL()

                    view.injectViewSchema()

                    view.setDBTModelName()

                    viewList.append(view)

                for view in viewList:
                    view.injectSqlTableName(viewList)
                    view.injectSqlTableNameInSQLTriggerValue(viewList)
                    view.writedbtModel()



if __name__ == "__main__":
    main()


'''


with open('../data/its_sig/its_sig.model.lkml', 'r') as file:
    parsed = lkml.load(file)
    #logging.info(parsed)

    model = Model()
    model.setModel(parsed)
    logging.info(model)

    viewList = []

    viewFile = '../data/its_sig/events_pdt.view.lkml'

    viewObj = View()
    views = viewObj.getViewInfomationFromFile(viewFile)

    for view in views:
        logging.info(view)

        logging.info(view.getViewSQL())

        view.schemaName = model.connection.schemaName
        view.databaseName = model.connection.databaseName
        view.targetSchema = model.name

        view.injectViewSchema()

        view.setDBTModelName()

        viewList.append(view)  

    #view.writedbtModel()


    for view in viewList:
        view.injectSqlTableName(viewList)
        view.injectSqlTableNameInSQLTriggerValue(viewList)
        view.writedbtModel()


'''