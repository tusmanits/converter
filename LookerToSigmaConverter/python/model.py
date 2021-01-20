import lkml
from connection import Connection
from logger import Logger
from view import View


class Model:
    def __init__(self):
        self.name = None
        self.connectionName = None
        self.connection = None
        self.label = None

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
            Label:                  {label}
            Name:                   {name}
            Connection Name:        {connectionName}
            Database Name:          {databaseName}
            SchemaName:             {schemaName}
            """.format(label = self.label, connectionName = self.connectionName, databaseName = self.connection.getDatabaseName(), schemaName = self.connection.getSchemaName(), name = self.name)

logging = Logger().getLogger()

with open('../data/its_sig/its_sig.model.lkml', 'r') as file:
    parsed = lkml.load(file)
    #logging.info(parsed)

    model = Model()
    model.setModel(parsed)
    logging.info(model)


    viewFile = '../data/its_sig/events_pdt.view.lkml'

    viewObj = View()
    views = viewObj.getViewInfomationFromFile(viewFile)

    for view in views:
        logging.info(view)

        logging.info(view.getViewSQL())

        view.schemaName = model.connection.schemaName
        view.databaseName = model.connection.databaseName
        view.targetSchema = model.name

        view.getTableNamesFromSQL()

        logging.info(view.dependencies)

        view.writedbtModel()

