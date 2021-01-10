import looker_sdk

class Connection:
    def __init__(self, name):
        self.name = None
        self.databaseName = None
        self.schemaName = None
        self.connection = None
        self.getConnectionFromAPI(name)

    def getConnectionFromAPI(self, name):
        sdk = looker_sdk.init31('looker.ini')
        connection = sdk.connection(name, 'name, database, schema')

        if connection.name:
            self.name = connection.name        

        if connection.database:
            self.databaseName = connection.database

        if connection.schema:
            self.schemaName = connection.schema

        self.connection = connection

    def getDatabaseName(self):
        return self.databaseName

    def getSchemaName(self):
        return self.schemaName

    def __str__(self):
        return """
            Connection: ---------------------------------------------------------------------------------------------------------------
            Connection Name:    {name}
            Database:           {database}
            Schema:             {schema}
            ConnectionObj:      {connection}
            """.format(name = self.name, database = self.databaseName, schema = self.schemaName, connection = self.connection)



