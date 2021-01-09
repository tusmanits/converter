import looker_sdk

class Connection:
    def __init__(self, name):
        self.name = None
        self.databaseName = None
        self.databaseSchema = None
        self.connection = None
        self.getConnectionFromAPI(name)

    def getConnectionFromAPI(self, name):
        sdk = looker_sdk.init31('looker.ini')
        connection = sdk.connection(name, 'name, database, schema')

        if connection.name:
            self.name = connection.name        

        if connection.database:
            self.database = connection.database

        if connection.schema:
            self.schema = connection.schema

        self.connection = connection

    def __str__(self):
        return """
            Connection: ---------------------------------------------------------------------------------------------------------------
            Connection Name:    {name}
            Database:           {database}
            Schema:             {schema}
            ConnectionObj:      {connection}
            """.format(name = self.name, database = self.database, schema = self.schema, connection = self.connection)
