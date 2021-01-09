import lkml
from connection import Connection
 
class Model:
    def __init__(self):
        self.name = ''
        self.connection = ''
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



con = Connection('its_warehouse')
print(con)