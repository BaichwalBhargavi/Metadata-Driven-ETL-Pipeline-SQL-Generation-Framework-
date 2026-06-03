class SQLBuilder:
    def __init__(self):
        self._columns ="*"
        self._table =""
        self._where =""
    
    def select(self, columns: list):
        self._columns = ", ".join(columns) 
        return self 

    def from_table(self, table: str):
        self._table = table
        return self

    def where(self , condition :str ):
        self._where = f"WHERE {condition}"
        return self

        
    def get_query(self)-> str:
        if not self._table:
            raise ValueError("FROM table must be specified") 
        
        query = f"SELECT {self._columns} FROM {self._table}"
        if self._where:
            query += f" {self._where}"
        return query 
    
    def __str__(self):
        return self.get_query()
    

