from .reestr_client import ReestrRequest as _client 
#from .data_transformer import ReestrData as _data 


__version__ = '1.1'
#__all__ = ['client', 'data']

client = _client()
client.config()

#data = _data()




#from .module1 import SomeClass
#from .module2 import some_function
   