
from Parser import reestr_client


o = reestr_client.ReestrRequest()
print(o.basedir)
print(o.session.verify)
o.config()
print(o.session.verify)

o.get_data_from_reestr()
print(o.json_data["RawOlapSettings"]["lazyLoadOptions"]["limit"] )