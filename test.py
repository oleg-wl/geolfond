
from Parser import client

x = client.get_data_from_reestr('oil')

print(x[0][1])
print(x[0][2])