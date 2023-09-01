
from configparser import ConfigParser

config_file = ConfigParser()
config_file.read('Parser/config.ini')

for i in config_file.sections():
    print(i)

z = {k:v for i in config_file.sections() for k, v in config_file.items(i)}
print(z) 

var3 = z.get('test_var_x')
print(var3)

print(config_file['test'])
print(config_file['test']['test_var'])

var = config_file['test']['test_var'] or None

try:
    var1 = config_file['test']['test_var1'] or None
except KeyError:
    var1 = None
print(var, var1)