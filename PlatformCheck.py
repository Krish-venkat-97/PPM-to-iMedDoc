#import pyodbc
import platform
#print(pyodbc.drivers())
#print(platform.architecture())
print(f"Running in: {platform.architecture()[0]} Python")