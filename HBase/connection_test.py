import paramiko
import happybase

host = "ubuhama.wi.lehre.mosbach.dhbw.de"
username = "wi17mes"
password = "wi17mes"

ssh = paramiko.SSHClient(host, username=username, password=password)



connection = happybase.Connection('ubuhama.wi.lehre.mosbach.dhbw.de', 54698)

print(connection.tables())

connection.create_table('my_table3', 
{
    'cf1': dict(max_versions=10), 
    'cf2': dict(max_versions=1, block_cache_enabled=False), 
    'cf3': dict()
    }
)

print(connection.tables())