import subprocess



with open('connectors/VERSION') as f:
    version = f.read()

version = version.split('.')

branch = f'{version[0]}.{version[1]}'

branches = [line.strip().decode() for line in
            subprocess.check_output('git branch', shell=True).split(b'\n')
            if line.strip() != b'']

if branch in branches:
    print(branch)
else:
    print('main')
