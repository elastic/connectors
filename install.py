import platform
import shutil
import subprocess
from enum import Enum

import distro

system = platform.system()

class SystemName(Enum):
    Linux = "linux"
    Darwin = "darwin"

if system.lower() == SystemName.Linux.value:
    if distro.id() == 'ubuntu' or shutil.which('apt'):  # Additional check for Ubuntu
        try:
            subprocess.run(['sudo', 'apt-get', 'update'], check=True)
            subprocess.run(['sudo', 'apt-get', 'install', '-y', '--no-install-recommends',
                            'gcc', 'heimdal-dev'], check=True)
            print("Package installation completed successfully.")
        except subprocess.CalledProcessError as exception:
            raise exception

    elif shutil.which('yum'):
        krb5_suuport_list = ("red", "almalinux", "rocky", "centos")
        try:
            if distro.id() == 'centos':
                subprocess.run(['sudo', 'yum', 'update', '-y'], check=True)    
            else:
                subprocess.run(['sudo', 'yum', 'update', '-y', '--nobest'], check=True)

            if distro.name(pretty=True).lower().startswith(krb5_suuport_list):
                subprocess.run(['sudo', 'yum', 'install', '-y', 'gcc', 'krb5-devel'], check=True)
            else:
                subprocess.run(['sudo', 'yum', 'install', '-y', 'gcc', 'heimdal-dev'], check=True)
            print("Package installation completed successfully.")
        except subprocess.CalledProcessError as exception:
            raise exception

    elif shutil.which('dnf'):
        try:
            subprocess.run(['sudo', 'dnf', 'update', '-y', '--nobest'], check=True)
            subprocess.run(['sudo', 'dnf', 'install', '-y', 'gcc', 'heimdal-dev'], check=True)
            print("Package installation completed successfully.")
        except subprocess.CalledProcessError as exception:
            raise exception

    else:
        print("Supported package managers are: yum, apt, dnf.")

elif system.lower() == SystemName.Darwin.value:
    if shutil.which('brew'):
        try:
            subprocess.run(['brew', 'update'], check=True)
            subprocess.run(['brew', 'install', 'gcc', 'heimdal'], check=True)
            print("Package installation completed successfully.")
        except subprocess.CalledProcessError as exception:
            raise exception
    else:
        print("Homebrew (brew) package manager not found. Please install Homebrew.")

else:
    print("System should be either Linux or Darwin")
