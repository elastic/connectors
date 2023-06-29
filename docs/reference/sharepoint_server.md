# SharePoint Server Connector Reference

The [Elastic SharePoint Server connector](../connectors/sources/sharepoint_server.py) is provided in the Elastic connectors python framework and can be used via [build a connector](https://www.elastic.co/guide/en/enterprise-search/current/build-connector.html).

## Availability and prerequisites

This connector is available as a **connector client** from the **Python connectors framework**. To use this connector, satisfy all [connector client requirements](https://www.elastic.co/guide/en/enterprise-search/master/build-connector.html).

This connector is in **technical preview** and is subject to change. The design and code is less mature than official GA features and is being provided as-is with no warranties. Beta features are not subject to the support SLA of official GA features.

**Note** If you are facing 401 Unauthorized error while running the connector with kerberos based authentication then try running kinit to renew the ticket. Ticket expires after 24 hours. You need to run kinit to renew the ticket.

## Usage

To use this connector as a **connector client**, use the **build a connector** workflow. See [Connector clients and frameworks](https://www.elastic.co/guide/en/enterprise-search/master/build-connector.html).

For additional operations, see [Usage](https://www.elastic.co/guide/en/enterprise-search/master/connectors-usage.html).

## Compatibility

SharePoint Server are supported by the connector.

For SharePoint Server, below mentioned versions are compatible with Elastic connector frameworks:
- SharePoint 2013
- SharePoint 2016
- SharePoint 2019

## Kerberos Server Authentication Setup:
### Kerberos setup on server side
`Step1:` Installing and configuring MIT Kerberos for Windows

For a 64-bit machine, use the following download link from the MIT Kerberos
Website: http://web.mit.edu/kerberos/dist/kfw/4.0/kfw-4.0.1-amd64.msi.

To run the installer, double-click the .msi file that you downloaded above.
Follow the instructions in the installer to complete the installation process.
When the installation completes, click Finish.

`Step2:` Setting up the Kerberos configuration file

Settings for Kerberos are specified through a configuration file. You can set up the configuration file as an .ini file in the default location, which is the C:\ProgramData\MIT\Kerberos5 directory.

which is the C:\ProgramData\MIT\Kerberos5 directory. Normally, the C:\ProgramData\MIT\Kerberos5 directory is hidden.

Procedure:

1. Obtain a krb5.conf configuration file. You can obtain this file from the /etc/krb5.conf folder on the machine.
2. Rename the configuration file from krb5.conf to krb5.ini.
3. Copy the krb5.ini file to the C:\ProgramData\MIT\Kerberos5 directory and overwrite the empty sample file.

`Step3:` Create Kerberos ticket.
1. Either create a new ticket using MIT kerberos ticket or try running kinit on command prompt “Run as” Administrator.
2. Once a ticket has been created, you can verify it by running klist.

`Step4:` Setting up the Kerberos credential cache file.

1. Create a directory where you want to save the Kerberos credential cache file..
2. Open the System window:
    - If you are using Windows 8 or later, right-click This PC on the Start screen, and then click Properties.
3. Click Advanced System Settings.
4. In the System Properties dialog box, click the Advanced tab and then click Environment Variables.
5. In the Environment Variables dialog box, under the System Variables list, click New.
6. In the New System Variable dialog box, in the Variable Name field, type KRB5CCNAME.
7. In the Variable Value field, type the path to the folder you created above, and then append the file name krb5cache.

    **Note:** krb5cache is a file (not a directory) that is managed by the Kerberos software, and it should not be created by the user. If you receive a permission error when you first use Kerberos, make sure that the krb5cache file does not already exist as a file or a directory.
8. Click OK to save the new variable.
9. Make sure that the variable appears in the System Variables list.
10. Click OK to close the Environment Variables dialog box, and then click OK to close the System Properties dialog box.
11. To make sure that Kerberos uses the new settings, restart your machine.


`Step5:` Configuring Kerberos Authentication On Share Point
Create new web application with Kerberos Authentication. While creating the web application set the authentication to “Negotiate Kerberos”


`Step6:` DNS Name Resolution
1. Open DNS Management in Administrative Tools on a DNS Server.
2. Expand "Forward Lookup Zones" container.
3. Right click on your domain and click on "New Host (A or AAAA)".
4. You will see the below screen for entering the new Host details.
5. Enter your Host Name - this is the URL of the web application (minus the domain part in a FQDN) and type in the IP address of Kerberos Server-. Check the above screen.
6. Click on "Add Host".
7. Click on "Done".
8. You will see the confirmation dialog box. Click OK to close the dialog box.
9. Verify that the A Record is created in the right pane with correct Host name and IP address.
10. (optional) Flush the DNS cache. Enter Ipconfig -flushdns

`Step7:` Service Principal Name (SPN)
1. Open command prompt “Run as” Administrator.
2. Register SPNs for Host Name on Application Pool Service Account.
    - ```
        setspn -S HTTP/spn host ip <space>App pool service account
        Example: setspn -S HTTP/10.50.3.24 administrator
      ```
    - ```
        setspn -S HTTP/<spn host ip>:<port> <space>App pool service account
        Example: setspn -S HTTP/10.50.3.24:8991 administrator
      ```
3. (Important command) Now, register SPN for the Full Qualified Domain Name (FQDN) also.

    - ```
        setspn -S HTTP/Full qualified domain name <space>App pool service account
        Example: setspn -S HTTP/ECSharPoint00.ECSP0000.local administrator
        ```
    - ```
        setspn -S HTTP/<spn hostname> <space>App pool service account
        Example: setspn -S HTTP/ECSharPoint00 administrator
        ```
4. setspn -L<space>App pool service account
    - ```
	    Example: setspn -L HTTP/10.50.3.24 administrator
        ```

`Step8:` Allow Trust for delegation
1. Go to Start -> Administrative Tools.
2. Open Active Directory Users and Computers.
3. Expand your Fully Qualified Domain Name on left panel. Click on Computers folder. If in case you do not find your Server listed, then right click on your Fully Qualified Domain Name and click on Find and search Computer Name.
4. Right click on the Server where you need to trust your Server for delegating the services.
5. Click on Properties.
6. On the Delegation tab, select "Trust this computer for delegation to any service ( Kerberos only) option.
7. Click OK.

### Kerberos Client Setup on Linux:
**Note:** The client to be joined with SharePoint domain
1. Use “nmtui” command to make changes in network configuration and add sharepoint Server IP there on first priority.
    - vi /etc/hosts 
    - Add sharepoint server hosts in your vm hosts.
    - set hostname
        - ```
            Example: hostnamectl set-hostname <name of your host>
            ```
    - run `bash` in your command line to change the hostname
2. Install: 
    - yum install realmd oddjob oddjob-mkhomedir
    - yum install realmd sssd 
    - yum install PackageKit
    - yum -y install krb5-workstation sssd pam_krb5
3. To join client with the domain:
    realm join --user=<username> >domainname> Examples:
    - realm join --user =administrator ECSP33147.local
4. To verify that client joined with domain you can run: `realm list`
5. vi /etc/krb5.conf	[ Note: Update the configuration file as per the requirements ]
6. useradd administrator
7. kinit <username>@<domainname>
  **Note** This command needs to run on daily basis because this will expires after 24 hours
8. Verify the generated ticket using command: `klist`


## Configuration

The following configuration fields need to be provided for setting up the connector:

#### `authentication_type`

Determines the SharePoint Server authentication type.

#### `username`

The username of the account for SharePoint Server.

#### `password`

The password of the account to be used for the SharePoint Server.

#### `host_url`

The server host url where the SharePoint Server is hosted. Examples:

- `https://192.158.1.38:8080`

#### `site_collections`

The site collections to fetch sites from SharePoint Server(allow comma separated collections also). Examples:
- `collection1`
- `collection1, collection2`

#### `ssl_enabled`

Whether SSL verification will be enabled. Default value is `False`.

#### `ssl_ca`

Content of SSL certificate needed for SharePoint Server.

**Note:** Keep this field empty, if `ssl_enabled` is set to `False` (Applicable on SharePoint Server only). Example certificate:


- ```
    -----BEGIN CERTIFICATE-----
    MIID+jCCAuKgAwIBAgIGAJJMzlxLMA0GCSqGSIb3DQEBCwUAMHoxCzAJBgNVBAYT
    ...
    AlVTMQwwCgYDVQQKEwNJQk0xFjAUBgNVBAsTDURlZmF1bHROb2RlMDExFjAUBgNV
    -----END CERTIFICATE-----
    ```

#### `retry_count`

The number of retry attempts after failed request to the SharePoint Server. Default value is `3`.

## Documents and syncs
The connector syncs the following SharePoint Server object types:
- Sites and Subsites
- Lists
- List Items and its attachment content
- Document Libraries and its attachment content(include Web Pages)

## Sync rules

- Content of files bigger than 10 MB won't be extracted.
- Permissions are not synced. **All documents** indexed to an Elastic deployment will be visible to **all users with access** to that Elasticsearch Index.
- Filtering rules are not available in the present version. Currently filtering is controlled via ingest pipelines.

## E2E Tests

The connector framework enables operators to run functional tests against a real data source. Refer to [Connector testing](https://www.elastic.co/guide/en/enterprise-search/master/build-connector.html#build-connector-testing) for more details.

To perform E2E testing for the SharePoint Server connector, run the following command:

```shell
$ make ftest NAME=sharepoint_server
```

ℹ️ Users can generate the [perf8](https://github.com/elastic/perf8) report using an argument i.e. `PERF8=True`. Users can also mention the size of the data to be tested for E2E test amongst SMALL, MEDIUM and LARGE by setting up an argument `DATA_SIZE=SMALL`. By Default, it is set to `MEDIUM`.

ℹ️ Users do not need to have a running Elasticsearch instance or a SharePoint Server source to run this test. The docker compose file manages the complete setup of the development environment, i.e. both the mock Elastic instance and mock SharePoint Server source using the docker image.

ℹ️ The connector uses the Elastic [ingest attachment processor](https://www.elastic.co/guide/en/enterprise-search/current/ingest-pipelines.html) plugin for extracting file contents. The ingest attachment processor extracts files by using the Apache text extraction library Tika. Supported file types eligible for extraction can be found as `TIKA_SUPPORTED_FILETYPES` in [utils.py](../connectors/utils.py) file.