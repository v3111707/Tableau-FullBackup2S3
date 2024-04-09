### Format
tsm_return_code\
tsm_stdout\
tsm_stderr

#### systemctl stop tabadmincontroller_0.service
```
(1, 
'Could not connect to server. Make sure that Tableau Server is running and try again.\n',
'')
```
#### Success
```
(0, 
"The previous GenerateBackupJob did not succeed after running for 6 minute(s).\nThe last successful run of GenerateBackupJob took 3 minute(s).\n\nJob id is '10', timeout is 1440 minutes.\n7% - Starting the Active Repository instance, File Store, and Cluster Controller.\nRunning - Waiting for the Active Repository, File Store, and Cluster Controller to start.\r                                                                                         \r14% - Waiting for the Active Repository, File Store, and Cluster Controller to start.\nRunning - Installing backup services.\r                                     \r21% - Installing backup services.\nRunning - Estimating required disk space.\r                                         \r28% - Estimating required disk space.\n35% - Gathering disk space information from all nodes.\n42% - Analyzing disk space information.\n50% - Checking if sufficient disk space is available on all nodes.\n57% - Backing up configuration.\nRunning - [Backing up object storage data.] [Backing up database.]\r                                                                  \r64% - Backing up object storage data.\nRunning - Backing up database.\r                              \r71% - Backing up database.\nRunning - Assembling the tsbak archive. Processing file 1 of 35.\r                                                                \r78% - Assembling the tsbak archive.\n85% - Stopping the Active Repository if necessary.\nRunning - Waiting for the Active Repository to stop if necessary.\r                                                                 \r92% - Waiting for the Active Repository to stop if necessary.\nRunning - Uninstalling backup services.\r                                       \r100% - Uninstalling backup services.\r                                    \r100% - Uninstalling backup services.\nBackup written to '/var/opt/tableau/tableau_server/data/tabsvc/files/backups/ts_backup-2024-04-08.tsbak' on the controller node.\n", 
'')
```
#### Cannot overwrite the existing file 
```
(1, 
"\nAn error occurred on the server generating the backup.\n\nSee '/var/opt/tableau/tableau_server/data/tabsvc/logs/tabadmincontroller/tabadmincontroller_*.log' on Tableau Server nodes running the Administration Controller process for server log information.\n\nResource Conflict: Cannot overwrite the existing file at '/var/opt/tableau/tableau_server/data/tabsvc/files/backups/ts_backup-2024-04-08.tsbak'\n", 
'')
```