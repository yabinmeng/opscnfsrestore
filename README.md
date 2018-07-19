# 1. Problem Overview

DataStax OpsCenter simplifies the task of backup and restore of data out of a DSE (DataStax Enterprise) cluster a lot through its out-of-the-box feature of [Backup Service](https://docs.datastax.com/en/opscenter/6.5/opsc/online_help/services/opscBackupService.html). Through this service, a user can choose to bakup DSE data to multiple locations, including NFS and AWS S3.

**==Restore Challenge==**

When we use OpsCener Service to restore backup data from another location like NFS or AWS S3, behind the scenes it utilizes the traditional Cassandra "sstableloader" utility. Simply speaking, OpsCenter server, through datatax-agent on each DSE node, fetches matching backup data from the backup location and once it is done, it kicks of "sstableloader" to bulk-load data into DSE cluster. It repeats the same process until all backup data in the backup location has been processed.

This approach has pros and cons: 
- The biggest pro is that it can tolerate DSE topology change, which means that the backup data can be restored to:
  1) the same cluster without any topology change; or
  2) the same cluster with some topology change; or
  3) a brand new cluster.

- A major downside is that it is going to consume extra disk space (and extra disk and network I/O bandwith) in order to complete the whole process. For a keyspace with replication factor N (N > 1, normally 3 or above), it causes N times of the backup data to be ingested into the cluster. Although over time, the C* compaction process will address the issue; but still, a lot of data has been transmitted over the network and processed in the system.

Meanwhile, in certain situations, sstableloader utility may fail to work due to a known bug (which is to be fixed in the future DSE release). In these situations, some other methods to restore OpsCenter backup data are needed.


# 2. Solution Overview and Usage Description

In many cases, when there is **NO DSE cluster topology change**, a much faster approach to restore OpsCenter backup data is to:
1) Simply copy the backup data to its corresponding DSE node, under the right C* keyspace/table (file system) data directory
2) Once the data is copied, either restart DSE node or run "nodetool refresh" command (no restart needed) to pick up the data-to-be-retored in DSE cluster.

The second step of this approach is very straightforward. But when it comes to the first step of fetching corresponding DSE node backup data (matching to a specific backup time, keyspace, and table) from the backup location, it is not that obvious and there is NO ready-to-use tool that can help. The goal of this code repository is to provide such a utility that can help user to fast (multi-threaded) restore DSE node specific backup data from the backup lcoation to a local directory on a DSE node. 

## 2.1. Usage Description

<h3><b>Disclaimer: This utility ONLY works with the data that is backed up by OpsCenter backup service because it relies on the OpsCenter backup metadata to filter out the correct backup data to be restored! </b></h3>


In order to use this utility, please take the following steps: 

1. Download the most recent release (version 1.0) of .jar file from [here](https://github.com/yabinmeng/opscnfsrestore/releases/download/1.0/opscnfsrestore-1.0-SNAPSHOT.jar)

2. Download the example configuration file (opsc_nfs_config.properties) from [here](https://github.com/yabinmeng/opscnfsrestore/blob/master/src/main/resources/opsc_nfs_config.properties)

   The example configuration file includes 3 items to configure. These items are quite straightforward and self-explanatory. Please update accordingly to your use case!
```
dse_contact_point: <DSE_cluster_contact_point>
local_download_home: <DSE_node_local_download_home_directory>
nfs_backup_home: <absolute_path_of_NFS_backup_location>
```
**NOTE**: Please make sure using the absolute path for both the NFS backup location and the local download home directory! The Linux user that runs this utility needs to have read privilege on the NFS backup location as well as both read and write privilege on the local download directory.

3. Run the program, providing the proper java options and arguments.
```
java 
  -jar ./opscnfsrestore-1.0-SNAPSHOT.jar com.dsetools.DseOpscNFSRestore 
  -l <all|DC:"<DC_name>"|>me[:"<dsenode_host_id_string>"]> 
  -c <opsc_nfs_configure.properties_full_path> 
  -d <concurrent_downloading_thread_num> 
  -k <keyspace_name> 
  [-t <table_name>] 
  -obt <opscenter_backup_time> 
  [-cls <true|false>]
  [-nds <true|false>]
```

The program needs a few Java options and parameters to work properly:

<table>
    <thead>
        <tr>
            <th width=43%>Java Option or Parameter</th>
            <th width=43%>Description</th>
            <th width=4%>Mandatory?</th>
        </tr>
    </thead>
    <tbody>
        <tr> 
            <td> -l &lt; all | DC:"&lt;DC_name&gt;" | me[:"&lt;dsenode_host_id_string&gt;"] </td>
            <td> List OpsCenter backup SSTables on the commandline output: <br/>
                <li> all -- list OpsCenter backup SSTables for all nodes in the cluster </li>
                <li> DC:"&lt;DC_name&gt;" -- list OpsCenter backup SSTables of all nodes in a specified DC </li>
                <li> me[:"&lt;dsenode_host_id_string&gt;"] -- list OpsCenter backup SSTables just for 
                   <ul> 
                      <li> myself (the node that runs this program - IP matching) </li> 
                      <li> for any DSE node with its host ID provided as second parameter for this option. </li>
                   </ul>
               </li>
             <td> Yes </td>
        </tr>
        <tr>
            <td> -c &lt; opsc_nfs_configure.properties_full_paht &gt; </td>
            <td> The full file path of "opsc_nfs_configure.properties" file. </td>
            <td> Yes </td>
        </tr>
        <tr>
            <td> -d &lt;max_concurrent_downloading_thread_num &gt; </td>
            <td> 
                <li> <b>ONLY works with "-l me" option; which means "-l all" and "-l DC" options are just for display purpose</b> </li>
                <li> &lt; concurrent_downloading_thread_num &gt; represents the number of threads (default 5 if not specified) that can concurrently download OpsCenter backup sstable sets. </li>
           </td>
           <td> No </td>
        </tr>
        <tr>
           <td> -k &lt;keyspace_name&gt; </td>
           <td> Download all OpsCenter backup SSTables that belong to the specified keyspace. </td>
           <td> Yes </td>
        </tr>
        <tr>
           <td> -t &lt;table_name&gt; </td>
           <td> <li> Download all OpsCenter backup SSTables that belong to the specified table. </li> 
                <li> When not specified, all Cassandra tables under the specified keyspace will be downloaded. </li>
           </td>
           <td> No </td>
         </tr>
         <tr>
           <td> -obt &lt;opsCenter_backup_time&gt; </td>
            <td> OpsCenter backup time (must be in format <b>M/d/yyyy h:mm a</b>) </li>
           </td>
           <td> Yes </td>
         </tr>
         <tr>
           <td> -cls &lt;true|false&gt; </td>
           <td> Whether to clear local download home directory before downloading (default: false)
           </td>
           <td> No </td>
         </tr>
         <tr>
           <td> -nds &lt;true|false&gt; </td>
           <td> Whether NOT to maitain backup location folder structure in the local download directory (default: false)
             <li> <b>ONLY applicable when "-t" option is specified.</b> </li> 
             <li> When NOT specified or NO "-t" option is specified, backup location folder structure is always maintained under the local download directory. This is to avoid possible SSTable name conflicts among different keyspaces and/or tables.</li>
           </td>
           <td> No </td>
         </tr>
    </tbody>
</table>
</br>

## 2.2. Filter OpsCenter backup SSTables by keyspace, table, and backup_time

This utility allows you to download OpsCenter backup SSTables further by the following categories:
1. Cassandra keyspace name that the SSTables belong to ("-k" option, Mandtory)
2. Cassandra table name that the SSTables belong to ("-t" option, Optional)
3. OpsCenter backup time ("-obt" option, Mandatory)  

If Cassandra table name is not specified, then all SSTables belonging to all tables under the specified keyspace will be downloaded.

When specifiying OpsCenter backup time, it <b>MUST</b> be 
- In format <b>M/d/yyyy h:mm a</b> (an example: 7/17/2018 10:02 PM)
- Matching the OpsCenter backup time from OpsCenter WebUI, as highlighted in the example screenshot below:
  <img src="src/main/images/Screen%20Shot%202018-07-09%20at%2022.21.18.png" width="250px"/>

## 2.3. Multi-threaded Download and Local Download Folder Structure

This utility is designed to be multi-threaded by nature to download multiple SSTable sets. When I say one SSTable set, it refers to the following files together:
* mc-<#>-big-CompresssionInfo.db
* mc-<#>-big-Data.db
* mc-<#>-big-Filter.db
* mc-<#>-big-Index.db
* mc-<#>-big-Statistics.db
* mc-<#>-big-Summary.db

**NOTE**: Currently this utility ONLY supports C* table with "mc" format (C* 3.0+/DSE 5.0/DSE5.1). It will be extended in the future to support other versions of formats.

Each thread is downloading one SSTable set. Multiple threads can download multiple sets concurrently. The maximum number threads that can concurrently download is determined by the value of <b>-d option</b>. If this option is not specified, then the utility only lists the OpsCenter backup SSTables without actually downloading it.

When "-d <concurrent_downloading_thread_num>" option is provided, the backup SSTables files will be downloaded (from NFS backup location) to the spcified local download home directory. The following 2 options determine how the local download home directory is organized:
* The "-cls <true|false>" option controls whether to clear the local download home directory before starting downloading!
* The "-nds <true|false>" option controls whether to maintain backup location folder structure underthe local download home directory. We maintain such structure by default in order to organized the recovered SSTables by keyspaces and tables. When this option has a "true" value (don't maintain the backup location folder structure), all restored SSTables are flattened out and put directly under the local download home directory. <b>In order to avoid possible SSTable name conflict among different keyspaces and/or tables. "-nds <true|false>" option ONLY works when you specify "-t" option.</b>

An example is demonstrated below.

```
<local_download_directory_home>/
└── snapshots
    └── 74c08172-9870-4dcc-9a7e-48bddfcc8572
        └── sstables
            └── testks
                ├── songs
                │   ├── mc-1-big-CompressionInfo.db
                │   ├── mc-1-big-Data.db
                │   ├── mc-1-big-Filter.db
                │   ├── mc-1-big-Index.db
                │   ├── mc-1-big-Statistics.db
                │   └── mc-1-big-Summary.db
                └── testbl
                    ├── mc-1-big-CompressionInfo.db
                    ├── mc-1-big-Data.db
                    ├── mc-1-big-Filter.db
                    ├── mc-1-big-Index.db
                    ├── mc-1-big-Statistics.db
                    └── mc-1-big-Summary.db
```



## 2.4. Examples

1. List **Only** OpsCenter backup SSTables for all nodes in a cluster that belong to C* table "testks.songs" (<keyspace.table>) for the backup taken at 7/17/2018 10:02 PM
```
java 
  -jar ./opscnfsrestore-1.0-SNAPSHOT.jar com.dsetools.DseOpscNFSRestore
  -c ./opsc_nfs_config.properties
  -l all 
  -k testks 
  -t songs 
  -obt "7/17/2018 10:02 PM"
```

2. List **Only** OpsCenter backup SSTables for the current node that runs this program and belong to C* keyspace "testks1" for the backup taken at 7/17/2018 10:02 PM
```
java 
  -jar ./opscnfsrestore-1.0-SNAPSHOT.jar com.dsetools.DseOpscNFSRestore 
  -c ./opsc_nfs_config.properties
  -l me
  -k testks1 
  -obt "7/17/2018 10:02 PM"
```

3. List and **Download** (with concurrent downloading thread number 5) OpsCenter backup SSTables for a particular node that runs this program and belong to C* keyspace "testks" for the backup taken at 7/17/2018 10:02 PM. Local download home directory is configured in "opsc_nfs_config.properties" file and will be cleared before downloading.
```
java 
  -jar ./opscnfsrestore-1.0-SNAPSHOT.jar com.dsetools.DseOpscNFSRestore
  -c ./opsc_nfs_config.properties 
  -l me:"74c08172-9870-4dcc-9a7e-48bddfcc8572" 
  -d 5
  -k testks
  -obt "7/17/2018 10:02 PM"
  -cls true
```

The utility command line output for example 3 above is something like below:
```
List and download OpsCenter NFS backup items for specified host (74c08172-9870-4dcc-9a7e-48bddfcc8572) ...
  - /Users/yabinmeng/Temp/nfs_bkup_simu/snapshots/74c08172-9870-4dcc-9a7e-48bddfcc8572/sstables/a2d0b957a3e915d9f891268f691a7e36-mc-1-big-CompressionInfo.db (size = 43 bytes) [keyspace: testks; table: songs]
  - /Users/yabinmeng/Temp/nfs_bkup_simu/snapshots/74c08172-9870-4dcc-9a7e-48bddfcc8572/sstables/a2d0b957a3e915d9f891268f691a7e36-mc-1-big-Data.db (size = 87 bytes) [keyspace: testks; table: songs]
  - /Users/yabinmeng/Temp/nfs_bkup_simu/snapshots/74c08172-9870-4dcc-9a7e-48bddfcc8572/sstables/a2d0b957a3e915d9f891268f691a7e36-mc-1-big-Filter.db (size = 16 bytes) [keyspace: testks; table: songs]
  - /Users/yabinmeng/Temp/nfs_bkup_simu/snapshots/74c08172-9870-4dcc-9a7e-48bddfcc8572/sstables/a2d0b957a3e915d9f891268f691a7e36-mc-1-big-Index.db (size = 40 bytes) [keyspace: testks; table: songs]
  - /Users/yabinmeng/Temp/nfs_bkup_simu/snapshots/74c08172-9870-4dcc-9a7e-48bddfcc8572/sstables/a2d0b957a3e915d9f891268f691a7e36-mc-1-big-Statistics.db (size = 4614 bytes) [keyspace: testks; table: songs]
  - /Users/yabinmeng/Temp/nfs_bkup_simu/snapshots/74c08172-9870-4dcc-9a7e-48bddfcc8572/sstables/a2d0b957a3e915d9f891268f691a7e36-mc-1-big-Summary.db (size = 92 bytes) [keyspace: testks; table: songs]
  Creating thread with ID 0 (6).
  - /Users/yabinmeng/Temp/nfs_bkup_simu/snapshots/74c08172-9870-4dcc-9a7e-48bddfcc8572/sstables/e6294485f35f23c22348440fc8f79cdb-mc-1-big-CompressionInfo.db (size = 43 bytes) [keyspace: testks; table: testbl]
  - /Users/yabinmeng/Temp/nfs_bkup_simu/snapshots/74c08172-9870-4dcc-9a7e-48bddfcc8572/sstables/e6294485f35f23c22348440fc8f79cdb-mc-1-big-Data.db (size = 92 bytes) [keyspace: testks; table: testbl]
  - /Users/yabinmeng/Temp/nfs_bkup_simu/snapshots/74c08172-9870-4dcc-9a7e-48bddfcc8572/sstables/e6294485f35f23c22348440fc8f79cdb-mc-1-big-Filter.db (size = 16 bytes) [keyspace: testks; table: testbl]
  - /Users/yabinmeng/Temp/nfs_bkup_simu/snapshots/74c08172-9870-4dcc-9a7e-48bddfcc8572/sstables/e6294485f35f23c22348440fc8f79cdb-mc-1-big-Index.db (size = 24 bytes) [keyspace: testks; table: testbl]
  - /Users/yabinmeng/Temp/nfs_bkup_simu/snapshots/74c08172-9870-4dcc-9a7e-48bddfcc8572/sstables/e6294485f35f23c22348440fc8f79cdb-mc-1-big-Statistics.db (size = 4668 bytes) [keyspace: testks; table: testbl]
  - /Users/yabinmeng/Temp/nfs_bkup_simu/snapshots/74c08172-9870-4dcc-9a7e-48bddfcc8572/sstables/e6294485f35f23c22348440fc8f79cdb-mc-1-big-Summary.db (size = 56 bytes) [keyspace: testks; table: testbl]
  Creating thread with ID 1 (6).
   - Starting thread 0 at: 2018-07-18 17:28:04
   - Starting thread 1 at: 2018-07-18 17:28:04
     [Thread 0] download of "/Users/yabinmeng/Temp/nfs_bkup_simu/snapshots/74c08172-9870-4dcc-9a7e-48bddfcc8572/sstables/a2d0b957a3e915d9f891268f691a7e36-mc-1-big-CompressionInfo.db[keyspace: testks; table: songs]" completed 
        >>> 43 of 43 bytes transferred.
     [Thread 1] download of "/Users/yabinmeng/Temp/nfs_bkup_simu/snapshots/74c08172-9870-4dcc-9a7e-48bddfcc8572/sstables/e6294485f35f23c22348440fc8f79cdb-mc-1-big-CompressionInfo.db[keyspace: testks; table: testbl]" completed 
        >>> 43 of 43 bytes transferred.
     [Thread 0] download of "/Users/yabinmeng/Temp/nfs_bkup_simu/snapshots/74c08172-9870-4dcc-9a7e-48bddfcc8572/sstables/a2d0b957a3e915d9f891268f691a7e36-mc-1-big-Data.db[keyspace: testks; table: songs]" completed 
        >>> 87 of 87 bytes transferred.
     [Thread 1] download of "/Users/yabinmeng/Temp/nfs_bkup_simu/snapshots/74c08172-9870-4dcc-9a7e-48bddfcc8572/sstables/e6294485f35f23c22348440fc8f79cdb-mc-1-big-Data.db[keyspace: testks; table: testbl]" completed 
        >>> 92 of 92 bytes transferred.
     [Thread 0] download of "/Users/yabinmeng/Temp/nfs_bkup_simu/snapshots/74c08172-9870-4dcc-9a7e-48bddfcc8572/sstables/a2d0b957a3e915d9f891268f691a7e36-mc-1-big-Filter.db[keyspace: testks; table: songs]" completed 
        >>> 16 of 16 bytes transferred.
     [Thread 1] download of "/Users/yabinmeng/Temp/nfs_bkup_simu/snapshots/74c08172-9870-4dcc-9a7e-48bddfcc8572/sstables/e6294485f35f23c22348440fc8f79cdb-mc-1-big-Filter.db[keyspace: testks; table: testbl]" completed 
        >>> 16 of 16 bytes transferred.
     [Thread 0] download of "/Users/yabinmeng/Temp/nfs_bkup_simu/snapshots/74c08172-9870-4dcc-9a7e-48bddfcc8572/sstables/a2d0b957a3e915d9f891268f691a7e36-mc-1-big-Index.db[keyspace: testks; table: songs]" completed 
        >>> 40 of 40 bytes transferred.
     [Thread 1] download of "/Users/yabinmeng/Temp/nfs_bkup_simu/snapshots/74c08172-9870-4dcc-9a7e-48bddfcc8572/sstables/e6294485f35f23c22348440fc8f79cdb-mc-1-big-Index.db[keyspace: testks; table: testbl]" completed 
        >>> 24 of 24 bytes transferred.
     [Thread 0] download of "/Users/yabinmeng/Temp/nfs_bkup_simu/snapshots/74c08172-9870-4dcc-9a7e-48bddfcc8572/sstables/a2d0b957a3e915d9f891268f691a7e36-mc-1-big-Statistics.db[keyspace: testks; table: songs]" completed 
        >>> 4614 of 4614 bytes transferred.
     [Thread 1] download of "/Users/yabinmeng/Temp/nfs_bkup_simu/snapshots/74c08172-9870-4dcc-9a7e-48bddfcc8572/sstables/e6294485f35f23c22348440fc8f79cdb-mc-1-big-Statistics.db[keyspace: testks; table: testbl]" completed 
        >>> 4668 of 4668 bytes transferred.
     [Thread 0] download of "/Users/yabinmeng/Temp/nfs_bkup_simu/snapshots/74c08172-9870-4dcc-9a7e-48bddfcc8572/sstables/a2d0b957a3e915d9f891268f691a7e36-mc-1-big-Summary.db[keyspace: testks; table: songs]" completed 
        >>> 92 of 92 bytes transferred.
   - Existing Thread 0 at 2018-07-18 17:28:04 (duration: 0 seconds): 6 of 6 OpsCenter backup SSTable files downloaded, 0 failed.
     [Thread 1] download of "/Users/yabinmeng/Temp/nfs_bkup_simu/snapshots/74c08172-9870-4dcc-9a7e-48bddfcc8572/sstables/e6294485f35f23c22348440fc8f79cdb-mc-1-big-Summary.db[keyspace: testks; table: testbl]" completed 
        >>> 56 of 56 bytes transferred.
   - Existing Thread 1 at 2018-07-18 17:28:04 (duration: 0 seconds): 6 of 6 OpsCenter backup SSTable files downloaded, 0 failed.
```
