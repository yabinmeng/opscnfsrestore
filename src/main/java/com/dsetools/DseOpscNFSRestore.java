package com.dsetools;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.dse.DseCluster;
import org.apache.commons.cli.*;
import org.apache.commons.io.FileUtils;
import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser;
import org.json.simple.JSONObject;

import java.io.*;
import java.net.*;
import java.nio.file.*;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Stream;


class NFSObjDownloadRunnable implements  Runnable {
    private int threadID;
    private String downloadHomeDir;
    private String[] opscObjNames;
    private long[] opscObjSizes;
    private String[] keyspaceNames;
    private String[] tableNames;
    private boolean noTargetDirStruct;

    private DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    NFSObjDownloadRunnable( int tID,
                           String download_dir,
                           String[] object_names,
                           long[] object_sizes,
                           String[] ks_names,
                           String[] tbl_names,
                           boolean no_dir_struct ) {
        assert (tID > 0);

        this.threadID = tID;
        this.downloadHomeDir = download_dir;
        this.opscObjNames = object_names;
        this.opscObjSizes = object_sizes;
        this.keyspaceNames = ks_names;
        this.tableNames = tbl_names;
        this.noTargetDirStruct = no_dir_struct;

        System.out.format("  Creating thread with ID %d (%d).\n", threadID, opscObjNames.length);
    }

    @Override
    public void run() {

        LocalDateTime startTime = LocalDateTime.now();

        System.out.println("   - Starting thread " + threadID + " at: " + startTime.format(formatter));

        int downloadedS3ObjNum = 0;
        int failedS3ObjNum = 0;

        for ( int i = 0; i < opscObjNames.length; i++ ) {
            try {
                int mcFlagStartPos = opscObjNames[i].indexOf(DseOpscNFSRestoreUtils.CASSANDRA_SSTABLE_FILE_CODE);
                String realSStableName = opscObjNames[i].substring(mcFlagStartPos);

                String tmp = opscObjNames[i].substring(0, mcFlagStartPos -1 );
                int lastPathSeperatorPos = tmp.lastIndexOf('/');

                String parentPathStr = tmp.substring(0, lastPathSeperatorPos);
                parentPathStr = parentPathStr.substring(parentPathStr.indexOf(DseOpscNFSRestoreUtils.OPSC_NFS_OBJKEY_BASESTR));

                File localFile = new File(downloadHomeDir + "/" +
                     ( noTargetDirStruct ? "" :
                        (parentPathStr + "/" + keyspaceNames[i] + "/" + tableNames[i] + "/") ) +
                     realSStableName );


                File nfsSrcFile = new File(opscObjNames[i]);

                FileUtils.copyFile(nfsSrcFile, localFile);

                downloadedS3ObjNum++;

                System.out.format("     [Thread %d] download of \"%s\" completed \n", threadID,
                        opscObjNames[i] + "[keyspace: " + keyspaceNames[i] + "; table: " + tableNames[i] + "]");
                System.out.format("        >>> %d of %d bytes transferred.\n",
                    localFile.length(),
                    opscObjSizes[i]);
            }
            catch ( IOException ioe) {
                System.out.format("     [Thread %d] download of \"%s\" encounters IO Exception\n", threadID,
                    opscObjNames[i] + "[keyspace: " + keyspaceNames[i] + "; table: " + tableNames[i] + "]");
                failedS3ObjNum++;
            }
            catch ( Exception ex ) {
                ex.printStackTrace();
                System.out.format("     [Thread %d] download of \"%s\" failed - unkown error\n", threadID,
                    opscObjNames[i] + "[keyspace: " + keyspaceNames[i] + "; table: " + tableNames[i] + "]");
                ex.printStackTrace();
                failedS3ObjNum++;
            }
        }

        LocalDateTime endTime = LocalDateTime.now();

        Duration duration = Duration.between(startTime, endTime);

        System.out.format("   - Existing Thread %d at %s (duration: %d seconds): %d of %d s3 objects downloaded, %d failed.\n",
            threadID,
            endTime.format(formatter),
            duration.getSeconds(),
            downloadedS3ObjNum,
            opscObjNames.length,
            failedS3ObjNum
        );
    }
}

public class DseOpscNFSRestore {

    private static Properties CONFIGPROP = null;
    private static Map<Path, Long> NFS_BACKUP_FILELIST = null;


    /**
     * List (and download) Opsc S3 backup objects for a specified host
     *
     * @param dseClusterMetadata
     * @param hostId
     * @param download
     * @param threadNum
     * @param keyspaceName
     * @param tableName
     * @param opscBckupTimeGmt
     * @param clearTargetDownDir
     * @param noTargetDirStruct
     */
    static void listDownloadNFSObjForHost(Metadata dseClusterMetadata,
                                         String hostId,
                                         boolean download,
                                         int threadNum,
                                         String keyspaceName,
                                         String tableName,
                                         ZonedDateTime opscBckupTimeGmt,
                                         boolean clearTargetDownDir,
                                         boolean noTargetDirStruct ) {
        assert (hostId != null);

        System.out.format("\nList" +
            (download ? " and download" : "") +
            " OpsCenter NFS backup items for specified host (%s) ...\n", hostId);

        String downloadHomeDir = CONFIGPROP.getProperty(DseOpscNFSRestoreUtils.CFG_KEY_LOCAL_DOWNLOAD_HOME);

        if (download) {
            assert (threadNum > 0);

            // If non-existing, create local home directory to hold S3 download files
            try {
                File file = new File(downloadHomeDir);

                if ( Files.notExists(file.toPath()))  {
                    FileUtils.forceMkdir(file);
                }
                else {
                    if (clearTargetDownDir) {
                        FileUtils.cleanDirectory(file);
                    }
                }
            }
            catch (IOException ioe) {
                System.out.println("ERROR: failed to create download home directory for S3 objects!");
                System.exit(-10);
            }
        }

        String basePrefix = CONFIGPROP.get(DseOpscNFSRestoreUtils.CFG_KEY_OPSC_NFS_BKUP_HOMEDIR) + "/" +
                DseOpscNFSRestoreUtils.OPSC_NFS_OBJKEY_BASESTR + "/" + hostId;

        Map<String, String> opscUniquifierToKsTbls = new HashMap<String, String>();

        DateTimeFormatter s3OpscObjTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH-mm-ss-z");
        String opscBckupTimeGmtStr = opscBckupTimeGmt.format(s3OpscObjTimeFormatter);


        // First, check OpsCenter records matching the backup time
        String opscPrefixString = basePrefix + "/" +
                DseOpscNFSRestoreUtils.OPSC_NFS_OBJKEY_OPSC_MARKER_STR + "_";

        for ( Path nfsBkupItem : NFS_BACKUP_FILELIST.keySet() ) {
            String opscObjName = nfsBkupItem.toFile().getAbsolutePath();

            if ( opscObjName.startsWith(opscPrefixString) ) {
                String opscObjNameShortZeroSecond =
                        opscObjName.substring(opscPrefixString.length(), opscPrefixString.length() + 16) + "-00-UTC";

                // Only deal with the S3 objects that fall in the specified OpsCenter backup time range
                if (opscBckupTimeGmtStr.equalsIgnoreCase(opscObjNameShortZeroSecond)) {
                    // Process OpsCenter backup.json file to get the Keyspace/Table/S3_Identifier mapping
                    if (opscObjName.contains("backup.json")) {
                        // After downloaded the backup.json file, process its content to get mapping between
                        // S3 Uniquifier to Keyspace-Table.
                        opscUniquifierToKsTbls = getOpscUniquifierToKsTblMapping(opscObjName);
                    }
                }
            }
        }

        // Download SSTable S3 object items
        int numSstableBkupItems = 0;

        //
        // Start multiple threads to process data ingestion concurrently
        //
        ExecutorService executor = Executors.newFixedThreadPool(threadNum);

        // For sstable download - we use mulitple threads per sstable set. One set includes the following files:
        // > mc-<#>-big-CompresssionInfo.db
        // > mc-<#>-big-Data.db
        // > mc-<#>-big-Filter.db
        // > mc-<#>-big-Index.db
        // > mc-<#>-big-Statistics.db
        // > mc-<#>-big-Summary.db
        final int SSTABLE_SET_FILENUM = 6;

        String[] opscSstableObjKeyNames = new String[SSTABLE_SET_FILENUM];
        long[] opscSstableObjKeySizes = new long[SSTABLE_SET_FILENUM];
        String[] opscSstableKSNames = new String[SSTABLE_SET_FILENUM];
        String[] opscSstableTBLNames = new String[SSTABLE_SET_FILENUM];

        int i = 0;
        int threadId = 0;

        String sstablePrefixString = basePrefix + "/" +
                DseOpscNFSRestoreUtils.OPSC_NFS_OBJKEY_SSTABLES_MARKER_STR;

        for ( Path nfsBkupItem : NFS_BACKUP_FILELIST.keySet() ) {
            String opscObjName = nfsBkupItem.toFile().getAbsolutePath();

            if ( opscObjName.startsWith(sstablePrefixString) ) {
                String sstableObjName = opscObjName.substring(opscObjName.lastIndexOf("/") + 1);

                if (opscUniquifierToKsTbls.containsKey(sstableObjName)) {

                    String[] ksTblUniquifer = opscUniquifierToKsTbls.get(sstableObjName).split(":");
                    String ks = ksTblUniquifer[0];
                    String tbl = ksTblUniquifer[1];

                    boolean filterKsTbl = keyspaceName.equalsIgnoreCase(ks);
                    if ((tableName != null) && !tableName.isEmpty()) {
                        filterKsTbl = filterKsTbl && tableName.equalsIgnoreCase(tbl);
                    }

                    if (filterKsTbl) {
                        numSstableBkupItems++;
                        System.out.println("  - " + opscObjName + " (size = " + NFS_BACKUP_FILELIST.get(nfsBkupItem)
                                + " bytes) [keyspace: " + ks + "; table: " + tbl + "]");

                        opscSstableObjKeyNames[i % SSTABLE_SET_FILENUM] = opscObjName;
                        opscSstableObjKeySizes[i % SSTABLE_SET_FILENUM] = NFS_BACKUP_FILELIST.get(nfsBkupItem);
                        opscSstableKSNames[i % SSTABLE_SET_FILENUM] = ks;
                        opscSstableTBLNames[i % SSTABLE_SET_FILENUM] = tbl;

                        if (download) {
                            if ((i > 0) && ((i + 1) % SSTABLE_SET_FILENUM == 0)) {
                                Runnable worker = new NFSObjDownloadRunnable(
                                        threadId,
                                        downloadHomeDir,
                                        opscSstableObjKeyNames,
                                        opscSstableObjKeySizes,
                                        opscSstableKSNames,
                                        opscSstableTBLNames,
                                        noTargetDirStruct);

                                threadId++;

                                opscSstableObjKeyNames = new String[SSTABLE_SET_FILENUM];
                                opscSstableObjKeySizes = new long[SSTABLE_SET_FILENUM];
                                opscSstableKSNames = new String[SSTABLE_SET_FILENUM];
                                opscSstableTBLNames = new String[SSTABLE_SET_FILENUM];

                                executor.execute(worker);
                            }

                            i++;
                        }
                    }
                }
            }
        }

        if ( download && ( threadId  < (numSstableBkupItems / SSTABLE_SET_FILENUM) ) ) {
            Runnable worker = new NFSObjDownloadRunnable(
                threadId,
                downloadHomeDir,
                opscSstableObjKeyNames,
                opscSstableObjKeySizes,
                opscSstableKSNames,
                opscSstableTBLNames,
                noTargetDirStruct);

            executor.execute(worker);
        }

        executor.shutdown();

        while (!executor.isTerminated()) {
        }

        if (numSstableBkupItems == 0) {
            System.out.println("  - Found no matching backup records for the specified conditions!.");
        }

        System.out.println("\n");
    }


    /**
     * Get the local host IP (non 127.0.0.1)
     *
     * @param nicName
     * @return
     */
    static String getLocalIP(String nicName) {

        String localhostIp = null;

        try {
            localhostIp = InetAddress.getLocalHost().getHostAddress();
            // Testing Purpose
            //localhostIp = "10.240.0.6";

            if ( (localhostIp != null) && (localhostIp.startsWith("127.0")) ) {

                NetworkInterface nic = NetworkInterface.getByName(nicName);
                Enumeration<InetAddress> inetAddress = nic.getInetAddresses();

                localhostIp = inetAddress.nextElement().getHostAddress();
            }
        }
        catch (Exception e) { }

        return localhostIp;
    }


    /**
     * List (and download) Opsc S3 backup objects for myself - the host that runs this program
     *
     * @param dseClusterMetadata
     * @param download
     * @param threadNum
     * @param hostIDStr
     * @param keyspaceName
     * @param tableName
     * @param opscBckupTimeGmt
     * @param clearTargetDownDir
     * @param noTargetDirStruct
     */
    static void listDownloadNFSObjForMe(Metadata dseClusterMetadata,
                                       boolean download,
                                       int threadNum,
                                       String hostIDStr,
                                       String keyspaceName,
                                       String tableName,
                                       ZonedDateTime opscBckupTimeGmt,
                                       boolean clearTargetDownDir,
                                       boolean noTargetDirStruct) {
        String myHostId = hostIDStr;

        if ( (hostIDStr == null) || (hostIDStr.isEmpty()) ) {

            String localhostIp = getLocalIP("eth0");

            if (localhostIp == null) {
                System.out.println("\nERROR: failed to get local host IP address!");
                return;
            }

            for (Host host : dseClusterMetadata.getAllHosts()) {
                InetAddress listen_address = host.getListenAddress();
                String listen_address_ip = (listen_address != null) ? host.getListenAddress().getHostAddress() : "";

                InetAddress broadcast_address = host.getListenAddress();
                String broadcast_address_ip = (broadcast_address != null) ? host.getBroadcastAddress().getHostAddress() : "";

                //System.out.println("listen_address: " + listen_address_ip);
                //System.out.println("broadcast_address: " + broadcast_address_ip + "\n");

                if (localhostIp.equals(listen_address_ip) || localhostIp.equals(broadcast_address_ip)) {
                    myHostId = host.getHostId().toString();
                    break;
                }
            }
        }

        if ( myHostId != null && !myHostId.isEmpty() ) {
            //System.out.println("locahost: " + myHostId);

            listDownloadNFSObjForHost(
                dseClusterMetadata,
                myHostId,
                download,
                threadNum,
                keyspaceName,
                tableName,
                opscBckupTimeGmt,
                clearTargetDownDir,
                noTargetDirStruct
            );
        }
    }


    /**
     * Get mapping from "S3_uniquifier" to "keyspace:table"
     *
     * @param backupJsonFileName
     * @return
     */
    static Map<String, String> getOpscUniquifierToKsTblMapping(String backupJsonFileName) {

        LinkedHashMap<String, String> opscObjMaps = new LinkedHashMap<String, String>();

        JSONParser jsonParser = new JSONParser();

        try {
            JSONObject jsonObject = (JSONObject) jsonParser.parse(new FileReader(backupJsonFileName));
            JSONArray jsonItemArr = (JSONArray)jsonObject.get(DseOpscNFSRestoreUtils.OPSC_NFS_OBJKEY_SSTABLES_MARKER_STR);

            Iterator<JSONObject> iterator = jsonItemArr.iterator();

            while (iterator.hasNext()) {
                String jsonItemContent = (String) iterator.next().toJSONString();
                jsonItemContent = jsonItemContent.substring(1, jsonItemContent.length() -1 );

                String[] keyValuePairs = jsonItemContent.split(",");

                String ssTableName = "";
                String uniquifierStr = "";
                String keyspaceName = "";
                String tableName = "";

                for ( String keyValuePair :  keyValuePairs ) {
                    String key = keyValuePair.split(":")[0];
                    String value = keyValuePair.split(":")[1];

                    key = key.substring(1, key.length() - 1 );
                    value = value.substring(1, value.length() - 1);

                    if ( key.equalsIgnoreCase("uniquifier") ) {
                        uniquifierStr = value;
                    }
                    else if ( key.equalsIgnoreCase("keyspace") ) {
                        keyspaceName = value;
                    }
                    else if ( key.equalsIgnoreCase("cf") ) {
                        tableName = value;
                    }
                    else if ( key.equalsIgnoreCase("name") ) {
                        ssTableName = value;
                    }
                }

                opscObjMaps.put(ssTableName, keyspaceName + ":" + tableName + ":" + uniquifierStr);
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        return opscObjMaps;
    }


    /**
     * Get file size of a file
     *
     * @param filePath
     * @return
     */
    static long getFileSize(Path filePath) {
        long size = 0;

        try {
            size = Files.size(filePath);
        }
        catch ( IOException ioe) {
        }

        return size;
    }
    /**
     * Recursively get all files and their sizes under a specified directory
     *
     * @param dirName
     * @return
     */
    static Map<Path, Long> listFilesForDir(String dirName) {
        assert ( (dirName != null) && !(dirName.isEmpty()) );

        Path homePath = Paths.get(dirName);

        //Map<Path, Long> paths = new LinkedHashMap<>();
        Map<Path, Long> paths = new TreeMap<>();

        try ( Stream<Path> stream = Files.walk(homePath) ) {
            stream.filter( p -> !p.toFile().isDirectory())
                  .forEach( p -> paths.put(p, getFileSize(p)) );
        }
        catch ( IOException ioe ) {
        }

        return paths;
    }


    /**
     * List Opsc S3 backup objects for all DSE cluster hosts
     *
     * @param dseClusterMetadata
     * @param keyspaceName
     * @param tableName
     * @param opscBckupTimeGmt
     */
    static void listNFSObjtForCluster(Metadata dseClusterMetadata,
                                     String keyspaceName,
                                     String tableName,
                                     ZonedDateTime opscBckupTimeGmt) {

        System.out.format("\nList OpsCenter NFS backup items for DSE cluster (%s) [%s] ...\n",
                dseClusterMetadata.getClusterName(),
                ((tableName == null) || (tableName.isEmpty())) ?
                        "Keyspace - " + keyspaceName :
                        "Table - " + keyspaceName + ":" + tableName
        );

        listNFSObjForDC(dseClusterMetadata, "", keyspaceName, tableName, opscBckupTimeGmt);
    }

    /**
     * List Opsc S3 backup objects for all hosts in a specified DC
     *
     * @param dseClusterMetadata
     * @param dcName
     * @param keyspaceName
     * @param tableName
     * @param opscBckupTimeGmt
     */
    static void listNFSObjForDC(Metadata dseClusterMetadata,
                               String dcName,
                               String keyspaceName,
                               String tableName,
                               ZonedDateTime opscBckupTimeGmt) {
        assert (CONFIGPROP != null);
        assert ( (keyspaceName != null) && !keyspaceName.isEmpty() );
        assert (opscBckupTimeGmt != null);

        Set<Host> hosts = dseClusterMetadata.getAllHosts();

        boolean dcOnly = ( (dcName != null) && !dcName.isEmpty() );
        if ( dcOnly ) {
            System.out.format("\nList OpsCenter NFS backup items for specified DC (%s) of DSE cluster (%s) [%s] ...\n",
                dcName,
                dseClusterMetadata.getClusterName(),
                ((tableName == null) || (tableName.isEmpty())) ?
                        "Keyspace - " + keyspaceName :
                        "Table - " + keyspaceName + ":" + tableName
            );
        }

        for ( Host host : hosts ) {
            int numSstableBkupItems = 0;

            String dc_name = host.getDatacenter();
            String rack_name = host.getRack();
            String host_id = host.getHostId().toString();

            // If not displaying for whole cluster (dcName == null),
            // then only displaying the specified DC
            if ( !dcOnly || (dc_name.equalsIgnoreCase(dcName)) ) {
                System.out.format("  Items for Host %s (rack: %s, DC: %s) ...\n",
                    host_id, rack_name, dc_name, dseClusterMetadata.getClusterName());

                String basePrefix = CONFIGPROP.get(DseOpscNFSRestoreUtils.CFG_KEY_OPSC_NFS_BKUP_HOMEDIR) + "/" +
                        DseOpscNFSRestoreUtils.OPSC_NFS_OBJKEY_BASESTR + "/" + host_id;

                Map<String, String> opscUniquifierToKsTbls = new HashMap<String, String>();

                DateTimeFormatter opscObjTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH-mm-ss-z");
                String opscBckupTimeGmtStr = opscBckupTimeGmt.format(opscObjTimeFormatter);

                // First, check OpsCenter records matching the backup time
                String opscPrefixString = basePrefix + "/" +
                        DseOpscNFSRestoreUtils.OPSC_NFS_OBJKEY_OPSC_MARKER_STR + "_";

                for ( Path nfsBkupItem : NFS_BACKUP_FILELIST.keySet() ) {
                    String opscObjName = nfsBkupItem.toFile().getAbsolutePath();

                    if ( opscObjName.startsWith(opscPrefixString) ) {
                        String opscObjNameShortZeroSecond =
                                opscObjName.substring(opscPrefixString.length(), opscPrefixString.length() + 16) + "-00-UTC";

                        // Only deal with the S3 objects that fall in the specified OpsCenter backup time range
                        if (opscBckupTimeGmtStr.equalsIgnoreCase(opscObjNameShortZeroSecond)) {
                            // Process OpsCenter backup.json file to get the Keyspace/Table/S3_Identifier mapping
                            if (opscObjName.contains("backup.json")) {
                                // After downloaded the backup.json file, process its content to get mapping between
                                // S3 Uniquifier to Keyspace-Table.
                                opscUniquifierToKsTbls = getOpscUniquifierToKsTblMapping(opscObjName);
                            }
                        }
                    }
                }

                // Second, check SSTables records matching the backup time, keyspace, and table
                String sstablePrefixString = basePrefix + "/" +
                        DseOpscNFSRestoreUtils.OPSC_NFS_OBJKEY_SSTABLES_MARKER_STR;

                for ( Path nfsBkupItem : NFS_BACKUP_FILELIST.keySet() ) {
                    String opscObjName = nfsBkupItem.toFile().getAbsolutePath();

                    if ( opscObjName.startsWith(sstablePrefixString) ) {
                        String sstableObjName = opscObjName.substring(opscObjName.lastIndexOf("/") + 1);

                        if (opscUniquifierToKsTbls.containsKey(sstableObjName)) {
                            String[] ksTblUniquifer = opscUniquifierToKsTbls.get(sstableObjName).split(":");
                            String ks = ksTblUniquifer[0];
                            String tbl = ksTblUniquifer[1];

                            boolean filterKsTbl = keyspaceName.equalsIgnoreCase(ks);
                            if ((tableName != null) && !tableName.isEmpty()) {
                                filterKsTbl = filterKsTbl && tableName.equalsIgnoreCase(tbl);
                            }

                            if (filterKsTbl) {
                                numSstableBkupItems++;
                                System.out.println("  - " + opscObjName + " (size = " + NFS_BACKUP_FILELIST.get(nfsBkupItem)
                                        + " bytes) [keyspace: " + ks + "; table: " + tbl + "]");
                            }
                        }
                    }
                }

                if (numSstableBkupItems == 0) {
                    System.out.println("  - Found no matching backup records for the specified conditions!.");
                }
            }

            System.out.println();
        }
    }


    /**
     *  Define Command Line Arguments
     */
    static Options options = new Options();

    static {
        Option helpOption = new Option(
            DseOpscNFSRestoreUtils.CMD_OPTION_HELP_SHORT,
            DseOpscNFSRestoreUtils.CMD_OPTION_HELP_LONG,
            false,
            "Displays this help message.");
        Option listOption = new Option(
            DseOpscNFSRestoreUtils.CMD_OPTION_LIST_SHORT,
            DseOpscNFSRestoreUtils.CMD_OPTION_LIST_LONG,
            true,
            "List OpsCenter S3 backup items (all | DC:\"<dc_name>\" | me[:\"<host_id_string>\"]).");
        Option downloadOption = new Option(
            DseOpscNFSRestoreUtils.CMD_OPTION_DOWNLOAD_SHORT,
            DseOpscNFSRestoreUtils.CMD_OPTION_DOWNLOAD_LONG,
            true,
            "Download OpsCenter S3 bakcup items to local directory (only applies to \"list me\" case");
        Option fileOption = new Option(
            DseOpscNFSRestoreUtils.CMD_OPTION_CFG_SHORT,
            DseOpscNFSRestoreUtils.CMD_OPTION_CFG_LONG,
            true,
            "Configuration properties file path");
        Option keyspaceOption = new Option(
            DseOpscNFSRestoreUtils.CMD_OPTION_KEYSPACE_SHORT,
            DseOpscNFSRestoreUtils.CMD_OPTION_KEYSPACE_LONG,
            true,
            "Keyspace name to be restored");
        Option tableOption = new Option(
            DseOpscNFSRestoreUtils.CMD_OPTION_TABLE_SHORT,
            DseOpscNFSRestoreUtils.CMD_OPTION_TABLE_LONG,
            true,
            "Table name to be restored");
        Option opscBkupTimeOPtion = new Option(
            DseOpscNFSRestoreUtils.CMD_OPTION_BACKUPTIME_SHORT,
            DseOpscNFSRestoreUtils.CMD_OPTION_BACKUPTIME_LONG,
            true,
            "OpsCetner backup datetime");
        Option clsTargetDirOPtion = new Option(
            DseOpscNFSRestoreUtils.CMD_OPTION_CLSDOWNDIR_SHORT,
            DseOpscNFSRestoreUtils.CMD_OPTION_CLSDOWNDIR_LONG,
            true,
            "Clear existing download directory content");
        Option noDirStructOPtion = new Option(
                DseOpscNFSRestoreUtils.CMD_OPTION_NODIR_SHORT,
                DseOpscNFSRestoreUtils.CMD_OPTION_NODIR_LONG,
                true,
                "Don't maintain keyspace/table backup data directory structure");

        options.addOption(helpOption);
        options.addOption(listOption);
        options.addOption(fileOption);
        options.addOption(downloadOption);
        options.addOption(keyspaceOption);
        options.addOption(tableOption);
        options.addOption(opscBkupTimeOPtion);
        options.addOption(clsTargetDirOPtion);
        options.addOption(noDirStructOPtion);
    }

    /**
     * Print out usage info. and exit
     */
    static void usageAndExit(int errorCode) {

        System.out.println();

        PrintWriter errWriter = new PrintWriter(System.out, true);

        try {
            HelpFormatter formatter = new HelpFormatter();

            formatter.printHelp(errWriter, 150, "DseOpscS3Restore",
                String.format("\nDseOpscS3Restore Options:"),
                options, 2, 1, "", true);

            System.out.println();
            System.out.println();
        }
        catch ( Exception e ) {
            e.printStackTrace();
        }
        finally
        {
            System.exit(errorCode);
        }
    }
    static void usageAndExit() {
        usageAndExit(0);
    }


    /**
     * Main function - java program entry point
     *
     * @param args
     */
    public static void main(String[] args) {

        /**
         *  Parsing commandline parameters (starts) ---->
         */

        DefaultParser parser = new DefaultParser();
        CommandLine cmd = null;

        try {
            cmd = parser.parse(options, args);
        }
        catch (ParseException e) {
            System.err.format("\nERROR: Failure parsing argument inputs: %s.\n", e.getMessage());
            usageAndExit(-10);
        }

        // Print help message
        if ( cmd.hasOption(DseOpscNFSRestoreUtils.CMD_OPTION_HELP_SHORT) ) {
            usageAndExit();
        }

        // "-c" option (Configuration File) is a must!
        String cfgFilePath = cmd.getOptionValue(DseOpscNFSRestoreUtils.CMD_OPTION_CFG_SHORT);
        if ( (cfgFilePath == null) || cfgFilePath.isEmpty() ) {
            System.err.println("\nERROR: Please specify a valid configuration file path as the \"-" +
                DseOpscNFSRestoreUtils.CMD_OPTION_CFG_SHORT + " option value.\n");
            usageAndExit(-20);
        }

        // "-l" option (ALL | DC:"<DC_Name>" | me[:"<C*_node_host_id>" is a must!
        boolean listCluster = false;
        boolean listDC = false;
        boolean listMe = false;

        String dcNameToList = "";
        String myHostID = "";

        String lOptVal = cmd.getOptionValue(DseOpscNFSRestoreUtils.CMD_OPTION_LIST_SHORT);
        if ( (lOptVal == null) || lOptVal.isEmpty() ) {
            System.out.println("\nERROR: Please specify proper value for \"-" +
                DseOpscNFSRestoreUtils.CMD_OPTION_LIST_SHORT + "\" option -- " +
                DseOpscNFSRestoreUtils.CMD_OPTION_LIST_ALL + " | " +
                DseOpscNFSRestoreUtils.CMD_OPTION_LIST_DC + ":\"<DC_Name>\" | " +
                DseOpscNFSRestoreUtils.CMD_OPTION_LIST_ME + "[:\"<host_id>\"].\n");
            usageAndExit(-30);
        }

        if ( lOptVal.equalsIgnoreCase(DseOpscNFSRestoreUtils.CMD_OPTION_LIST_ALL) ) {
            listCluster = true;
        }
        else if ( lOptVal.toUpperCase().startsWith(DseOpscNFSRestoreUtils.CMD_OPTION_LIST_DC) ) {
            listDC = true;

            String[] strSplits = lOptVal.split(":");
            if (strSplits.length != 2) {
                System.out.println("\nERROR: Please specify proper value for \"-" +
                    DseOpscNFSRestoreUtils.CMD_OPTION_LIST_SHORT + " " + DseOpscNFSRestoreUtils.CMD_OPTION_LIST_DC +
                    "\" option -- DC:\"<DC_Name>\".\n");
                usageAndExit(-40);
            }
            else {
                dcNameToList = strSplits[1];
            }
        }
        else if ( lOptVal.toUpperCase().startsWith(DseOpscNFSRestoreUtils.CMD_OPTION_LIST_ME) ) {
            listMe = true;

            String[] strSplits = lOptVal.split(":");
            if ( lOptVal.contains(":") && (strSplits.length != 2) ) {
                System.out.println("\nERROR: Please specify proper value for \"-" +
                    DseOpscNFSRestoreUtils.CMD_OPTION_LIST_SHORT + " " + DseOpscNFSRestoreUtils.CMD_OPTION_LIST_ME +
                    "\" option -- [:\"<specified_host_id_string>\"].\n");
                usageAndExit(-50);
            }
            else if ( lOptVal.contains(":") ) {
                myHostID = lOptVal.split(":")[1];
            }
        }
        else {
            System.out.println("\nERROR: Please specify proper value for \"-" +
                DseOpscNFSRestoreUtils.CMD_OPTION_LIST_SHORT + "\" option -- " +
                DseOpscNFSRestoreUtils.CMD_OPTION_LIST_ALL + " | " +
                DseOpscNFSRestoreUtils.CMD_OPTION_LIST_DC + ":\"<DC_Name>\" | " +
                DseOpscNFSRestoreUtils.CMD_OPTION_LIST_ME + "[:\"<host_id>\"].\n");
            usageAndExit(-60);
        }

        // Download option ONLY works for "-l me" option! If "-d" option value is not specified, use the default value
        boolean downloadS3Obj = false;
        int downloadS3ObjThreadNum = DseOpscNFSRestoreUtils.DOWNLOAD_THREAD_POOL_SIZE;

        if ( cmd.hasOption(DseOpscNFSRestoreUtils.CMD_OPTION_DOWNLOAD_SHORT) ) {
            downloadS3Obj = true;

            String dOptVal = cmd.getOptionValue(DseOpscNFSRestoreUtils.CMD_OPTION_DOWNLOAD_SHORT);
            if ( (dOptVal != null) && (!dOptVal.isEmpty()) ) {
                try {
                    downloadS3ObjThreadNum = Integer.parseInt(dOptVal);
                }
                catch (NumberFormatException nfe) {
                    System.out.println("\nWARN: Incorrect \"-" + DseOpscNFSRestoreUtils.CMD_OPTION_DOWNLOAD_SHORT +
                        "\" option value -- must be a positive integer! Using default value (" +
                        DseOpscNFSRestoreUtils.DOWNLOAD_THREAD_POOL_SIZE + ").\n");
                }
            }
        }

        // "-k" option (Keyspace name) is a must
        String keyspaceName = cmd.getOptionValue(DseOpscNFSRestoreUtils.CMD_OPTION_KEYSPACE_SHORT);
        if ( (keyspaceName == null) || keyspaceName.isEmpty() ) {
            System.out.println("\nERROR: Please specify proper keypsace name as the \"-" +
                DseOpscNFSRestoreUtils.CMD_OPTION_KEYSPACE_SHORT + "\" option value.\n");
            usageAndExit(-70);
        }

        // "-t" option (Table name) is optional. If not specified, all Tables of the specified keyspaces will be processed.
        String tableName = cmd.getOptionValue(DseOpscNFSRestoreUtils.CMD_OPTION_TABLE_SHORT);

        // "-obt" option is a must
        // OpsCenter Backup Date Time String (Can get  from OpsCenter Backup Service Window)
        String obtOptOptValue = cmd.getOptionValue(DseOpscNFSRestoreUtils.CMD_OPTION_BACKUPTIME_SHORT);

        if ( (obtOptOptValue == null) || (obtOptOptValue.isEmpty()) ) {
            System.out.println("\nERROR: Please specify proper OpsCenter S3 backup time string (M/d/yyyy h:mm a) as the \"-" +
                DseOpscNFSRestoreUtils.CMD_OPTION_BACKUPTIME_SHORT + "\" option value.");
            usageAndExit(-80);
        }

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("M/d/yyyy h:mm a");
        ZonedDateTime opscBackupTime_gmt = null;
        try {
            LocalDateTime ldt = LocalDateTime.parse(obtOptOptValue, formatter);

            ZoneId gmtZoneId = ZoneId.of("UTC");
            opscBackupTime_gmt = ldt.atZone(gmtZoneId);
        }
        catch (DateTimeParseException dte) {
            dte.printStackTrace();

            System.out.println("\nERROR: Please specify correct time string format (M/d/yyyy h:mm a) for \"-" +
                DseOpscNFSRestoreUtils.CMD_OPTION_BACKUPTIME_SHORT + "\" option.");
            usageAndExit(-90);
        }

        // "-cls" option is optional
        boolean clearTargetDownDir = false;
        String clsOptOptValue = cmd.getOptionValue(DseOpscNFSRestoreUtils.CMD_OPTION_CLSDOWNDIR_SHORT);

        if ( (clsOptOptValue != null) && (!clsOptOptValue.isEmpty()) ) {
            try {
                clearTargetDownDir = Boolean.parseBoolean(clsOptOptValue);
            }
            catch (NumberFormatException nfe) {
            }
        }

        // "-nds" option is optional.
        // ONLY works when "-t"/"--table" option is provided;
        //    Otherwise, target directory structure is automatically maintained.
        boolean noTargetDirStruct = false;
        String ndsOptOptValue = cmd.getOptionValue(DseOpscNFSRestoreUtils.CMD_OPTION_NODIR_SHORT);

        if ( (ndsOptOptValue != null) && (!ndsOptOptValue.isEmpty()) ) {
            try {
                noTargetDirStruct = Boolean.parseBoolean(ndsOptOptValue);

                if  ( (tableName == null) || tableName.isEmpty() ) {
                    noTargetDirStruct = false;
                }
            }
            catch (NumberFormatException nfe) {
            }
        }


        /**
         *  Parsing commandline parameters (ends)  <----
         */



        /**
         * Load configuration files
         */

        CONFIGPROP = DseOpscNFSRestoreUtils.LoadConfigFile(cfgFilePath);
        if (CONFIGPROP == null) {
            System.exit(-100);
        }

        NFS_BACKUP_FILELIST = listFilesForDir(CONFIGPROP.getProperty(DseOpscNFSRestoreUtils.CFG_KEY_OPSC_NFS_BKUP_HOMEDIR));
        // Testing purpose
        /*
        for ( Path path : NFS_BACKUP_FILELIST.keySet() ) {
            System.out.println(path.toFile().getAbsolutePath() + "(size: " + NFS_BACKUP_FILELIST.get(path) + ")");
        }

        System.exit(0);
        */


        /**
         * Check if NFS backup home directory is reachable!
         */
        Path nfsBackupHomePath = Paths.get(CONFIGPROP.getProperty(DseOpscNFSRestoreUtils.CFG_KEY_OPSC_NFS_BKUP_HOMEDIR));
        if ( !Files.isDirectory(nfsBackupHomePath) || Files.notExists(nfsBackupHomePath) ) {
            System.out.println("\nERROR: Specified NFS OpsCenter backup home directory (in the config file) is not correct!");
            usageAndExit(-110);
        }



        /**
         * Get Dse cluster metadata
         */
        QueryOptions queryOptions = new QueryOptions();
        queryOptions.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);

        DseCluster dseCluster = DseCluster.builder()
            .addContactPoint(CONFIGPROP.getProperty(DseOpscNFSRestoreUtils.CFG_KEY_CONTACT_POINT))
            .withQueryOptions(queryOptions)
            .build();

        // dseCluster.connect();    /* NO NEED for acutal connection */
        Metadata dseClusterMetadata = dseCluster.getMetadata();

        // List Opsc S3 backup items for all Dse Cluster hosts
        if ( listCluster ) {
            listNFSObjtForCluster(dseClusterMetadata, keyspaceName, tableName, opscBackupTime_gmt);
        }
        // List Opsc S3 backup items for all hosts in a specified DC of the Dse cluster
        else if ( listDC ) {
            listNFSObjForDC(dseClusterMetadata, dcNameToList, keyspaceName, tableName, opscBackupTime_gmt);
        }
        // List (and download) Opsc S3 backup items for myself (the host that runs this program)
        else if ( listMe ) {
            listDownloadNFSObjForMe(
                dseClusterMetadata,
                downloadS3Obj,
                downloadS3ObjThreadNum,
                myHostID,
                keyspaceName,
                tableName,
                opscBackupTime_gmt,
                clearTargetDownDir,
                noTargetDirStruct );
        }

        System.exit(0);
    }
}