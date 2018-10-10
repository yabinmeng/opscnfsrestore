package com.dsetools;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.dse.DseCluster;
import org.apache.commons.cli.*;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.FileFilterUtils;
import org.apache.commons.io.filefilter.IOFileFilter;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser;
import org.json.simple.JSONObject;

import java.io.*;
import java.net.*;
import java.nio.file.*;
import java.time.*;
import java.time.Duration;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


class NFSObjDownloadRunnable implements  Runnable {
    private int threadID;
    private boolean fileSizeChk;
    private String downloadHomeDir;
    private String[] opscObjNames;
    private long[] opscObjSizes;
    private String[] keyspaceNames;
    private String[] tableNames;
    private String[] sstableVersions;
    private boolean noTargetDirStruct;

    private DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    NFSObjDownloadRunnable(int tID,
                           boolean file_size_chk,
                           String download_dir,
                           String[] object_names,
                           long[] object_sizes,
                           String[] ks_names,
                           String[] tbl_names,
                           String[] sstable_versions,
                           boolean no_dir_struct ) {
        assert (tID > 0);

        this.threadID = tID;
        this.fileSizeChk = file_size_chk;
        this.downloadHomeDir = download_dir;
        this.opscObjNames = object_names;
        this.opscObjSizes = object_sizes;
        this.keyspaceNames = ks_names;
        this.tableNames = tbl_names;
        this.sstableVersions = sstable_versions;
        this.noTargetDirStruct = no_dir_struct;

        System.out.format("  Creating thread with ID %d (%d).\n", threadID, opscObjNames.length);
    }

    @Override
    public void run() {

        LocalDateTime startTime = LocalDateTime.now();

        System.out.println("   - Starting thread " + threadID + " at: " + startTime.format(formatter));

        int downloadedOpscObjNum = 0;
        int failedOpscObjNum = 0;

        for ( int i = 0; i < opscObjNames.length; i++ ) {
            try {
                int sstblVersionStartPos = opscObjNames[i].indexOf(sstableVersions[i]);
                String realSStableName = opscObjNames[i].substring(sstblVersionStartPos);

                String tmp = opscObjNames[i].substring(0, sstblVersionStartPos -1 );
                int lastPathSeperatorPos = tmp.lastIndexOf('/');

                String parentPathStr = tmp.substring(0, lastPathSeperatorPos);
                parentPathStr = parentPathStr.substring(parentPathStr.indexOf(DseOpscNFSRestoreUtils.OPSC_NFS_OBJKEY_BASESTR));

                File localFile = new File(downloadHomeDir + "/" +
                     ( noTargetDirStruct ? "" :
                        (parentPathStr + "/" + keyspaceNames[i] + "/" + tableNames[i] + "/") ) +
                     realSStableName );


                File nfsSrcFile = new File(opscObjNames[i]);

                FileUtils.copyFile(nfsSrcFile, localFile);

                downloadedOpscObjNum++;

                System.out.format("     [Thread %d] download of \"%s\" completed \n", threadID,
                    opscObjNames[i] + " [keyspace: " + keyspaceNames[i] + "; table: " + tableNames[i] + "]");
                if (fileSizeChk) {
                    System.out.format("        >>> %d of %d bytes transferred.\n",
                        localFile.length(),
                        opscObjSizes[i]);
                }
            }
            catch ( IOException ioe) {
                System.out.format("     [Thread %d] download of \"%s\" encounters IO Exception\n", threadID,
                    opscObjNames[i] + "[keyspace: " + keyspaceNames[i] + "; table: " + tableNames[i] + "]");
                failedOpscObjNum++;
            }
            catch ( Exception ex ) {
                ex.printStackTrace();
                System.out.format("     [Thread %d] download of \"%s\" failed - unkown error\n", threadID,
                    opscObjNames[i] + "[keyspace: " + keyspaceNames[i] + "; table: " + tableNames[i] + "]");
                ex.printStackTrace();
                failedOpscObjNum++;
            }
        }

        LocalDateTime endTime = LocalDateTime.now();

        Duration duration = Duration.between(startTime, endTime);

        System.out.format("   - Existing Thread %d at %s (duration: %d seconds): %d of %d OpsCenter SSTable backup files downloaded, %d failed.\n",
            threadID,
            endTime.format(formatter),
            duration.getSeconds(),
            downloadedOpscObjNum,
            opscObjNames.length,
            failedOpscObjNum
        );
    }
}



public class DseOpscNFSRestore {

    private static Properties CONFIGPROP = null;
    private static boolean debugOpt = false;


    /**
     * Get the full file path of the "backup.json" file that corresponds
     * to the specified DSE Host ID and OpsCenter backup time
     *
     * @param hostId
     * @param opscBckupTimeGmt
     * @return
     */
    static Path getMyBackupJson(String hostId,
                                ZonedDateTime opscBckupTimeGmt)
    {
        DateTimeFormatter opscObjTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH-mm-ss-z");
        String opscBckupTimeGmtStr = opscBckupTimeGmt.format(opscObjTimeFormatter);

        String nodeHomeDirString =
            CONFIGPROP.get(DseOpscNFSRestoreUtils.CFG_KEY_OPSC_NFS_BKUP_HOMEDIR) + "/" +
            DseOpscNFSRestoreUtils.OPSC_NFS_OBJKEY_BASESTR + "/" +
            hostId;

        Path nodeHomeDirPath = Paths.get(nodeHomeDirString);

        IOFileFilter fileFilter = FileFilterUtils.nameFileFilter(DseOpscNFSRestoreUtils.OPSC_BKUP_METADATA_FILE);
        IOFileFilter dirFilter = FileFilterUtils.prefixFileFilter(DseOpscNFSRestoreUtils.OPSC_NFS_OBJKEY_OPSC_MARKER_STR);

        Collection<File> opscBkupJsonFiles =
            FileUtils.listFiles(nodeHomeDirPath.toFile(), fileFilter, dirFilter);

        Path myBackupJsonFilePath = null;

        if (debugOpt) {
            System.out.println("     [DEBUG] getMyBackupJson() START ");
            System.out.println("     [DEBUG]    opscBckupTimeGmtStr: " + opscBckupTimeGmtStr );
        }

        for ( File file: opscBkupJsonFiles ) {
            String absFilePath = file.getAbsolutePath();

            // <nfs_backup_home_dir>/snapshots/<host_id>/opscenter_<schedule_time_uuid_string>_yyyy-MM-dd-HH-mm-ss-UTC/backup.json
            // <nfs_backup_home_dir>/snapshots/<host_id>/opscenter_adhoc_yyyy-MM-dd-HH-mm-ss-UTC/backup.json
            int startOfTimeStampPos =
                absFilePath.length()
                - DseOpscNFSRestoreUtils.OPSC_BKUP_METADATA_FILE.length()  // "backup.json"
                - 1     // "/"
                - 23;   // "yyyy-MM-dd-HH-mm-ss-UTC"

            String backupTimeShortZeroSecondStr =
                absFilePath.substring(startOfTimeStampPos, startOfTimeStampPos + 16) + "-00-UTC";

            if (debugOpt) {
                System.out.println("     [DEBUG]    absFilePath (backupTimeShortZeroSecondStr): " +
                    absFilePath + "(" + backupTimeShortZeroSecondStr + ")");
            }

            if (opscBckupTimeGmtStr.equalsIgnoreCase(backupTimeShortZeroSecondStr)) {
                myBackupJsonFilePath = file.toPath();
            }
        }

        if (debugOpt) {
            System.out.println("     [DEBUG]    myBackupJsonFilePath: " + myBackupJsonFilePath);
            System.out.println("     [DEBUG] getMyBackupJson() END ");
        }

        return myBackupJsonFilePath;
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
     * ------------------------------
     * NOTE: for large cluster with recurring backup items, scanning through
     *       the entire backup directory could be expensive.
     *
     *       This method is retired. Instead, we should use the new methods
     *       of fetching the right backup file list from corresponding backup.json
     *       metadata file.
     * ------------------------------
     *
     * @param dirPath
     * @param fileSizeChk
     * @return
     */
    static Map<Path, Long> listFilesForDir(Path dirPath,
                                           boolean fileSizeChk) {
        assert ( dirPath != null );

        //Map<Path, Long> paths = new LinkedHashMap<>();
        Map<Path, Long> paths = new TreeMap<>();

        /**
         * Oracle Java 8 File stream API implementation
         * ------------------------------------
         try ( Stream<Path> stream = Files.walk(dirPath) ) {
         stream.filter( p -> !p.toFile().isDirectory())
         .forEach( p -> paths.put(p, getFileSize(p)) );
         }
         catch ( IOException ioe ) {
         ioe.printStackTrace();
         }
         */

        /**
         * Apache commons API
         */

        Collection<File> files = FileUtils.listFiles(dirPath.toFile(), TrueFileFilter.INSTANCE, TrueFileFilter.INSTANCE);
        for ( File file: files ) {
            Path path= file.toPath();
            paths.put(path, (!fileSizeChk ? 0 : getFileSize(path)));
        }

        return paths;
    }

    /**
     * Get mapping from "sstable_name" to "keyspace:table:sstable_unique_identifier:sstable_version"
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

            Iterator iterator = jsonItemArr.iterator();

            while (iterator.hasNext()) {
                String jsonItemContent = (String) ((JSONObject)iterator.next()).toJSONString();
                jsonItemContent = jsonItemContent.substring(1, jsonItemContent.length() -1 );

                String[] keyValuePairs = jsonItemContent.split(",");

                String ssTableName = "";
                String uniquifierStr = "";
                String keyspaceName = "";
                String tableName = "";
                String ssTableVersion = "";

                for ( String keyValuePair :  keyValuePairs ) {
                    String key = keyValuePair.split(":")[0];
                    String value = keyValuePair.split(":")[1];

                    key = key.substring(1, key.length() - 1 );
                    value = value.substring(1, value.length() - 1);

                    if ( key.equalsIgnoreCase("uniquifier") ) {
                        uniquifierStr = value;
                    }
                    else if ( key.equalsIgnoreCase("version") ) {
                        ssTableVersion = value;
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

                opscObjMaps.put(ssTableName, keyspaceName + ":" + tableName + ":" + uniquifierStr + ":" + ssTableVersion);
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        return opscObjMaps;
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
     * Find my DSE host ID by checking with DSE cluster
     *
     * @param dseClusterMetadata
     * @return
     */
    static String findMyHostID(Metadata dseClusterMetadata) {
        assert (dseClusterMetadata != null);

        String myHostId = null;

        String localhostIp = getLocalIP(CONFIGPROP.getProperty(DseOpscNFSRestoreUtils.CFG_KEY_IP_MATCHING_NIC));

        if (localhostIp == null) {
            System.out.println("\nERROR: failed to get local host IP address!");
        }
        else {
            boolean foundMatchingHost = false;
            for (Host host : dseClusterMetadata.getAllHosts()) {
                InetAddress listen_address = host.getListenAddress();
                String listen_address_ip = (listen_address != null) ? listen_address.getHostAddress() : "";

                InetAddress broadcast_address = host.getBroadcastAddress();
                String broadcast_address_ip = (broadcast_address != null) ? broadcast_address.getHostAddress() : "";

                //System.out.println("listen_address: " + listen_address_ip);
                //System.out.println("broadcast_address: " + broadcast_address_ip + "\n");

                if (localhostIp.equals(listen_address_ip) || localhostIp.equals(broadcast_address_ip)) {
                    myHostId = host.getHostId().toString();
                    foundMatchingHost = true;
                    break;
                }
            }

            if (!foundMatchingHost) {
                System.out.format("\nERROR: Failed to match my DSE host address by IP (NIC Name: %s; NIC IP: %s)!\n",
                        CONFIGPROP.getProperty(DseOpscNFSRestoreUtils.CFG_KEY_IP_MATCHING_NIC),
                        localhostIp);
            }
        }

        return myHostId;
    }


    /**
     * List (and download) Opsc backup objects for a specified host
     *
     * @param fileSizeChk
     * @param hostId
     * @param download
     * @param threadNum
     * @param keyspaceName
     * @param tableName
     * @param opscBckupTimeGmt
     * @param clearTargetDownDir
     * @param noTargetDirStruct
     */
    static void listDownloadNFSObjForHost(boolean fileSizeChk,
                                          String hostId,
                                          boolean download,
                                          int threadNum,
                                          String keyspaceName,
                                          String tableName,
                                          ZonedDateTime opscBckupTimeGmt,
                                          boolean clearTargetDownDir,
                                          boolean noTargetDirStruct )
    {
        assert (hostId != null);

        System.out.format("\nList" +
            (download ? " and download" : "") +
            " OpsCenter NFS backup items for specified host (%s) ...\n", hostId);

        String downloadHomeDir = CONFIGPROP.getProperty(DseOpscNFSRestoreUtils.CFG_KEY_LOCAL_DOWNLOAD_HOME);

        if (download) {
            assert (threadNum > 0);

            // If non-existing, create local home directory to hold download files
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
                System.out.println("ERROR: Failed to create download home directory for OpsCenter backup objects!");
                return;
            }
        }

        Map<String, String> opscUniquifierToKsTbls = new HashMap<String, String>();

        Path myBackupJsonFilePath = getMyBackupJson(hostId, opscBckupTimeGmt);

        if (myBackupJsonFilePath == null) {
            DateTimeFormatter opscObjTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH-mm-ss-z");
            String opscBckupTimeGmtStr = opscBckupTimeGmt.format(opscObjTimeFormatter);

            System.out.format("ERROR: Failed to find %s file for host (%s) at backup time (%s)\n",
                DseOpscNFSRestoreUtils.OPSC_BKUP_METADATA_FILE,
                hostId,
                opscBckupTimeGmtStr);

            return;
        }
        else {
            opscUniquifierToKsTbls =
                getOpscUniquifierToKsTblMapping(myBackupJsonFilePath.toFile().getAbsolutePath());
        }


        if ( opscUniquifierToKsTbls.isEmpty() ) {
            System.out.println("ERROR: Failed to get backup SSTable file list from " +
                    DseOpscNFSRestoreUtils.OPSC_BKUP_METADATA_FILE + " file!");
            return;
        }


        // Download OpsCenter backup SSTables
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
        String[] opscSstableVersions = new String[SSTABLE_SET_FILENUM];

        int i = 0;
        int threadId = 0;

        String sstablePrefixString =
            CONFIGPROP.get(DseOpscNFSRestoreUtils.CFG_KEY_OPSC_NFS_BKUP_HOMEDIR) + "/" +
            DseOpscNFSRestoreUtils.OPSC_NFS_OBJKEY_BASESTR + "/" +
            hostId + "/" +
            DseOpscNFSRestoreUtils.OPSC_NFS_OBJKEY_SSTABLES_MARKER_STR;


        for ( String sstableObjName : opscUniquifierToKsTbls.keySet() )  {

            String opscObjName = sstablePrefixString + "/" + sstableObjName;

            String[] ksTblUniquifer = opscUniquifierToKsTbls.get(sstableObjName).split(":");
            String ks = ksTblUniquifer[0];
            String tbl = ksTblUniquifer[1];
            String version = ksTblUniquifer[3];

            boolean filterKsTbl = keyspaceName.equalsIgnoreCase(ks);
            if ((tableName != null) && !tableName.isEmpty()) {
                filterKsTbl = filterKsTbl && tableName.equalsIgnoreCase(tbl);
            }

            if (filterKsTbl) {
                numSstableBkupItems++;

                long opscObjSize = 0;
                if (fileSizeChk) {
                    try {
                        opscObjSize = Files.size(Paths.get(opscObjName));
                    }
                    catch (IOException ioe) {
                        opscObjSize = -1;
                    }
                }

                System.out.println("  - " + opscObjName +
                    ( !fileSizeChk ? "" : (" (size = " + opscObjSize + " bytes)") ) +
                    " [keyspace: " + ks + "; table: " + tbl + "]");

                opscSstableObjKeyNames[i % SSTABLE_SET_FILENUM] = opscObjName;
                opscSstableObjKeySizes[i % SSTABLE_SET_FILENUM] = opscObjSize;
                opscSstableKSNames[i % SSTABLE_SET_FILENUM] = ks;
                opscSstableTBLNames[i % SSTABLE_SET_FILENUM] = tbl;
                opscSstableVersions[i % SSTABLE_SET_FILENUM] = version;

                if (download) {
                    if ((i > 0) && ((i + 1) % SSTABLE_SET_FILENUM == 0)) {
                        Runnable worker = new NFSObjDownloadRunnable(
                            threadId,
                            fileSizeChk,
                            downloadHomeDir,
                            opscSstableObjKeyNames,
                            opscSstableObjKeySizes,
                            opscSstableKSNames,
                            opscSstableTBLNames,
                            opscSstableVersions,
                            noTargetDirStruct);

                        threadId++;

                        opscSstableObjKeyNames = new String[SSTABLE_SET_FILENUM];
                        opscSstableObjKeySizes = new long[SSTABLE_SET_FILENUM];
                        opscSstableKSNames = new String[SSTABLE_SET_FILENUM];
                        opscSstableTBLNames = new String[SSTABLE_SET_FILENUM];
                        opscSstableVersions = new String[SSTABLE_SET_FILENUM];

                        executor.execute(worker);
                    }

                    i++;
                }
            }
        }

        if ( download && ( threadId  < (numSstableBkupItems / SSTABLE_SET_FILENUM) ) ) {
            Runnable worker = new NFSObjDownloadRunnable(
                threadId,
                fileSizeChk,
                downloadHomeDir,
                opscSstableObjKeyNames,
                opscSstableObjKeySizes,
                opscSstableKSNames,
                opscSstableTBLNames,
                opscSstableVersions,
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
     * List (and download) Opsc backup objects for myself - the host that runs this program
     *
     * @param dseClusterMetadata
     * @param fileSizeChk
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
                                        boolean fileSizeChk,
                                        boolean download,
                                        int threadNum,
                                        String hostIDStr,
                                        String keyspaceName,
                                        String tableName,
                                        ZonedDateTime opscBckupTimeGmt,
                                        boolean clearTargetDownDir,
                                        boolean noTargetDirStruct) {
        String myHostId = hostIDStr;

        // When not providing DSE host ID explicitly, find it by checking with DSE cluster
        if ( (hostIDStr == null) || (hostIDStr.isEmpty()) ) {
            myHostId = findMyHostID(dseClusterMetadata);
        }

        if ( myHostId != null && !myHostId.isEmpty() ) {
            listDownloadNFSObjForHost(
                fileSizeChk,
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
     * List Opsc backup objects for all DSE cluster hosts
     *
     * @param dseClusterMetadata
     * @param fileSizeChk
     * @param keyspaceName
     * @param tableName
     * @param opscBckupTimeGmt
     */
    static void listNFSObjtForCluster(Metadata dseClusterMetadata,
                                      boolean fileSizeChk,
                                      String keyspaceName,
                                      String tableName,
                                      ZonedDateTime opscBckupTimeGmt) {

        System.out.format("\nList OpsCenter NFS backup items for DSE cluster (%s) [%s] ...\n",
                dseClusterMetadata.getClusterName(),
                ((tableName == null) || (tableName.isEmpty())) ?
                        "Keyspace - " + keyspaceName :
                        "Table - " + keyspaceName + ":" + tableName
        );

        listNFSObjForDC(dseClusterMetadata, fileSizeChk, "", keyspaceName, tableName, opscBckupTimeGmt);
    }

    /**
     * List Opsc backup objects for all hosts in a specified DC
     *
     * @param dseClusterMetadata
     * @param fileSizeChk
     * @param keyspaceName
     * @param dcName
     * @param tableName
     * @param opscBckupTimeGmt
     */
    static void listNFSObjForDC(Metadata dseClusterMetadata,
                                boolean fileSizeChk,
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


                // First. get the backup.json file corresponds to the specified host and backup time

                Map<String, String> opscUniquifierToKsTbls = new HashMap<>();

                Path myBackupJsonFilePath = getMyBackupJson(host_id, opscBckupTimeGmt);

                if (myBackupJsonFilePath == null) {
                    DateTimeFormatter opscObjTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH-mm-ss-z");
                    String opscBckupTimeGmtStr = opscBckupTimeGmt.format(opscObjTimeFormatter);

                    System.out.format("    ERROR: Failed to find %s file for host (%s) at backup time (%s)\n",
                        DseOpscNFSRestoreUtils.OPSC_BKUP_METADATA_FILE,
                        host_id,
                        opscBckupTimeGmtStr);

                    continue;
                }
                else {
                    opscUniquifierToKsTbls =
                        getOpscUniquifierToKsTblMapping(myBackupJsonFilePath.toFile().getAbsolutePath());
                }


                if ( opscUniquifierToKsTbls.isEmpty() ) {
                    System.out.println("    ERROR: Failed to get backup SSTable file list from " +
                        DseOpscNFSRestoreUtils.OPSC_BKUP_METADATA_FILE + " file!");

                    continue;
                }


                // Second, check SSTables records matching the backup time, keyspace, and table

                String sstablePrefixString =
                    CONFIGPROP.get(DseOpscNFSRestoreUtils.CFG_KEY_OPSC_NFS_BKUP_HOMEDIR) + "/" +
                    DseOpscNFSRestoreUtils.OPSC_NFS_OBJKEY_BASESTR + "/" +
                    host_id + "/" +
                    DseOpscNFSRestoreUtils.OPSC_NFS_OBJKEY_SSTABLES_MARKER_STR;


                for ( String sstableObjName : opscUniquifierToKsTbls.keySet() )  {

                    String opscObjName = sstablePrefixString + "/" + sstableObjName;

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

                            long opscObjSize = 0;
                            if (fileSizeChk) {
                                try {
                                    opscObjSize = Files.size(Paths.get(opscObjName));
                                }
                                catch (IOException ioe) {
                                    opscObjSize = -1;
                                }
                            }

                            System.out.println("  - " + opscObjName +
                                ( !fileSizeChk ? "" : (" (size = " + opscObjSize + " bytes)") ) +
                                " [keyspace: " + ks + "; table: " + tbl + "]");
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
            "List OpsCenter backup items (all | DC:\"<dc_name>\" | me[:\"<host_id_string>\"]).");
        Option downloadOption = new Option(
            DseOpscNFSRestoreUtils.CMD_OPTION_DOWNLOAD_SHORT,
            DseOpscNFSRestoreUtils.CMD_OPTION_DOWNLOAD_LONG,
            true,
            "Download OpsCenter bakcup items to local directory (only applies to \"list me\" case");
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
        Option opscBkupTimeOption = new Option(
            DseOpscNFSRestoreUtils.CMD_OPTION_BACKUPTIME_SHORT,
            DseOpscNFSRestoreUtils.CMD_OPTION_BACKUPTIME_LONG,
            true,
            "OpsCetner backup datetime");
        Option clsTargetDirOption = new Option(
            DseOpscNFSRestoreUtils.CMD_OPTION_CLSDOWNDIR_SHORT,
            DseOpscNFSRestoreUtils.CMD_OPTION_CLSDOWNDIR_LONG,
            true,
            "Clear existing download directory content");
        Option noDirStructOption = new Option(
            DseOpscNFSRestoreUtils.CMD_OPTION_NODIR_SHORT,
            DseOpscNFSRestoreUtils.CMD_OPTION_NODIR_LONG,
            true,
            "Don't maintain keyspace/table backup data directory structure");
        Option userOption = new Option(
            DseOpscNFSRestoreUtils.CMD_OPTION_USER_SHORT,
            DseOpscNFSRestoreUtils.CMD_OPTION_USER_LONG,
            true,
            "Cassandra user name");
        Option passwdOption = new Option(
            DseOpscNFSRestoreUtils.CMD_OPTION_PWD_SHORT,
            DseOpscNFSRestoreUtils.CMD_OPTION_PWD_LONG,
            true,
            "Cassandra user password");
        Option debugOption = new Option(
            DseOpscNFSRestoreUtils.CMD_OPTION_DEBUG_SHORT,
            DseOpscNFSRestoreUtils.CMD_OPTION_DEBUG_LONG,
            false,
            "Debug output");

        options.addOption(helpOption);
        options.addOption(listOption);
        options.addOption(fileOption);
        options.addOption(downloadOption);
        options.addOption(keyspaceOption);
        options.addOption(tableOption);
        options.addOption(opscBkupTimeOption);
        options.addOption(clsTargetDirOption);
        options.addOption(noDirStructOption);
        options.addOption(userOption);
        options.addOption(passwdOption);
        options.addOption(debugOption);
    }

    /**
     * Print out usage info. and exit
     */
    static void usageAndExit(int errorCode) {

        System.out.println();

        PrintWriter errWriter = new PrintWriter(System.out, true);

        try {
            HelpFormatter formatter = new HelpFormatter();

            formatter.printHelp(errWriter, 150, "DseOpscNFSRestore",
                String.format("\nDseOpscNFSRestore Options:"),
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
            usageAndExit(10);
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
            usageAndExit(20);
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
            usageAndExit(30);
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
                usageAndExit(40);
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
                usageAndExit(50);
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
            usageAndExit(60);
        }

        // Download option ONLY works for "-l me" option! If "-d" option value is not specified, use the default value
        boolean downloadOpscObj = false;
        int downloadOpscObjThreadNum = DseOpscNFSRestoreUtils.DOWNLOAD_THREAD_POOL_SIZE;

        if ( cmd.hasOption(DseOpscNFSRestoreUtils.CMD_OPTION_DOWNLOAD_SHORT) ) {
            downloadOpscObj = true;

            String dOptVal = cmd.getOptionValue(DseOpscNFSRestoreUtils.CMD_OPTION_DOWNLOAD_SHORT);
            if ( (dOptVal != null) && (!dOptVal.isEmpty()) ) {
                try {
                    downloadOpscObjThreadNum = Integer.parseInt(dOptVal);
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
            usageAndExit(70);
        }

        // "-t" option (Table name) is optional. If not specified, all Tables of the specified keyspaces will be processed.
        String tableName = cmd.getOptionValue(DseOpscNFSRestoreUtils.CMD_OPTION_TABLE_SHORT);

        // "-obt" option is a must
        // OpsCenter Backup Date Time String (Can get  from OpsCenter Backup Service Window)
        String obtOptOptValue = cmd.getOptionValue(DseOpscNFSRestoreUtils.CMD_OPTION_BACKUPTIME_SHORT);

        if ( (obtOptOptValue == null) || (obtOptOptValue.isEmpty()) ) {
            System.out.println("\nERROR: Please specify proper OpsCenter backup time string (M/d/yyyy h:mm a) as the \"-" +
                DseOpscNFSRestoreUtils.CMD_OPTION_BACKUPTIME_SHORT + "\" option value.");
            usageAndExit(80);
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
            usageAndExit(90);
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

        // "-u" and "-p" option is optional.
        String userName = cmd.getOptionValue(DseOpscNFSRestoreUtils.CMD_OPTION_USER_SHORT);
        String passWord = cmd.getOptionValue(DseOpscNFSRestoreUtils.CMD_OPTION_PWD_SHORT);

        // "-dbg" option is optional (default: false)
        if ( cmd.hasOption(DseOpscNFSRestoreUtils.CMD_OPTION_DEBUG_SHORT) ) {
            debugOpt = true;
        }


        /**
         *  Parsing commandline parameters (ends)  <----
         */



        /**
         * Load configuration files
         */

        CONFIGPROP = DseOpscNFSRestoreUtils.LoadConfigFile(cfgFilePath);
        if (CONFIGPROP == null) {
            usageAndExit(100);
        }

        // Check whether "use_ssl" config file parameter is true (default false).
        // - If so, java system properties "-Djavax.net.ssl.trustStore" and "-Djavax.net.ssl.trustStorePassword" must be set.
        boolean useSsl = false;
        String useSslStr = CONFIGPROP.getProperty(DseOpscNFSRestoreUtils.CFG_KEY_USE_SSL);
        if ( (useSslStr != null) && !(useSslStr.isEmpty()) ) {
            useSsl = Boolean.parseBoolean(useSslStr);
        }

        if (useSsl) {
            String trustStoreProp = System.getProperty(DseOpscNFSRestoreUtils.JAVA_SSL_TRUSTSTORE_PROP);
            String trustStorePassProp = System.getProperty(DseOpscNFSRestoreUtils.JAVA_SSL_TRUSTSTORE_PASS_PROP);

            if ( (trustStoreProp == null) || trustStoreProp.isEmpty() ||
                 (trustStorePassProp == null) || trustStorePassProp.isEmpty()) {
                System.out.format("\nERROR: Must provide SSL system properties (-D%s and -D%s) when " +
                                "configuration file parameter \"%s\" is set true!\n",
                    DseOpscNFSRestoreUtils.JAVA_SSL_TRUSTSTORE_PROP,
                    DseOpscNFSRestoreUtils.JAVA_SSL_TRUSTSTORE_PASS_PROP,
                    DseOpscNFSRestoreUtils.CFG_KEY_USE_SSL);
                usageAndExit(104);
            }
        }

        // Check whether "user_auth" config file parameter is true (default false).
        // - If so, command line parameter "-u (--user)" and "-p (--password)" must be set.
        boolean userAuth = false;
        String userAuthStr = CONFIGPROP.getProperty(DseOpscNFSRestoreUtils.CFG_KEY_USER_AUTH);
        if ( (userAuthStr != null) && !(userAuthStr.isEmpty()) ) {
            userAuth = Boolean.parseBoolean(userAuthStr);
        }

        if (userAuth) {
            if ( (userName == null) || userName.isEmpty() ||
                 (passWord == null) || passWord.isEmpty() ) {
                System.out.println("\nERROR: Must provide user name and password when configuration file parameter \"" +
                    DseOpscNFSRestoreUtils.CFG_KEY_USER_AUTH + "\" is set true!");
                usageAndExit(105);
            }
        }

        // Check whether "file_size_mon" config file parameter is true (default false).
        boolean fileSizeChk = false;
        String fileSizeMonStr = CONFIGPROP.getProperty(DseOpscNFSRestoreUtils.CFG_KEY_FILE_SIZE_CHK);
        if ( (fileSizeMonStr != null) && !(fileSizeMonStr.isEmpty()) ) {
            fileSizeChk = Boolean.parseBoolean(fileSizeMonStr);
        }


        /**
         * Check if NFS backup home directory is reachable! Otherwise, list files under it.
         */
        Path nfsBackupHomePath = Paths.get(CONFIGPROP.getProperty(DseOpscNFSRestoreUtils.CFG_KEY_OPSC_NFS_BKUP_HOMEDIR));
        if ( Files.notExists(nfsBackupHomePath) ||
             !Files.isDirectory(nfsBackupHomePath)  ||
             !(nfsBackupHomePath.toFile().canRead() && nfsBackupHomePath.toFile().canExecute()) ) {
            System.out.println("\nERROR: [Config File] Specified NFS OpsCenter backup directory is not correct (doesn't exist, non-directory, or no READ privilege)!");
            usageAndExit(110);
        }

        /**
         * Retired code - scanning through the entire backup folder for
         *                a large cluster and/or frequent backup can be
         *                expensive
         * ---------------------------------
        // List the files (recursively) under the NFS backup home directory
        NFS_BACKUP_FILELIST = listFilesForDir(nfsBackupHomePath, fileSizeChk);
        if ( ( NFS_BACKUP_FILELIST == null) || ( NFS_BACKUP_FILELIST.isEmpty() ) ) {
            System.out.println("\nERROR: [Config File] Specified NFS OpsCenter backup home directory doesn't have any file contents in it!");
            usageAndExit(115);
        }
        */

        /**
         * If the specified download home directory already exists,
         *      check if it is reachable and write-able; otherwise, error-out.
         *
         * If the specified download home directory doesn't exist, do nothing here
         */
        Path localDownloadHomePath = Paths.get(CONFIGPROP.getProperty(DseOpscNFSRestoreUtils.CFG_KEY_LOCAL_DOWNLOAD_HOME));
        if ( Files.exists(localDownloadHomePath) &&
             ( !Files.isDirectory(localDownloadHomePath)  ||
               !( localDownloadHomePath.toFile().canRead() &&
                  localDownloadHomePath.toFile().canWrite() &&
                  localDownloadHomePath.toFile().canExecute()
                )
             )
           )
        {
            System.out.println("\nERROR: [Config File] Specified local download directory is not correct (doesn't exist, non-directory, or no Read/Write privilege)!");
            usageAndExit(120);
        }


        // Testing purpose
        /*
        for ( Path path : NFS_BACKUP_FILELIST.keySet() ) {
            System.out.println(path.toFile().getAbsolutePath() + "(size: " + NFS_BACKUP_FILELIST.get(path) + ")");
        }

        System.exit(0);
        */


        /**
         * Get Dse cluster metadata
         */
        QueryOptions queryOptions = new QueryOptions();
        queryOptions.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);


        // dseCluster.connect();    /* NO NEED for acutal connection */
        Metadata dseClusterMetadata = null;

        // Do NOT check cluster metadata for "-l me:<dse_host_id>" option
        boolean checkDseMetadata = listCluster || listDC || (listMe && ((myHostID == null) || myHostID.isEmpty()) );
        if (checkDseMetadata) {

            try {
                DseCluster.Builder clusterBuilder = DseCluster.builder()
                    .addContactPoint(CONFIGPROP.getProperty(DseOpscNFSRestoreUtils.CFG_KEY_CONTACT_POINT))
                    .withQueryOptions(queryOptions);


                if (useSsl) {
                    clusterBuilder.withSSL();
                }

                if (userAuth) {
                    AuthProvider authProvider = new PlainTextAuthProvider(userName, passWord);
                    clusterBuilder.withAuthProvider(authProvider);
                }

                DseCluster dseCluster = clusterBuilder.build();
                dseClusterMetadata = dseCluster.getMetadata();
            }
            catch (NoHostAvailableException nhae) {
                System.out.println("\nERROR: Failed to check DSE cluster metadata. " +
                        "Please check DSE cluster status and/or connection requirements (e.g. SSL/TLS, username/password)!");
                usageAndExit(120);
            }
            catch (Exception e) {
                System.out.println("\nERROR: Unknown error when checking DSE cluster metadata!");
                e.printStackTrace();
                usageAndExit(125);
            }
        }

        // List OpsCenter backup SSTables for all Dse Cluster hosts
        if ( listCluster ) {
            listNFSObjtForCluster(
                dseClusterMetadata,
                fileSizeChk,
                keyspaceName,
                tableName,
                opscBackupTime_gmt);
        }
        // List OpsCenter backup SSTables for all hosts in a specified DC of the Dse cluster
        else if ( listDC ) {
            listNFSObjForDC(
                dseClusterMetadata,
                fileSizeChk,
                dcNameToList,
                keyspaceName,
                tableName,
                opscBackupTime_gmt);
        }
        // List (and download) OpsCenter backup SSTables for myself (the host that runs this program)
        else if ( listMe ) {
            listDownloadNFSObjForMe(
                dseClusterMetadata,
                fileSizeChk,
                downloadOpscObj,
                downloadOpscObjThreadNum,
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
